const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const Redis = require('ioredis');

const app = express();
app.use(cors());
app.use(express.json({ limit: '60mb' }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');
const KNN_BRAIN_FILE = path.join(DATA_DIR, 'knn_brain.json');

const MAX_PENDING = 100;
const MAX_TRAINED = 50000;
const DASHBOARD_PAGE_SIZE = 20;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};
let centralizedKnnDataset = {};

const taskAssignments = new Map();
const browserSockets = new Map();
const activeWorkers = new Map();

let gridWorkerIndex = 0;
let manualWorkerIndex = 0;

let redis = null;
if (process.env.REDIS_URL) {
    try {
        redis = new Redis(process.env.REDIS_URL, { retryStrategy: () => 2000, lazyConnect: true });
        redis.connect().then(() => console.log('✅ Redis Connected')).catch(() => {});
    } catch(e) {}
}

function getCleanKey(task) {
    let p = (task.prompt || "").split("|||")[0].trim().toLowerCase();
    return "TXT_" + p;
}

function isGridTask(task) {
    if (!task || !task.media || task.media.length < 4) return false;
    let p = (task.prompt || "").toLowerCase();
    if (p.includes('drag') || p.includes('move') || p.includes('pattern') || p.includes('fit') || p.includes('spot')) return false;
    return task.media.every(m => m.type === 'image');
}

function dhashToBigInt(hexOrBin) {
    if (!hexOrBin || hexOrBin === "0000000000000000") return null;
    if (/^[01]+$/.test(hexOrBin)) return BigInt('0b' + hexOrBin);
    try { return BigInt('0x' + hexOrBin); } catch(e) { return null; }
}

function getHammingDistance(h1, h2) {
    if (!h1 || !h2 || h1.length !== h2.length) return 999;
    const b1 = dhashToBigInt(h1);
    const b2 = dhashToBigInt(h2);
    if (b1 !== null && b2 !== null) {
        let xor = b1 ^ b2;
        let diff = 0;
        while (xor > 0n) { diff += Number(xor & 1n); xor >>= 1n; }
        return diff;
    }
    let diff = 0;
    for (let i = 0; i < h1.length; i++) { if (h1[i] !== h2[i]) diff++; }
    return diff;
}

function rebuildConceptBank() {
    conceptBank = {};
    for (let id in hcaptchaTrained) {
        _addToConceptBank(hcaptchaTrained[id]);
    }
}

function _addToConceptBank(tr) {
    let cKey = getCleanKey(tr);
    if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
    (tr.clicks || []).forEach(idx => {
        if (typeof idx === 'number' && tr.media && tr.media[idx] && tr.media[idx].dhash && tr.media[idx].dhash !== "0000000000000000") {
            conceptBank[cKey].add(tr.media[idx].dhash);
        }
    });
}

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            const raw = fs.readFileSync(DB_FILE, 'utf8');
            const data = JSON.parse(raw);
            hcaptchaPending = {};
            hcaptchaTrained = data.trained || {};
            rebuildConceptBank();
            console.log(`[DB] Engine Loaded. Trained: ${Object.keys(hcaptchaTrained).length}`);
        } catch (e) {
            console.error("[DB] Error loading database:", e.message);
        }
    }
    if (fs.existsSync(KNN_BRAIN_FILE)) {
        try { centralizedKnnDataset = JSON.parse(fs.readFileSync(KNN_BRAIN_FILE, 'utf8')); } catch(e) {}
    }
}
initDB();

let saveTimeout = null;
function persistDatabase() {
    if (saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
        fs.writeFile(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8', () => {});
        fs.writeFile(KNN_BRAIN_FILE, JSON.stringify(centralizedKnnDataset), 'utf8', () => {});
    }, 2000);
}

function evaluateAutoSolve(task) {
    if (hcaptchaTrained[task.taskId]) {
        return { solved: true, clicks: hcaptchaTrained[task.taskId].clicks || [] };
    }

    if (task.media && task.media.length === 1) {
        const item = task.media[0];
        for (let tid in hcaptchaTrained) {
            const trained = hcaptchaTrained[tid];
            if (trained.media && trained.media.length === 1) {
                const trItem = trained.media[0];
                if (trItem.stableHash && item.stableHash && trItem.stableHash === item.stableHash) {
                    return { solved: true, clicks: trained.clicks || [] };
                }
                if (item.dhash && trItem.dhash && item.dhash !== "0000000000000000" && getHammingDistance(item.dhash, trItem.dhash) <= 1) {
                    return { solved: true, clicks: trained.clicks || [] };
                }
            }
        }
        return { solved: false };
    }

    let cKey = getCleanKey(task);
    let targetDhashes = conceptBank[cKey];

    if (targetDhashes && targetDhashes.size > 0 && isGridTask(task)) {
        let matchedClicks = [];
        task.media.forEach((item, idx) => {
            if (!item.dhash || item.dhash === "0000000000000000") return;
            for (let savedHash of targetDhashes) {
                if (getHammingDistance(item.dhash, savedHash) <= 2) {
                    matchedClicks.push(idx);
                    break;
                }
            }
        });

        if (matchedClicks.length >= 2 && matchedClicks.length <= 5) {
            return { solved: true, clicks: matchedClicks };
        }
    }
    return { solved: false };
}

function getOnlineWorkers(mode) {
    let list = [];
    activeWorkers.forEach((meta, ws) => {
        if (ws.readyState === WebSocket.OPEN && meta.mode === mode) {
            list.push({ ws, meta });
        }
    });
    if (list.length === 0) {
        activeWorkers.forEach((meta, ws) => {
            if (ws.readyState === WebSocket.OPEN) list.push({ ws, meta });
        });
    }
    return list;
}

function broadcastCounts() {
    let gridWorkers = 0, manualWorkers = 0;
    activeWorkers.forEach(meta => {
        if (meta.mode === 'grid') gridWorkers++; else manualWorkers++;
    });

    const payload = JSON.stringify({
        action: 'counts_update',
        counts: {
            pending: Object.keys(hcaptchaPending).length,
            trained: Object.keys(hcaptchaTrained).length,
            concepts: Object.keys(conceptBank).length,
            gridWorkersOnline: gridWorkers,
            manualWorkersOnline: manualWorkers
        }
    });

    activeWorkers.forEach((_, ws) => {
        if (ws.readyState === WebSocket.OPEN) ws.send(payload);
    });
}

function notifyBrowsers(taskId, clicks) {
    if (browserSockets.has(taskId)) {
        const sockets = browserSockets.get(taskId);
        const payload = JSON.stringify({ action: 'solve', taskId, clicks });
        sockets.forEach(ws => {
            if (ws.readyState === WebSocket.OPEN) ws.send(payload);
        });
        browserSockets.delete(taskId);
    }
}

function dispatchTaskToWorker(taskData) {
    if (taskAssignments.has(taskData.id)) return;

    let targetMode = isGridTask(taskData) ? 'grid' : 'manual';
    let workers = getOnlineWorkers(targetMode);

    if (workers.length === 0) return;

    let targetWorker = null;
    if (targetMode === 'grid') {
        gridWorkerIndex = (gridWorkerIndex + 1) % workers.length;
        targetWorker = workers[gridWorkerIndex];
    } else {
        manualWorkerIndex = (manualWorkerIndex + 1) % workers.length;
        targetWorker = workers[manualWorkerIndex];
    }

    if (targetWorker && targetWorker.ws.readyState === WebSocket.OPEN) {
        taskAssignments.set(taskData.id, targetWorker.ws);
        targetWorker.meta.assignedTasks.add(taskData.id);
        targetWorker.ws.send(JSON.stringify({ action: 'new_task', task: taskData }));
    }
}

function handleNewTask(task, wsSource) {
    if (!task || !task.taskId) return { success: false };

    if (hcaptchaPending[task.taskId]) {
        return { success: true, autoSolved: false };
    }

    let autoRes = evaluateAutoSolve(task);
    if (autoRes.solved) {
        if (wsSource && wsSource.readyState === WebSocket.OPEN) {
            wsSource.send(JSON.stringify({ action: 'solve', taskId: task.taskId, clicks: autoRes.clicks }));
        }
        notifyBrowsers(task.taskId, autoRes.clicks);
        return { success: true, autoSolved: true, clicks: autoRes.clicks };
    }

    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) {
        let deletedKeys = keys.slice(0, 15);
        deletedKeys.forEach(k => {
            delete hcaptchaPending[k];
            taskAssignments.delete(k);
        });
        activeWorkers.forEach((_, ws) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ action: 'bulk_remove_pending', taskIds: deletedKeys }));
            }
        });
    }

    const taskData = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media,
        timestamp: task.timestamp
    };
    hcaptchaPending[task.taskId] = taskData;

    dispatchTaskToWorker(taskData);
    broadcastCounts();
    return { success: true, autoSolved: false };
}

function handleTileUpdate(payload) {
    const { taskId, index, thumb, dhash, stableHash } = payload;
    if (!taskId || index === undefined) return;

    let target = hcaptchaPending[taskId];
    if (target && target.media && target.media[index]) {
        target.media[index].thumb = thumb || target.media[index].thumb;
        target.media[index].dhash = dhash || target.media[index].dhash;
        if (stableHash) target.media[index].stableHash = stableHash;
    }

    const msg = JSON.stringify({ action: 'tile_patch', taskId, index, thumb, dhash });
    activeWorkers.forEach((meta, ws) => {
        if (ws.readyState === WebSocket.OPEN && meta.assignedTasks.has(taskId)) {
            ws.send(msg);
        }
    });
}

function handleSubmitTask(taskId, clicks) {
    if (!taskId || !clicks || !Array.isArray(clicks) || clicks.length === 0) return { success: false };

    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    if (source) {
        let lightMedia = (source.media || []).map(m => ({
            dhash: m.dhash || "",
            stableHash: m.stableHash || "",
            type: m.type || "image",
            index: m.index !== undefined ? m.index : 0,
            thumb: m.thumb || (m.frames ? m.frames[0] : "")
        }));

        let cKey = getCleanKey(source);
        if (!conceptBank[cKey]) conceptBank[cKey] = new Set();

        clicks.forEach(idx => {
            if (typeof idx === 'number' && lightMedia[idx] && lightMedia[idx].dhash && lightMedia[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(lightMedia[idx].dhash);
            }
        });

        hcaptchaTrained[taskId] = {
            id: taskId,
            prompt: source.prompt,
            conceptKey: cKey,
            media: lightMedia,
            clicks: clicks,
            trainedAt: new Date().toISOString()
        };

        const trainedKeys = Object.keys(hcaptchaTrained);
        if (trainedKeys.length > MAX_TRAINED) {
            const oldest = trainedKeys.slice(0, trainedKeys.length - MAX_TRAINED);
            oldest.forEach(k => delete hcaptchaTrained[k]);
        }

        delete hcaptchaPending[taskId];
        taskAssignments.delete(taskId);
        persistDatabase();
        notifyBrowsers(taskId, clicks);

        // Broadcast remove command to all active dashboards so no lingering cards remain
        const solveMsg = JSON.stringify({ action: 'task_solved', taskId });
        activeWorkers.forEach((meta, ws) => {
            meta.assignedTasks.delete(taskId);
            if (ws.readyState === WebSocket.OPEN) ws.send(solveMsg);
        });

        broadcastCounts();
    }
    return { success: true };
}

wss.on('connection', (ws) => {
    let boundTaskId = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (data.action === 'register_dashboard') {
                const workerMeta = {
                    name: data.workerName || 'Worker_' + Math.floor(Math.random()*1000),
                    mode: data.mode || 'manual',
                    assignedTasks: new Set()
                };
                activeWorkers.set(ws, workerMeta);

                let isGrid = workerMeta.mode === 'grid';
                let availablePending = {};
                
                Object.values(hcaptchaPending).forEach(task => {
                    let taskIsGrid = isGridTask(task);
                    if (((isGrid && taskIsGrid) || (!isGrid && !taskIsGrid)) && !taskAssignments.has(task.id)) {
                        taskAssignments.set(task.id, ws);
                        workerMeta.assignedTasks.add(task.id);
                        availablePending[task.id] = task;
                    }
                });

                ws.send(JSON.stringify({
                    action: 'init_state',
                    pending: availablePending,
                    knnBrain: centralizedKnnDataset,
                    counts: {
                        pending: Object.keys(hcaptchaPending).length,
                        trained: Object.keys(hcaptchaTrained).length,
                        concepts: Object.keys(conceptBank).length
                    }
                }));

                broadcastCounts();
                return;
            }

            if (data.action === 'tile_update') {
                handleTileUpdate(data);
                return;
            }

            if (data.action === 'sync_knn_update' && data.knnUpdates) {
                Object.assign(centralizedKnnDataset, data.knnUpdates);
                persistDatabase();
                
                const syncPayload = JSON.stringify({ action: 'knn_sync_broadcast', knnUpdates: data.knnUpdates });
                activeWorkers.forEach((meta, clientWs) => {
                    if (clientWs !== ws && clientWs.readyState === WebSocket.OPEN && meta.mode === 'grid') {
                        clientWs.send(syncPayload);
                    }
                });
                return;
            }

            if (data.action === 'new_task') {
                boundTaskId = data.task?.taskId;
                if (boundTaskId) {
                    if (!browserSockets.has(boundTaskId)) browserSockets.set(boundTaskId, new Set());
                    browserSockets.get(boundTaskId).add(ws);
                }
                const res = handleNewTask(data.task, ws);
                ws.send(JSON.stringify({ action: 'new_task_ack', taskId: boundTaskId, ...res }));
                return;
            }

            if (data.action === 'register' && data.taskId) {
                boundTaskId = data.taskId;
                if (!browserSockets.has(boundTaskId)) browserSockets.set(boundTaskId, new Set());
                browserSockets.get(boundTaskId).add(ws);

                if (hcaptchaTrained[boundTaskId]) {
                    ws.send(JSON.stringify({ action: 'solve', taskId: boundTaskId, clicks: hcaptchaTrained[boundTaskId].clicks }));
                }
                return;
            }

            if (data.action === 'submit_task') {
                handleSubmitTask(data.taskId, data.clicks);
                return;
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        if (activeWorkers.has(ws)) {
            const leavingWorker = activeWorkers.get(ws);
            activeWorkers.delete(ws);

            leavingWorker.assignedTasks.forEach(tid => {
                taskAssignments.delete(tid);
                if (hcaptchaPending[tid]) dispatchTaskToWorker(hcaptchaPending[tid]);
            });

            broadcastCounts();
        }

        if (boundTaskId && browserSockets.has(boundTaskId)) {
            const set = browserSockets.get(boundTaskId);
            set.delete(ws);
            if (set.size === 0) browserSockets.delete(boundTaskId);
        }
    });
});

app.post('/api/new-hcaptcha', (req, res) => res.json(handleNewTask(req.body)));
app.post('/api/submit-hcaptcha', (req, res) => res.json(handleSubmitTask(req.body.taskId, req.body.clicks)));

app.get('/api/tasks', (req, res) => {
    let source = req.query.tab === 'trained' ? hcaptchaTrained : hcaptchaPending;
    res.json({ tasks: source, total: Object.keys(source).length });
});

app.get('/api/counts', (req, res) => {
    res.json({
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length
    });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    taskAssignments.delete(req.params.id);
    rebuildConceptBank(); 
    persistDatabase();
    
    const delMsg = JSON.stringify({ action: 'task_deleted', taskId: req.params.id });
    activeWorkers.forEach((meta, ws) => {
        meta.assignedTasks.delete(req.params.id);
        if (ws.readyState === WebSocket.OPEN) ws.send(delMsg);
    });

    broadcastCounts();
    res.json({ success: true });
});

app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    if (fs.existsSync(f)) res.sendFile(f);
    else res.status(404).send('hcaptcha-dashboard.html not found.');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Master Server Live on Port ${PORT}`));
