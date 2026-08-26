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
const wss = new WebSocket.Server({ server, perMessageDeflate: false });

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');
const KNN_BRAIN_FILE = path.join(DATA_DIR, 'knn_brain.json');

const MAX_PENDING = 100;
const MAX_TRAINED = 50000;
const DASHBOARD_PAGE_SIZE = 20;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};
let centralizedKnnDataset = {}; // Shared tensor memory across all workers

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
        try {
            centralizedKnnDataset = JSON.parse(fs.readFileSync(KNN_BRAIN_FILE, 'utf8'));
            console.log(`[AI] Centralized Brain Loaded (${Object.keys(centralizedKnnDataset).length} classes)`);
        } catch(e) {}
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

    if (targetDhashes && targetDhashes.size > 0 && task.media && task.media.length > 1) {
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
    return list;
}

function broadcastCounts() {
    let gridWorkers = getOnlineWorkers('grid').map(w => w.meta.name);
    let manualWorkers = getOnlineWorkers('manual').map(w => w.meta.name);

    const payload = JSON.stringify({
        action: 'counts_update',
        counts: {
            pending: Object.keys(hcaptchaPending).length,
            trained: Object.keys(hcaptchaTrained).length,
            concepts: Object.keys(conceptBank).length,
            gridWorkersOnline: gridWorkers.length,
            manualWorkersOnline: manualWorkers.length,
            gridWorkerNames: gridWorkers,
            manualWorkerNames: manualWorkers
        }
    });

    // FIX: Sirf active workers ko bhejo, browser sockets ko nahi
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
    let mLen = (taskData.media || []).length;
    let isGrid    = mLen > 1;   // 9-tile grid
    let isEmpty   = mLen === 0; // media abhi nahi aayi

    if (isEmpty) {
        // Media abhi nahi — dono modes ko bhejo taake count update ho
        let allWorkers = [...getOnlineWorkers('grid'), ...getOnlineWorkers('manual')];
        allWorkers.forEach(w => {
            w.meta.assignedTasks.add(taskData.id);
            w.ws.send(JSON.stringify({ action: 'new_task', task: taskData }));
        });
        return;
    }

    if (isGrid) {
        // Grid tile task — sirf grid workers ko
        let workers = getOnlineWorkers('grid');
        if (!workers.length) return;
        let target = workers[gridWorkerIndex % workers.length];
        gridWorkerIndex = (gridWorkerIndex + 1) % workers.length;
        if (target && target.ws.readyState === WebSocket.OPEN) {
            target.meta.assignedTasks.add(taskData.id);
            target.ws.send(JSON.stringify({ action: 'new_task', task: taskData }));
        }
    } else {
        // Canvas / coord / drag task (media.length === 1) — DONO modes ko bhejo
        // Kyunki grid worker dashboard mein bhi ye tasks solve kar sakta hai
        let gridWs   = getOnlineWorkers('grid');
        let manualWs = getOnlineWorkers('manual');
        let allTarget = [...gridWs, ...manualWs];
        allTarget.forEach(w => {
            w.meta.assignedTasks.add(taskData.id);
            w.ws.send(JSON.stringify({ action: 'new_task', task: taskData }));
        });
    }
}

function handleNewTask(task, wsSource) {
    if (!task || !task.taskId) return { success: false };

    // ✅ FIX: Agar task already pending mein hai aur ab media aa gayi — update karo
    if (hcaptchaPending[task.taskId] && task.media && task.media.length > 0) {
        let existingTask = hcaptchaPending[task.taskId];
        let wasEmpty = !existingTask.media || existingTask.media.length === 0;
        existingTask.media = task.media;
        existingTask.prompt = task.prompt || existingTask.prompt;

        if (wasEmpty) {
            // Ab media aa gayi — dashboard workers ko updated task bhejo
            const updatePayload = JSON.stringify({
                action: 'task_media_ready',
                taskId: task.taskId,
                task: existingTask
            });
            activeWorkers.forEach((meta, clientWs) => {
                if (clientWs.readyState === WebSocket.OPEN && meta.assignedTasks.has(task.taskId)) {
                    clientWs.send(updatePayload);
                }
            });
        }

        let autoRes = evaluateAutoSolve(existingTask);
        if (autoRes.solved) {
            if (wsSource && wsSource.readyState === WebSocket.OPEN) {
                wsSource.send(JSON.stringify({ action: 'solve', taskId: task.taskId, clicks: autoRes.clicks }));
            }
            notifyBrowsers(task.taskId, autoRes.clicks);
            return { success: true, autoSolved: true, clicks: autoRes.clicks };
        }
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
        deletedKeys.forEach(k => delete hcaptchaPending[k]);
        activeWorkers.forEach((_, ws) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ action: 'bulk_remove_pending', taskIds: deletedKeys }));
            }
        });
    }

    const taskData = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media || [],
        timestamp: task.timestamp
    };
    hcaptchaPending[task.taskId] = taskData;

    // ✅ FIX: Chahe media empty ho ya nahi — workers ko task bhejo taake count update ho
    dispatchTaskToWorker(taskData);
    broadcastCounts();
    return { success: true, autoSolved: false };
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
            thumb: m.thumb || (m.frames ? m.frames[0] : "") || m.src || "",
            src: m.src || m.thumb || (m.frames ? m.frames[0] : "") || ""
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
        persistDatabase();
        notifyBrowsers(taskId, clicks);

        const solveMsg = JSON.stringify({ action: 'task_solved', taskId, trainedTask: hcaptchaTrained[taskId] });
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
                    mode: data.mode || 'grid',
                    assignedTasks: new Set()
                };
                activeWorkers.set(ws, workerMeta);

                let isGrid = workerMeta.mode === 'grid';
                let availablePending = {};
                
                Object.values(hcaptchaPending).forEach(task => {
                    let mLen = (task.media || []).length;
                    let taskIsGrid   = mLen > 1;
                    let taskIsManual = mLen === 1;  // canvas/coord/drag = 1 media item
                    let taskIsEmpty  = mLen === 0;
                    // Empty tasks — dono workers ko dikhao
                    // Grid tasks (9-tile) — sirf grid workers
                    // Manual/canvas/drag tasks — sirf manual workers
                    // Grid workers ko bhi manual tasks dikhao (dono modes handle kar saken)
                    let shouldSend = taskIsEmpty
                        || (isGrid && (taskIsGrid || taskIsManual))
                        || (!isGrid && (taskIsManual || taskIsEmpty));
                    if (shouldSend) {
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

            // Sync AI Brain Updates across all connected grid instances
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

            // ✅ FIX: tile_update — extension se aata hai, dashboard workers ko forward karo
            if (data.action === 'tile_update' && data.taskId) {
                // Pending task ki media bhi update karo server pe
                if (hcaptchaPending[data.taskId] && hcaptchaPending[data.taskId].media) {
                    let m = hcaptchaPending[data.taskId].media[data.index];
                    if (m) {
                        m.thumb = data.thumb || m.thumb;
                        m.dhash = data.dhash || m.dhash;
                        m.stableHash = data.stableHash || m.stableHash;
                    }
                }
                // Assigned dashboard workers ko patch bhejo
                const patchPayload = JSON.stringify({
                    action: 'tile_patch',
                    taskId: data.taskId,
                    index: data.index,
                    thumb: data.thumb,
                    dhash: data.dhash,
                    stableHash: data.stableHash
                });
                activeWorkers.forEach((meta, clientWs) => {
                    if (clientWs.readyState === WebSocket.OPEN && meta.assignedTasks.has(data.taskId)) {
                        clientWs.send(patchPayload);
                    }
                });
                return;
            }

            // ✅ FIX: media_ready — jab extension tiles fully load ho jaaye, task ko re-dispatch karo
            if (data.action === 'media_ready' && data.taskId) {
                if (hcaptchaPending[data.taskId]) {
                    const readyPayload = JSON.stringify({
                        action: 'task_media_ready',
                        taskId: data.taskId,
                        task: hcaptchaPending[data.taskId]
                    });
                    activeWorkers.forEach((meta, clientWs) => {
                        if (clientWs.readyState === WebSocket.OPEN && meta.assignedTasks.has(data.taskId)) {
                            clientWs.send(readyPayload);
                        }
                    });
                }
                return;
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        if (activeWorkers.has(ws)) {
            const leavingWorker = activeWorkers.get(ws);
            const orphanedTasks = Array.from(leavingWorker.assignedTasks);
            activeWorkers.delete(ws);

            orphanedTasks.forEach(tid => {
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
    const tab = req.query.tab === 'trained' ? 'trained' : 'pending';
    const page = Math.max(0, parseInt(req.query.page) || 0);
    const size = Math.min(30, Math.max(1, parseInt(req.query.size) || DASHBOARD_PAGE_SIZE));

    let source = tab === 'trained' ? hcaptchaTrained : hcaptchaPending;
    let ids = Object.keys(source);

    if (tab === 'trained') ids = ids.reverse();
    let total = ids.length;
    let pageIds = ids.slice(page * size, (page + 1) * size);
    let tasks = {};
    pageIds.forEach(id => { tasks[id] = source[id]; });

    res.json({ tasks, total, page, pages: Math.ceil(total / size), tab });
});

app.get('/api/counts', (req, res) => {
    let gridWorkers = getOnlineWorkers('grid').map(w => w.meta.name);
    let manualWorkers = getOnlineWorkers('manual').map(w => w.meta.name);
    res.json({
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length,
        gridWorkersOnline: gridWorkers.length,
        manualWorkersOnline: manualWorkers.length
    });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    const delId = req.params.id;
    delete hcaptchaPending[delId];
    if (hcaptchaTrained[delId]) {
        let cKey = getCleanKey(hcaptchaTrained[delId]);
        delete hcaptchaTrained[delId];
        let stillExists = Object.values(hcaptchaTrained).some(t => getCleanKey(t) === cKey);
        if (!stillExists) delete conceptBank[cKey];
        else {
            conceptBank[cKey] = new Set();
            Object.values(hcaptchaTrained).filter(t => getCleanKey(t) === cKey).forEach(t => _addToConceptBank(t));
        }
    }
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
