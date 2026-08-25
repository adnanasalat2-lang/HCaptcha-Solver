const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json({ limit: '60mb' }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');
const KNN_BRAIN_FILE = path.join(DATA_DIR, 'knn_brain.json');

const MAX_PENDING = 80;
const MAX_TRAINED = 50000;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};
let centralizedKnnDataset = {};

const taskAssignments = new Map();
const browserSockets = new Map();
const activeWorkers = new Map();

let roundRobinWorkerIndex = 0;

function isGridTask(task) {
    if (!task || !task.media || task.media.length < 4) return false;
    let p = (task.prompt || "").toLowerCase();
    if (p.includes('drag') || p.includes('move') || p.includes('pattern') || p.includes('fit') || p.includes('spot')) return false;
    return task.media.every(m => m.type === 'image');
}

function getCleanKey(task) {
    let p = (task.prompt || "").split("|||")[0].trim().toLowerCase();
    return "TXT_" + p;
}

function rebuildConceptBank() {
    conceptBank = {};
    for (let id in hcaptchaTrained) {
        let cKey = getCleanKey(hcaptchaTrained[id]);
        if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
        (hcaptchaTrained[id].clicks || []).forEach(idx => {
            if (typeof idx === 'number' && hcaptchaTrained[id].media && hcaptchaTrained[id].media[idx]?.dhash) {
                conceptBank[cKey].add(hcaptchaTrained[id].media[idx].dhash);
            }
        });
    }
}

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            const raw = fs.readFileSync(DB_FILE, 'utf8');
            const data = JSON.parse(raw);
            hcaptchaPending = {};
            hcaptchaTrained = data.trained || {};
            rebuildConceptBank();
        } catch (e) {}
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

function getOnlineWorkers(preferredMode) {
    let list = [];
    activeWorkers.forEach((meta, ws) => {
        if (ws.readyState === WebSocket.OPEN && meta.mode === preferredMode) {
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
        sockets.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(payload); });
        browserSockets.delete(taskId);
    }
}

function dispatchTaskToWorker(taskData) {
    let targetMode = isGridTask(taskData) ? 'grid' : 'manual';
    let workers = getOnlineWorkers(targetMode);
    if (workers.length === 0) return;

    roundRobinWorkerIndex = (roundRobinWorkerIndex + 1) % workers.length;
    let targetWorker = workers[roundRobinWorkerIndex];

    if (targetWorker && targetWorker.ws.readyState === WebSocket.OPEN) {
        taskAssignments.set(taskData.id, targetWorker.ws);
        targetWorker.meta.assignedTasks.add(taskData.id);
        targetWorker.ws.send(JSON.stringify({ action: 'new_task', task: taskData }));
    }
}

function handleNewTask(task, wsSource) {
    if (!task || !task.taskId) return { success: false };

    // Auto solve by direct ID match
    if (hcaptchaTrained[task.taskId]) {
        let clicks = hcaptchaTrained[task.taskId].clicks || [];
        if (wsSource && wsSource.readyState === WebSocket.OPEN) {
            wsSource.send(JSON.stringify({ action: 'solve', taskId: task.taskId, clicks }));
        }
        notifyBrowsers(task.taskId, clicks);
        return { success: true, autoSolved: true, clicks };
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

        hcaptchaTrained[taskId] = {
            id: taskId,
            prompt: source.prompt,
            conceptKey: getCleanKey(source),
            media: lightMedia,
            clicks: clicks,
            trainedAt: new Date().toISOString()
        };

        delete hcaptchaPending[taskId];
        taskAssignments.delete(taskId);
        persistDatabase();
        notifyBrowsers(taskId, clicks);

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
                    name: data.workerName || 'Worker_' + Math.floor(Math.random()*100),
                    mode: data.mode || 'manual',
                    assignedTasks: new Set()
                };
                activeWorkers.set(ws, workerMeta);

                let availablePending = {};
                Object.values(hcaptchaPending).forEach(task => {
                    if (!taskAssignments.has(task.id)) {
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
                        trained: Object.keys(hcaptchaTrained).length
                    }
                }));

                broadcastCounts();
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
        trained: Object.keys(hcaptchaTrained).length
    });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    taskAssignments.delete(req.params.id);
    persistDatabase();
    activeWorkers.forEach((meta, ws) => {
        meta.assignedTasks.delete(req.params.id);
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ action: 'task_deleted', taskId: req.params.id }));
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
