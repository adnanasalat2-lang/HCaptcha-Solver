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
const AI_BRAIN_FILE = path.join(DATA_DIR, 'ai_brain.json'); 

const MAX_PENDING = 60;
const MAX_TRAINED = 50000;
const DASHBOARD_PAGE_SIZE = 40;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};
let globalAIBrain = {}; 

const browserSockets = new Map();
const dashboardWorkers = new Map(); 

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
    if (fs.existsSync(AI_BRAIN_FILE)) {
        try {
            globalAIBrain = JSON.parse(fs.readFileSync(AI_BRAIN_FILE, 'utf8'));
            console.log(`[AI] Global Brain Loaded. Concepts: ${Object.keys(globalAIBrain).length}`);
        } catch(e) {
            console.error("[AI] Error loading AI Brain:", e.message);
        }
    }
}
initDB();

let saveTimeout = null;
function persistDatabase() {
    if (saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
        fs.writeFile(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8', () => {});
    }, 2000);
}

let aiSaveTimer = null;
function persistAIBrain() {
    if (aiSaveTimer) clearTimeout(aiSaveTimer);
    aiSaveTimer = setTimeout(() => {
        fs.writeFile(AI_BRAIN_FILE, JSON.stringify(globalAIBrain), 'utf8', () => {});
    }, 5000);
}

function evaluateAutoSolve(task) {
    if (hcaptchaTrained[task.taskId]) {
        return { solved: true, clicks: hcaptchaTrained[task.taskId].clicks || [] };
    }
    const isVideoOrCoord = task.media && (task.media.length === 1 || task.media[0].type === 'video_frames' || task.media[0].frames);
    if (isVideoOrCoord) return { solved: false };

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

function broadcastDashboard(type, data) {
    if (!dashboardWorkers.size) return;
    const msg = JSON.stringify({ type, data });
    dashboardWorkers.forEach((info, ws) => {
        if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    });
}

function assignTask(taskId) {
    let task = hcaptchaPending[taskId];
    if (!task || task.assignedTab) return; 

    let taskMode = (task.media && task.media.length > 1) ? 'grid' : 'manual';
    let bestWs = null;
    let minCount = Infinity;
    let oldestAssignTime = Infinity;

    dashboardWorkers.forEach((info, ws) => {
        if (info.mode === taskMode && ws.readyState === WebSocket.OPEN) {
            let count = 0;
            for (let k in hcaptchaPending) {
                if (hcaptchaPending[k].assignedTab === info.tabId) count++;
            }
            
            let lastTime = info.lastAssigned || 0;
            
            if (count < minCount || (count === minCount && lastTime < oldestAssignTime)) {
                minCount = count;
                oldestAssignTime = lastTime;
                bestWs = ws;
            }
        }
    });

    if (bestWs) {
        let info = dashboardWorkers.get(bestWs);
        task.assignedTo = info.workerId;      // Standard visibility
        task.assignedTab = info.tabId;        // Perfect TAB Load Balancing
        info.lastAssigned = Date.now();
        bestWs.send(JSON.stringify({ type: 'new_task', data: task }));
    }
}

function getCountsData() {
    return {
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length
    };
}

wss.on('connection', (ws) => {
    let boundTaskId = null;
    let isDashboard = false;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            if (data.action === 'ping') return; 
            
            if (data.action === 'register' && data.taskId) {
                boundTaskId = data.taskId;
                if (!browserSockets.has(boundTaskId)) {
                    browserSockets.set(boundTaskId, new Set());
                }
                browserSockets.get(boundTaskId).add(ws);

                if (hcaptchaTrained[boundTaskId]) {
                    ws.send(JSON.stringify({
                        action: 'solve',
                        taskId: boundTaskId,
                        clicks: hcaptchaTrained[boundTaskId].clicks
                    }));
                }
                return;
            }

            if (data.action === 'dashboard') {
                isDashboard = true;
                // Save the tabId received from the client for accurate routing
                dashboardWorkers.set(ws, { tabId: data.tabId, workerId: data.workerId, mode: data.mode, lastAssigned: 0 });
                
                ws.send(JSON.stringify({ type: 'counts', data: getCountsData() }));
                
                for (let taskId in hcaptchaPending) {
                    if (!hcaptchaPending[taskId].assignedTab) {
                        let taskMode = (hcaptchaPending[taskId].media && hcaptchaPending[taskId].media.length > 1) ? 'grid' : 'manual';
                        if (taskMode === data.mode) assignTask(taskId);
                    }
                }
                return;
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        if (boundTaskId && browserSockets.has(boundTaskId)) {
            const set = browserSockets.get(boundTaskId);
            set.delete(ws);
            if (set.size === 0) browserSockets.delete(boundTaskId);
        }
        if (isDashboard && dashboardWorkers.has(ws)) {
            let info = dashboardWorkers.get(ws);
            dashboardWorkers.delete(ws);
            
            for (let taskId in hcaptchaPending) {
                if (hcaptchaPending[taskId].assignedTab === info.tabId) {
                    hcaptchaPending[taskId].assignedTab = null;
                    hcaptchaPending[taskId].assignedTo = null;
                    assignTask(taskId);
                }
            }
        }
    });
});

app.post('/api/new-hcaptcha', (req, res) => {
    const task = req.body;
    if (!task || !task.taskId) return res.json({ success: false, error: 'Missing taskId' });

    let autoRes = evaluateAutoSolve(task);
    if (autoRes.solved) {
        notifyBrowsers(task.taskId, autoRes.clicks);
        return res.json({ success: true, autoSolved: true, clicks: autoRes.clicks });
    }

    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) {
        keys.slice(0, 15).forEach(k => delete hcaptchaPending[k]);
    }

    hcaptchaPending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media,
        timestamp: task.timestamp,
        assignedTo: null,
        assignedTab: null
    };

    assignTask(task.taskId); 
    broadcastDashboard('counts', getCountsData()); 

    res.json({ success: true, autoSolved: false });
});

app.post('/api/submit-hcaptcha', (req, res) => {
    const { taskId, clicks } = req.body;
    if (!taskId || !clicks || clicks.length === 0) return res.json({ success: false });

    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    if (source) {
        let lightMedia = (source.media || []).map(m => ({
            dhash: m.dhash || "", stableHash: m.stableHash || "", type: m.type || "image",
            index: m.index !== undefined ? m.index : 0, thumb: m.thumb || (m.frames ? m.frames[0] : "")
        }));
        let cKey = getCleanKey(source);
        if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
        clicks.forEach(idx => {
            if (typeof idx === 'number' && lightMedia[idx] && lightMedia[idx].dhash && lightMedia[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(lightMedia[idx].dhash);
            }
        });
        hcaptchaTrained[taskId] = {
            id: taskId, prompt: source.prompt, conceptKey: cKey, media: lightMedia, clicks: clicks, trainedAt: new Date().toISOString()
        };

        const trainedKeys = Object.keys(hcaptchaTrained);
        if (trainedKeys.length > MAX_TRAINED) {
            Object.keys(hcaptchaTrained).slice(0, trainedKeys.length - MAX_TRAINED).forEach(k => delete hcaptchaTrained[k]);
        }

        delete hcaptchaPending[taskId];
        persistDatabase();
        notifyBrowsers(taskId, clicks);
        broadcastDashboard('task_solved', { taskId });
        broadcastDashboard('counts', getCountsData());
    }
    res.json({ success: true });
});

app.get('/api/ai-brain', (req, res) => {
    res.json(globalAIBrain);
});

app.post('/api/sync-ai-brain-batch', (req, res) => {
    const { workerId, updates } = req.body;
    if (updates && Array.isArray(updates)) {
        let changed = false;
        updates.forEach(u => {
            let { label, featureArr } = u;
            
            const WEIGHT = 10;
            let heavyFeatures = [];
            for(let i=0; i<WEIGHT; i++) {
                heavyFeatures.push(...featureArr);
            }

            if (!globalAIBrain[label]) {
                globalAIBrain[label] = { data: heavyFeatures, shape: [WEIGHT, 1280] };
                changed = true;
            } else {
                const old = globalAIBrain[label];
                const MAX_EXAMPLES = 5000; 
                const currentN = old.shape[0];
                
                if (currentN >= MAX_EXAMPLES) {
                    old.data.splice(0, 1280 * WEIGHT);
                    old.data.push(...heavyFeatures);
                } else {
                    old.data.push(...heavyFeatures);
                    old.shape[0] += WEIGHT;
                }
                changed = true;
            }
        });
        if (changed) persistAIBrain();
        broadcastDashboard('sync_ai_batch', { workerId, updates });
    }
    res.json({ success: true });
});

app.post('/api/reset-ai-brain', (req, res) => {
    globalAIBrain = {};
    persistAIBrain();
    res.json({ success: true });
});

app.get('/api/tasks', (req, res) => {
    const tab = req.query.tab === 'trained' ? 'trained' : 'pending';
    const workerId = req.query.workerId;
    const tabId = req.query.tabId;
    const page = Math.max(0, parseInt(req.query.page) || 0);
    const size = Math.min(40, Math.max(1, parseInt(req.query.size) || DASHBOARD_PAGE_SIZE));

    let source = tab === 'trained' ? hcaptchaTrained : hcaptchaPending;
    let ids = Object.keys(source);

    if (tab === 'pending') {
        if (tabId) {
            ids = ids.filter(id => hcaptchaPending[id].assignedTab === tabId);
        } else if (workerId) {
            ids = ids.filter(id => hcaptchaPending[id].assignedTo === workerId);
        }
    }

    if (tab === 'trained') ids = ids.reverse();
    let total = ids.length;
    let pageIds = ids.slice(page * size, (page + 1) * size);
    let tasks = {};
    pageIds.forEach(id => { tasks[id] = source[id]; });

    res.json({ tasks, total, page, pages: Math.ceil(total / size), tab });
});

app.get('/api/counts', (req, res) => res.json(getCountsData()));
app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    rebuildConceptBank(); persistDatabase();
    broadcastDashboard('task_deleted', { taskId: req.params.id });
    broadcastDashboard('counts', getCountsData());
    res.json({ success: true });
});

app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    if (fs.existsSync(f)) res.sendFile(f);
    else res.status(404).send('hcaptcha-dashboard.html not found.');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Master Server Live on Port ${PORT}`));
