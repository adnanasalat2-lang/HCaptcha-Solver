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

const MAX_PENDING = 80;
const MAX_TRAINED = 50000;
const DASHBOARD_PAGE_SIZE = 40;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {}; // { [cleanPrompt]: Set<dhash> }

const browserSockets = new Map();
const dashboardSockets = new Set();

function getCleanKey(task) {
    let p = (task.prompt || "").split("|||")[0].toLowerCase();
    return p.replace(/[^a-z0-9]/g, ' ').replace(/\s+/g, ' ').trim();
}

function hexToBin(h) {
    if (!h || h === "0000000000000000") return "";
    if (/^[01]+$/.test(h)) return h;
    let s = "";
    for (let c of h) {
        let v = parseInt(c, 16);
        if (isNaN(v)) return h;
        s += v.toString(2).padStart(4, '0');
    }
    return s;
}

function getHammingDistance(h1, h2) {
    let b1 = hexToBin(h1), b2 = hexToBin(h2);
    if (!b1 || !b2 || b1.length !== b2.length) return 999;
    let diff = 0;
    for (let i = 0; i < b1.length; i++) { if (b1[i] !== b2[i]) diff++; }
    return diff;
}

function rebuildConceptBank() {
    conceptBank = {};
    for (let id in hcaptchaTrained) {
        _addToConceptBank(hcaptchaTrained[id]);
    }
    console.log(`[Concept Bank] Rebuilt. Total Concepts: ${Object.keys(conceptBank).length}`);
}

function _addToConceptBank(tr) {
    let cKey = getCleanKey(tr);
    if (!cKey) return;
    if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
    (tr.clicks || []).forEach(idx => {
        if (typeof idx === 'number' && tr.media && tr.media[idx]) {
            let h = tr.media[idx].dhash || tr.media[idx].stableHash;
            if (h && h !== "0000000000000000") {
                conceptBank[cKey].add(h);
            }
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
}
initDB();

let saveTimeout = null;
function persistDatabase() {
    if (saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
        fs.writeFile(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8', (err) => {
            if (err) console.error('[DB] PERSIST ERROR:', err.message);
        });
    }, 2000);
}

function evaluateAutoSolve(task) {
    if (hcaptchaTrained[task.taskId]) {
        return { solved: true, clicks: hcaptchaTrained[task.taskId].clicks || [] };
    }

    const isVideoOrCoord = task.media && (task.media.length === 1 || task.media[0].type === 'video_frames' || task.media[0].frames);
    if (isVideoOrCoord) return { solved: false };

    let cKey = getCleanKey(task);
    let targetHashes = conceptBank[cKey];

    if (targetHashes && targetHashes.size > 0 && task.media && task.media.length > 1) {
        let matchedClicks = [];
        task.media.forEach((item, idx) => {
            let h = item.dhash || item.stableHash;
            if (!h || h === "0000000000000000") return;
            for (let savedHash of targetHashes) {
                if (getHammingDistance(h, savedHash) <= 4) {
                    matchedClicks.push(idx);
                    break;
                }
            }
        });

        if (matchedClicks.length >= 1 && matchedClicks.length <= 6) {
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
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(payload);
            }
        });
        browserSockets.delete(taskId);
    }
}

function notifyDashboard(type, data) {
    if (!dashboardSockets.size) return;
    const msg = JSON.stringify({ type, data });
    dashboardSockets.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(msg);
        }
    });
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
                dashboardSockets.add(ws);
                ws.send(JSON.stringify({ type: 'counts', data: getCountsData() }));
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
        if (isDashboard) {
            dashboardSockets.delete(ws);
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
        timestamp: task.timestamp
    };

    notifyDashboard('new_task', hcaptchaPending[task.taskId]);
    notifyDashboard('counts', getCountsData());

    res.json({ success: true, autoSolved: false });
});

app.post('/api/submit-hcaptcha', (req, res) => {
    const { taskId, clicks } = req.body;
    if (!taskId) return res.json({ success: false, error: 'Missing taskId' });

    if (!clicks || !Array.isArray(clicks) || clicks.length === 0) {
        return res.json({ success: false, error: 'Empty clicks blocked' });
    }

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
        if (cKey) {
            if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
            clicks.forEach(idx => {
                if (typeof idx === 'number' && lightMedia[idx]) {
                    let h = lightMedia[idx].dhash || lightMedia[idx].stableHash;
                    if (h && h !== "0000000000000000") conceptBank[cKey].add(h);
                }
            });
        }

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
        notifyDashboard('task_solved', { taskId });
        notifyDashboard('counts', getCountsData());
    }
    res.json({ success: true });
});

app.get('/api/tasks', (req, res) => {
    const tab = req.query.tab === 'trained' ? 'trained' : 'pending';
    const page = Math.max(0, parseInt(req.query.page) || 0);
    const size = Math.min(500, Math.max(1, parseInt(req.query.size) || DASHBOARD_PAGE_SIZE));

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
    res.json(getCountsData());
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    rebuildConceptBank(); 
    persistDatabase();
    notifyDashboard('task_deleted', { taskId: req.params.id });
    notifyDashboard('counts', getCountsData());
    res.json({ success: true });
});

app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    if (fs.existsSync(f)) res.sendFile(f);
    else res.status(404).send('hcaptcha-dashboard.html not found.');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Master Server Live on Port ${PORT}`));
