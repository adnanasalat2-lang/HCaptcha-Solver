const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const { WebSocketServer } = require('ws');
const Redis = require('ioredis');

const app = express();
app.use(cors());
app.use(express.json({ limit: '15mb' }));

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');

const MAX_PENDING = 1000;
const MAX_TRAINED = 10000;
const DASHBOARD_PAGE_SIZE = 20;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};

// Redis Initialization with fallback to in-memory mode
let redis = null;
let pubsub = null;
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

try {
    redis = new Redis(REDIS_URL, { retryStrategy: () => 2000, lazyConnect: true });
    pubsub = new Redis(REDIS_URL, { retryStrategy: () => 2000, lazyConnect: true });
    redis.connect().then(() => console.log('✅ Redis Connected')).catch(() => console.log('ℹ️ Running on In-Memory Fallback'));
    pubsub.connect().then(() => {
        pubsub.subscribe('hcaptcha_solutions', () => {});
    }).catch(() => {});
} catch (e) {
    console.log('ℹ️ Running in Pure In-Memory Mode');
}

function getCleanKey(task) {
    let p = (task.prompt || "").split("|||")[0].trim().toLowerCase();
    return "TXT_" + p;
}

function dhashToBigInt(hexOrBin) {
    if (!hexOrBin || hexOrBin === "0000000000000000") return null;
    if (/^[01]+$/.test(hexOrBin)) {
        return BigInt('0b' + hexOrBin);
    }
    try { return BigInt('0x' + hexOrBin); } catch (e) { return null; }
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

function _removeFromConceptBank() {
    rebuildConceptBank();
}

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            const raw = fs.readFileSync(DB_FILE, 'utf8');
            const data = JSON.parse(raw);
            hcaptchaPending = {};
            hcaptchaTrained = data.trained || {};

            const trainedKeys = Object.keys(hcaptchaTrained);
            if (trainedKeys.length > MAX_TRAINED) {
                const toDelete = trainedKeys.slice(0, trainedKeys.length - MAX_TRAINED);
                toDelete.forEach(k => delete hcaptchaTrained[k]);
            }
            rebuildConceptBank();
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
            if (err) console.error('[DB] Write error:', err.message);
        });
    }, 2000);
}

function evaluateAutoSolve(task) {
    if (hcaptchaTrained[task.taskId]) {
        return { solved: true, clicks: hcaptchaTrained[task.taskId].clicks || [] };
    }

    let cKey = getCleanKey(task);
    let targetDhashes = conceptBank[cKey];
    if (targetDhashes && targetDhashes.size > 0 && task.media && task.media.length > 1) {
        let matchedClicks = [];
        task.media.forEach((item, idx) => {
            if (!item.dhash || item.dhash === "0000000000000000") return;
            for (let savedHash of targetDhashes) {
                if (getHammingDistance(item.dhash, savedHash) <= 3) {
                    matchedClicks.push(idx);
                    break;
                }
            }
        });

        if (matchedClicks.length >= 2 && matchedClicks.length <= 5) {
            return { solved: true, clicks: matchedClicks };
        }
    }

    if (task.media && task.media.length === 1) {
        let item = task.media[0];
        if (item.dhash && item.dhash !== "0000000000000000") {
            for (let id in hcaptchaTrained) {
                let tr = hcaptchaTrained[id];
                if (tr.media && tr.media.length === 1 && getCleanKey(tr) === cKey) {
                    if (getHammingDistance(item.dhash, tr.media[0].dhash) <= 5) {
                        return { solved: true, clicks: tr.clicks };
                    }
                }
            }
        }
    }
    return { solved: false };
}

// ==========================================
// HTTP + WEBSOCKET ENGINE
// ==========================================
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const wsClients = new Map();

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (msgStr) => {
        try {
            const data = JSON.parse(msgStr);
            if (data.action === 'register' && data.taskId) {
                ws.taskId = data.taskId;
                wsClients.set(data.taskId, ws);

                if (hcaptchaTrained[data.taskId]) {
                    ws.send(JSON.stringify({ status: 'solved', taskId: data.taskId, clicks: hcaptchaTrained[data.taskId].clicks || [] }));
                }
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        if (ws.taskId) wsClients.delete(ws.taskId);
    });
});

// Ping interval for connection maintenance
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

if (pubsub) {
    pubsub.on('message', (channel, message) => {
        if (channel === 'hcaptcha_solutions') {
            try {
                const { taskId, clicks } = JSON.parse(message);
                const targetWs = wsClients.get(taskId);
                if (targetWs && targetWs.readyState === 1) {
                    targetWs.send(JSON.stringify({ status: 'solved', taskId, clicks }));
                }
            } catch (e) {}
        }
    });
}

function broadcastSolution(taskId, clicks) {
    const targetWs = wsClients.get(taskId);
    if (targetWs && targetWs.readyState === 1) {
        targetWs.send(JSON.stringify({ status: 'solved', taskId, clicks }));
    }
    if (redis && redis.status === 'ready') {
        redis.publish('hcaptcha_solutions', JSON.stringify({ taskId, clicks })).catch(() => {});
    }
}

// ROUTES
app.post('/api/new-hcaptcha', (req, res) => {
    const task = req.body;
    if (!task || !task.taskId) return res.json({ success: false, error: 'Missing taskId' });

    let autoRes = evaluateAutoSolve(task);
    if (autoRes.solved) {
        broadcastSolution(task.taskId, autoRes.clicks);
        return res.json({ success: true, autoSolved: true, clicks: autoRes.clicks });
    }

    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) {
        keys.slice(0, 50).forEach(k => delete hcaptchaPending[k]);
    }

    hcaptchaPending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media,
        timestamp: task.timestamp
    };

    res.json({ success: true, autoSolved: false });
});

app.get('/api/check-hcaptcha/:id', (req, res) => {
    const tid = req.params.id;
    if (hcaptchaTrained[tid]) {
        res.json({ status: 'solved', clicks: hcaptchaTrained[tid].clicks || [] });
    } else {
        res.json({ status: 'pending' });
    }
});

app.get('/api/tasks', (req, res) => {
    const tab = req.query.tab === 'trained' ? 'trained' : 'pending';
    const page = Math.max(0, parseInt(req.query.page) || 0);
    const size = Math.min(50, Math.max(1, parseInt(req.query.size) || DASHBOARD_PAGE_SIZE));

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
    res.json({
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length
    });
});

app.get('/api/get-hcaptcha', (req, res) => {
    res.json({ pending: hcaptchaPending, trained: hcaptchaTrained });
});

app.post('/api/submit-hcaptcha', (req, res) => {
    const { taskId, clicks } = req.body;
    if (!taskId) return res.json({ success: false, error: 'Missing taskId' });

    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    if (source) {
        let lightMedia = (source.media || []).map(m => ({
            dhash: m.dhash || "",
            stableHash: m.stableHash || "",
            type: m.type || "image",
            index: m.index !== undefined ? m.index : 0,
            thumb: m.thumb || ""
        }));

        let cKey = getCleanKey(source);
        if (!conceptBank[cKey]) conceptBank[cKey] = new Set();

        (clicks || []).forEach(idx => {
            if (typeof idx === 'number' && lightMedia[idx] && lightMedia[idx].dhash && lightMedia[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(lightMedia[idx].dhash);
            }
        });

        hcaptchaTrained[taskId] = {
            id: taskId,
            prompt: source.prompt,
            conceptKey: getCleanKey(source),
            media: lightMedia,
            clicks: clicks || [],
            trainedAt: new Date().toISOString()
        };

        const trainedKeys = Object.keys(hcaptchaTrained);
        if (trainedKeys.length > MAX_TRAINED) {
            const oldest = trainedKeys.slice(0, trainedKeys.length - MAX_TRAINED);
            oldest.forEach(k => delete hcaptchaTrained[k]);
        }

        delete hcaptchaPending[taskId];
        persistDatabase();
        broadcastSolution(taskId, clicks || []);
    }
    res.json({ success: true });
});

app.post('/api/restore-hcaptcha', (req, res) => {
    const { taskId } = req.body;
    if (!taskId) return res.json({ success: false, error: 'Missing taskId' });

    if (hcaptchaTrained[taskId]) {
        hcaptchaPending[taskId] = {
            ...hcaptchaTrained[taskId],
            clicks: [],
            restoredAt: new Date().toISOString()
        };
        delete hcaptchaTrained[taskId];
        _removeFromConceptBank();
        persistDatabase();
    }
    res.json({ success: true });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    _removeFromConceptBank();
    persistDatabase();
    res.json({ success: true });
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'ok',
        wsClients: wsClients.size,
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length
    });
});

app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    if (fs.existsSync(f)) res.sendFile(f);
    else res.status(404).send('hcaptcha-dashboard.html nahi mila. Server folder mein rakho.');
});
app.get('/dashboard', (req, res) => res.redirect('/'));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 High-Concurrency Engine on Port ${PORT}`));
