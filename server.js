// ============================================================
// server.js — Railway Optimized (Max Performance)
// Target: 3,000-5,000 browsers on $5 Hobby plan
// ============================================================

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();

// ── Performance: Response compression + JSON limit ──
app.use(cors());
app.use(express.json({ limit: '25mb' })); // 60mb → 25mb (memory save)

const server = http.createServer(app);

// ── WebSocket: Ping/Pong se dead connections saaf karo ──
const wss = new WebSocket.Server({ 
    server,
    maxPayload: 1024 * 50, // 50KB max per message
    perMessageDeflate: false // CPU save
});

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');

// ── Limits: Railway memory ke hisaab se ──
const MAX_PENDING = 200;      // Zyada tasks pending rakh sakte hain
const MAX_TRAINED = 50000;
const PAGE_SIZE = 20;

// ── In-Memory Store ──
let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};          // prompt → Set of dhashes
let taskIdIndex = {};          // taskId → quick lookup

// ── WebSocket connections: taskId → Set of ws ──
const browserSockets = new Map();

// ── Hamming Distance (fast) ──
function getHammingDistance(h1, h2) {
    if (!h1 || !h2 || h1.length !== h2.length) return 999;
    try {
        let b1 = BigInt('0b' + h1);
        let b2 = BigInt('0b' + h2);
        let xor = b1 ^ b2;
        let diff = 0;
        while (xor > 0n) { diff += Number(xor & 1n); xor >>= 1n; }
        return diff;
    } catch(e) {
        let diff = 0;
        for (let i = 0; i < h1.length; i++) if (h1[i] !== h2[i]) diff++;
        return diff;
    }
}

function getCleanKey(task) {
    return "TXT_" + (task.prompt || "").split("|||")[0].trim().toLowerCase();
}

// ── Concept Bank: fast rebuild ──
function rebuildConceptBank() {
    conceptBank = {};
    taskIdIndex = {};
    for (let id in hcaptchaTrained) {
        let tr = hcaptchaTrained[id];
        taskIdIndex[id] = true;
        if (!tr.media || tr.media.length <= 1) continue;
        let cKey = getCleanKey(tr);
        if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
        (tr.clicks || []).forEach(idx => {
            if (typeof idx === 'number' && tr.media[idx]?.dhash && tr.media[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(tr.media[idx].dhash);
            }
        });
    }
}

// ── Auto-Solve Engine ──
function evaluateAutoSolve(task) {
    // 1. Exact match — sabse fast
    if (hcaptchaTrained[task.taskId]) {
        return { solved: true, clicks: hcaptchaTrained[task.taskId].clicks || [] };
    }

    // 2. Single image / canvas task
    if (!task.media || task.media.length <= 1) return { solved: false };

    // 3. Grid match via concept bank
    let cKey = getCleanKey(task);
    let targetDhashes = conceptBank[cKey];
    if (!targetDhashes || targetDhashes.size === 0) return { solved: false };

    let matchedClicks = [];
    for (let i = 0; i < task.media.length; i++) {
        let item = task.media[i];
        if (!item.dhash || item.dhash === "0000000000000000") continue;
        for (let savedHash of targetDhashes) {
            if (getHammingDistance(item.dhash, savedHash) <= 2) {
                matchedClicks.push(i);
                break;
            }
        }
    }

    // Min 2, max 5 matches — reliable range
    if (matchedClicks.length >= 2 && matchedClicks.length <= 5) {
        return { solved: true, clicks: matchedClicks };
    }
    return { solved: false };
}

// ── Database: Debounced save (memory efficient) ──
let saveTimer = null;
function persistDB() {
    if (saveTimer) clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
        try {
            // Sirf trained save karo — pending volatile hai
            let data = JSON.stringify({ trained: hcaptchaTrained });
            fs.writeFileSync(DB_FILE, data, 'utf8');
        } catch(e) { console.error('[DB] Save error:', e.message); }
    }, 3000); // 3 sec debounce
}

// ── DB Load ──
function initDB() {
    if (!fs.existsSync(DB_FILE)) return;
    try {
        let data = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
        hcaptchaTrained = data.trained || {};

        // Trim if over limit
        let keys = Object.keys(hcaptchaTrained);
        if (keys.length > MAX_TRAINED) {
            keys.slice(0, keys.length - MAX_TRAINED).forEach(k => delete hcaptchaTrained[k]);
        }

        rebuildConceptBank();
        console.log(`[DB] Loaded — Trained: ${Object.keys(hcaptchaTrained).length} | Concepts: ${Object.keys(conceptBank).length}`);
    } catch(e) {
        console.error('[DB] Load error:', e.message);
    }
}
initDB();

// ── WebSocket: Dead connection cleanup ──
const WS_PING_INTERVAL = 30000; // 30 sec
setInterval(() => {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) { ws.terminate(); return; }
        ws.isAlive = false;
        ws.ping();
    });
}, WS_PING_INTERVAL);

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    let boundTaskId = null;
    ws.on('message', (msg) => {
        try {
            let data = JSON.parse(msg);
            if (data.action === 'register' && data.taskId) {
                boundTaskId = data.taskId;
                if (!browserSockets.has(boundTaskId)) browserSockets.set(boundTaskId, new Set());
                browserSockets.get(boundTaskId).add(ws);

                // Agar already trained hai to turant bhejo
                if (hcaptchaTrained[boundTaskId]) {
                    ws.send(JSON.stringify({
                        action: 'solve',
                        taskId: boundTaskId,
                        clicks: hcaptchaTrained[boundTaskId].clicks
                    }));
                }
            }
        } catch(e) {}
    });

    ws.on('close', () => {
        if (boundTaskId && browserSockets.has(boundTaskId)) {
            let set = browserSockets.get(boundTaskId);
            set.delete(ws);
            if (set.size === 0) browserSockets.delete(boundTaskId);
        }
    });

    ws.on('error', () => ws.terminate());
});

function notifyBrowsers(taskId, clicks) {
    if (!browserSockets.has(taskId)) return;
    let payload = JSON.stringify({ action: 'solve', taskId, clicks });
    let sockets = browserSockets.get(taskId);
    sockets.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) ws.send(payload);
    });
    browserSockets.delete(taskId);
}

// ============================================================
// API ROUTES
// ============================================================

// ── New Task ──
app.post('/api/new-hcaptcha', (req, res) => {
    let task = req.body;
    if (!task?.taskId) return res.json({ success: false, error: 'Missing taskId' });

    // Auto-solve check
    let autoRes = evaluateAutoSolve(task);
    if (autoRes.solved) {
        notifyBrowsers(task.taskId, autoRes.clicks);
        return res.json({ success: true, autoSolved: true, clicks: autoRes.clicks });
    }

    // Pending mein add karo
    let keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) {
        // Purane 20 hata do
        keys.slice(0, 20).forEach(k => delete hcaptchaPending[k]);
    }

    hcaptchaPending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt || "",
        media: task.media || [],
        timestamp: Date.now()
    };

    res.json({ success: true, autoSolved: false });
});

// ── Submit Solution ──
app.post('/api/submit-hcaptcha', (req, res) => {
    let { taskId, clicks } = req.body;
    if (!taskId) return res.json({ success: false, error: 'Missing taskId' });
    if (!clicks || !Array.isArray(clicks) || clicks.length === 0) {
        return res.json({ success: false, error: 'Empty clicks' });
    }

    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    if (!source) return res.json({ success: false, error: 'Task not found' });

    // Light media — thumb size limit
    let lightMedia = (source.media || []).map(m => ({
        dhash: m.dhash || "",
        stableHash: m.stableHash || "",
        type: m.type || "image",
        index: m.index ?? 0,
        // Thumb: max 300 chars (URL ya chhota base64 prefix)
        thumb: (m.thumb || m.src || "").substring(0, 300)
    }));

    // Concept bank update
    let cKey = getCleanKey(source);
    if (lightMedia.length > 1) {
        if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
        clicks.forEach(idx => {
            if (typeof idx === 'number' && lightMedia[idx]?.dhash && lightMedia[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(lightMedia[idx].dhash);
            }
        });
    }

    hcaptchaTrained[taskId] = {
        id: taskId,
        prompt: source.prompt,
        conceptKey: cKey,
        media: lightMedia,
        clicks,
        trainedAt: new Date().toISOString()
    };

    // Trim if needed
    let tKeys = Object.keys(hcaptchaTrained);
    if (tKeys.length > MAX_TRAINED) {
        tKeys.slice(0, tKeys.length - MAX_TRAINED).forEach(k => delete hcaptchaTrained[k]);
    }

    delete hcaptchaPending[taskId];
    persistDB();
    notifyBrowsers(taskId, clicks);
    res.json({ success: true });
});

// ── Tasks API (Paged) ──
app.get('/api/tasks', (req, res) => {
    let tab = req.query.tab === 'trained' ? 'trained' : 'pending';
    let page = Math.max(0, parseInt(req.query.page) || 0);
    let size = tab === 'pending' ? 200 : PAGE_SIZE;
    let source = tab === 'trained' ? hcaptchaTrained : hcaptchaPending;
    let ids = Object.keys(source);
    if (tab === 'trained') ids = ids.reverse();
    let total = ids.length;
    let pageIds = ids.slice(page * size, (page + 1) * size);
    let tasks = {};
    pageIds.forEach(id => { tasks[id] = source[id]; });
    res.json({ tasks, total, page, pages: Math.ceil(total / size) || 1, tab });
});

// ── Counts ──
app.get('/api/counts', (req, res) => {
    res.json({
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length
    });
});

// ── Delete ──
app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    rebuildConceptBank();
    persistDB();
    res.json({ success: true });
});

// ── Health Check ──
app.get('/api/health', (req, res) => {
    let mem = process.memoryUsage();
    res.json({
        status: 'ok',
        uptime: Math.floor(process.uptime()),
        memory_mb: Math.floor(mem.heapUsed / 1024 / 1024),
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length,
        ws_connections: wss.clients.size
    });
});

// ── AI Brain (Dashboard ke liye) ──
let aiBrainDataset = {};
const AI_FILE = path.join(DATA_DIR, 'ai_brain.json');
if (fs.existsSync(AI_FILE)) {
    try { aiBrainDataset = JSON.parse(fs.readFileSync(AI_FILE, 'utf8')); } catch(e) {}
}

app.get('/api/ai-brain', (req, res) => res.json(aiBrainDataset));
app.post('/api/ai-brain', (req, res) => {
    if (req.body && typeof req.body === 'object') {
        aiBrainDataset = req.body;
        setTimeout(() => {
            try { fs.writeFileSync(AI_FILE, JSON.stringify(aiBrainDataset), 'utf8'); } catch(e) {}
        }, 3000);
        return res.json({ success: true });
    }
    res.json({ success: false });
});
app.delete('/api/ai-brain', (req, res) => {
    aiBrainDataset = {};
    try { fs.writeFileSync(AI_FILE, '{}', 'utf8'); } catch(e) {}
    res.json({ success: true });
});

// ── Dashboard ──
app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    if (fs.existsSync(f)) res.sendFile(f);
    else res.status(404).send('Dashboard not found');
});
app.get('/dashboard', (req, res) => res.redirect('/'));
app.get('/favicon.ico', (req, res) => res.status(204).end());

// ── Memory Monitor (Railway ke liye) ──
setInterval(() => {
    let mem = process.memoryUsage();
    let mb = Math.floor(mem.heapUsed / 1024 / 1024);
    if (mb > 400) {
        // Memory high — purana pending saaf karo
        let now = Date.now();
        let oldPending = Object.keys(hcaptchaPending).filter(k => 
            now - (hcaptchaPending[k].timestamp || 0) > 60000 // 60 sec purana
        );
        oldPending.forEach(k => delete hcaptchaPending[k]);
        console.log(`[MEM] ${mb}MB — Cleaned ${oldPending.length} old pending tasks`);
    }
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📊 Memory: ${Math.floor(process.memoryUsage().heapUsed/1024/1024)}MB`);
});
