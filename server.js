// ============================================================
// server.js — 50K Browser Scale Edition
// WebSocket-first | Zero polling | Memory-optimized
// ============================================================
'use strict';

const express = require('express');
const http    = require('http');
const WS      = require('ws');
const cors    = require('cors');
const fs      = require('fs');
const path    = require('path');

const app    = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json({ limit: '25mb' }));

// ── WebSocket Server ──────────────────────────────────────────
const wss = new WS.Server({
    server,
    perMessageDeflate: false,   // CPU save
    maxPayload: 64 * 1024       // 64KB max
});

// ── Storage ──────────────────────────────────────────────────
const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE  = path.join(DATA_DIR, 'database.json');

const MAX_PENDING = 500;
const MAX_TRAINED = 50000;
const PAGE_SIZE   = 20;

let pending  = {};   // taskId → task
let trained  = {};   // taskId → solved task
let bank     = {};   // promptKey → Set<dhash>

// ── Browser WebSocket Map ─────────────────────────────────────
// taskId → Set<ws>  — jab solve hota hai push karo
const bMap = new Map();

// ── Dashboard WebSocket Set ───────────────────────────────────
const dSet = new Set();

// ── DB ───────────────────────────────────────────────────────
function initDB() {
    if (!fs.existsSync(DB_FILE)) return;
    try {
        let d = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
        trained = d.trained || {};
        let keys = Object.keys(trained);
        if (keys.length > MAX_TRAINED)
            keys.slice(0, keys.length - MAX_TRAINED).forEach(k => delete trained[k]);
        rebuildBank();
        console.log(`[DB] Trained:${Object.keys(trained).length} Bank:${Object.keys(bank).length}`);
    } catch(e) { console.error('[DB]', e.message); }
}

let _saveTimer = null;
function saveDB() {
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(() => {
        try { fs.writeFileSync(DB_FILE, JSON.stringify({ trained }), 'utf8'); }
        catch(e) { console.error('[DB] Save:', e.message); }
    }, 3000);
}

// ── Concept Bank ─────────────────────────────────────────────
function pKey(t) {
    return 'K_' + (t.prompt || '').split('|||')[0].trim().toLowerCase();
}
function rebuildBank() {
    bank = {};
    for (let id in trained) _addBank(trained[id]);
}
function _addBank(tr) {
    if (!tr.media || tr.media.length <= 1) return;
    let k = pKey(tr);
    if (!bank[k]) bank[k] = new Set();
    (tr.clicks || []).forEach(i => {
        let m = tr.media[i];
        if (m?.dhash && m.dhash !== '0000000000000000') bank[k].add(m.dhash);
    });
}

// ── Hamming ──────────────────────────────────────────────────
function ham(h1, h2) {
    if (!h1 || !h2 || h1.length !== h2.length) return 999;
    try {
        let x = BigInt('0b' + h1) ^ BigInt('0b' + h2), d = 0;
        while (x > 0n) { d += Number(x & 1n); x >>= 1n; }
        return d;
    } catch(e) {
        let d = 0;
        for (let i = 0; i < h1.length; i++) if (h1[i] !== h2[i]) d++;
        return d;
    }
}

// ── Auto Solve ───────────────────────────────────────────────
function autoSolve(task) {
    // 1. Exact match
    if (trained[task.taskId])
        return { ok: true, clicks: trained[task.taskId].clicks || [] };

    // 2. Grid match — strict: bank >= 5 examples, hamming <= 2
    if (task.media?.length > 1) {
        let k = pKey(task), b = bank[k];
        if (b && b.size >= 5) {
            let hits = [];
            task.media.forEach((m, i) => {
                if (!m.dhash || m.dhash === '0000000000000000') return;
                for (let h of b) if (ham(m.dhash, h) <= 2) { hits.push(i); break; }
            });
            // 2-5 hits AND max 55% of grid
            if (hits.length >= 2 && hits.length <= 5 &&
                hits.length / task.media.length <= 0.55)
                return { ok: true, clicks: hits };
        }
    }
    return { ok: false };
}

// ── Push helpers ─────────────────────────────────────────────
function pushBrowser(taskId, clicks) {
    let sockets = bMap.get(taskId);
    if (!sockets) return;
    let msg = JSON.stringify({ action: 'solve', taskId, clicks });
    sockets.forEach(ws => {
        if (ws.readyState === WS.OPEN) ws.send(msg);
    });
    bMap.delete(taskId);
}

function pushDash(type, data) {
    if (!dSet.size) return;
    let msg = JSON.stringify({ type, data });
    dSet.forEach(ws => {
        if (ws.readyState === WS.OPEN) ws.send(msg);
    });
}

function getCounts() {
    return {
        pending:  Object.keys(pending).length,
        trained:  Object.keys(trained).length,
        concepts: Object.keys(bank).length,
        browsers: bMap.size,
        dashboards: dSet.size
    };
}

// ── WebSocket Handler ─────────────────────────────────────────
wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('error', () => ws.terminate());

    ws.on('message', raw => {
        try {
            let msg = JSON.parse(raw);

            // Browser register — taskId ke saath
            if (msg.action === 'register' && msg.taskId) {
                ws.type = 'browser';
                ws.taskId = msg.taskId;
                if (!bMap.has(msg.taskId)) bMap.set(msg.taskId, new Set());
                bMap.get(msg.taskId).add(ws);

                // Already trained? Foran bhejo
                if (trained[msg.taskId]) {
                    ws.send(JSON.stringify({
                        action: 'solve',
                        taskId: msg.taskId,
                        clicks: trained[msg.taskId].clicks
                    }));
                }
                return;
            }

            // Dashboard register
            if (msg.action === 'dashboard') {
                ws.type = 'dashboard';
                dSet.add(ws);
                ws.send(JSON.stringify({ type: 'counts', data: getCounts() }));
                return;
            }

            // Ping
            if (msg.type === 'ping') {
                ws.send(JSON.stringify({ type: 'pong' }));
                return;
            }
        } catch(e) {}
    });

    ws.on('close', () => {
        if (ws.type === 'browser' && ws.taskId) {
            let s = bMap.get(ws.taskId);
            if (s) { s.delete(ws); if (!s.size) bMap.delete(ws.taskId); }
        }
        if (ws.type === 'dashboard') dSet.delete(ws);
    });
});

// Ping/pong — dead connections hatao
setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.isAlive) { ws.terminate(); return; }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

// ── API ROUTES ───────────────────────────────────────────────

// Extension: naya task
app.post('/api/new-hcaptcha', (req, res) => {
    let task = req.body;
    if (!task?.taskId) return res.json({ success: false, error: 'Missing taskId' });

    let r = autoSolve(task);
    if (r.ok) {
        pushBrowser(task.taskId, r.clicks);
        return res.json({ success: true, autoSolved: true, clicks: r.clicks });
    }

    // Pending trim
    let pKeys = Object.keys(pending);
    if (pKeys.length >= MAX_PENDING)
        pKeys.slice(0, 30).forEach(k => delete pending[k]);

    pending[task.taskId] = {
        id:        task.taskId,
        prompt:    task.prompt || '',
        media:     task.media  || [],
        timestamp: Date.now()
    };

    // Dashboard ko real-time push — sirf metadata (media bhi)
    pushDash('new_pending', {
        id:        task.taskId,
        prompt:    task.prompt || '',
        media:     task.media  || [],
        timestamp: pending[task.taskId].timestamp
    });
    pushDash('counts', getCounts());

    res.json({ success: true, autoSolved: false });
});

// Extension: polling fallback (WebSocket nahi chala to)
app.get('/api/check-hcaptcha/:id', (req, res) => {
    let t = trained[req.params.id];
    res.json(t ? { status: 'solved', clicks: t.clicks || [] } : { status: 'pending' });
});

// Dashboard: paginated tasks
app.get('/api/tasks', (req, res) => {
    let tab  = req.query.tab === 'trained' ? 'trained' : 'pending';
    let page = Math.max(0, parseInt(req.query.page) || 0);
    let size = tab === 'pending' ? 200 : PAGE_SIZE;
    let src  = tab === 'trained' ? trained : pending;
    let ids  = Object.keys(src);
    if (tab === 'trained') ids = ids.reverse();
    let total   = ids.length;
    let pageIds = ids.slice(page * size, (page + 1) * size);
    let tasks   = {};
    pageIds.forEach(id => { tasks[id] = src[id]; });
    res.json({ tasks, total, page, pages: Math.ceil(total / size) || 1, tab });
});

// Dashboard: counts
app.get('/api/counts', (req, res) => res.json(getCounts()));

// Dashboard: submit solution
app.post('/api/submit-hcaptcha', (req, res) => {
    let { taskId, clicks } = req.body;
    if (!taskId) return res.json({ success: false });
    if (!clicks?.length) return res.json({ success: false, error: 'Empty clicks' });

    let src = pending[taskId] || trained[taskId];
    if (!src) return res.json({ success: false, error: 'Not found' });

    // Light media — thumb max 50KB
    let lm = (src.media || []).map(m => {
        let thumb = m.thumb || m.src || '';
        if (thumb.startsWith('data:image') && thumb.length > 50000)
            thumb = thumb.substring(0, 50000);
        return {
            dhash:      m.dhash      || '',
            stableHash: m.stableHash || '',
            type:       m.type       || 'image',
            index:      m.index      ?? 0,
            thumb,
            frames:     m.type === 'video_frames' ? (m.frames || []).slice(0, 3) : undefined
        };
    });

    // Bank update
    if (lm.length > 1) {
        let k = pKey(src);
        if (!bank[k]) bank[k] = new Set();
        clicks.forEach(i => {
            let m = lm[i];
            if (m?.dhash && m.dhash !== '0000000000000000') bank[k].add(m.dhash);
        });
    }

    trained[taskId] = {
        id:         taskId,
        prompt:     src.prompt,
        conceptKey: pKey(src),
        media:      lm,
        clicks,
        trainedAt:  new Date().toISOString()
    };

    // Trim
    let tKeys = Object.keys(trained);
    if (tKeys.length > MAX_TRAINED)
        tKeys.slice(0, tKeys.length - MAX_TRAINED).forEach(k => delete trained[k]);

    delete pending[taskId];
    saveDB();

    pushBrowser(taskId, clicks);
    pushDash('task_solved', { taskId, clicks });
    pushDash('counts', getCounts());

    res.json({ success: true });
});

// Restore
app.post('/api/restore-hcaptcha', (req, res) => {
    let { taskId } = req.body;
    if (trained[taskId]) {
        pending[taskId] = { ...trained[taskId], clicks: [], restoredAt: Date.now() };
        delete trained[taskId];
        rebuildBank();
        saveDB();
        pushDash('counts', getCounts());
    }
    res.json({ success: true });
});

// Delete
app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    let id = req.params.id;
    delete pending[id];
    delete trained[id];
    rebuildBank();
    saveDB();
    pushDash('task_deleted', { taskId: id });
    pushDash('counts', getCounts());
    res.json({ success: true });
});

// Health
app.get('/api/health', (req, res) => {
    let mem = process.memoryUsage();
    res.json({
        status:    'ok',
        uptime:    Math.floor(process.uptime()),
        memory_mb: Math.floor(mem.heapUsed / 1024 / 1024),
        ...getCounts()
    });
});

// Static files
app.get('/ai-worker.js', (req, res) =>
    res.sendFile(path.join(__dirname, 'ai-worker.js')));
app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    fs.existsSync(f) ? res.sendFile(f) : res.status(404).send('Dashboard not found');
});
app.get('/dashboard', (req, res) => res.redirect('/'));
app.get('/favicon.ico', (req, res) => res.status(204).end());

// Memory watchdog
setInterval(() => {
    let mb = Math.floor(process.memoryUsage().heapUsed / 1024 / 1024);
    if (mb > 420) {
        let now = Date.now();
        let old = Object.keys(pending).filter(k =>
            now - (pending[k].timestamp || 0) > 60000);
        old.forEach(k => delete pending[k]);
        if (old.length) console.log(`[MEM] ${mb}MB — Removed ${old.length} old pending`);
    }
}, 30000);

initDB();
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Port:${PORT} | WS ready | ${Math.floor(process.memoryUsage().heapUsed/1024/1024)}MB`);
});
