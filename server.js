// ============================================================
// server.js — 98% Fuzzy Match Precision Auto-Solver
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
app.use(express.json({ limit: '60mb' }));

const wss = new WS.Server({
    server,
    perMessageDeflate: false,
    maxPayload: 30 * 1024 * 1024
});

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE  = path.join(DATA_DIR, 'database.json');
const AI_FILE  = path.join(DATA_DIR, 'ai_brain.json');

const MAX_PENDING = 400;

let pending   = {};
let trained   = {};
let bank      = {};
let aiBrainDS = {};

const bMap = new Map();
const dSet = new Set();

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            let d = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
            trained = d.trained || {};
            rebuildBank();
        } catch(e) {}
    }
    if (fs.existsSync(AI_FILE)) {
        try {
            aiBrainDS = JSON.parse(fs.readFileSync(AI_FILE, 'utf8'));
        } catch(e) {}
    }
}

let _saveTimer = null;
function saveDB() {
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(() => {
        try { fs.writeFileSync(DB_FILE, JSON.stringify({ trained }), 'utf8'); } catch(e) {}
    }, 1500);
}

function pKey(t) {
    return 'K_' + (t.prompt || '').split('|||')[0].replace(/[\n\r\t]+/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
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

// 98% Fuzzy Tolerance String/Hash Matcher
function isSimilar(s1, s2, threshold = 0.98) {
    if (!s1 || !s2) return false;
    if (s1 === s2) return true;
    let maxLen = Math.max(s1.length, s2.length);
    if (maxLen === 0) return true;
    let matches = 0;
    let minLen = Math.min(s1.length, s2.length);
    for (let i = 0; i < minLen; i++) {
        if (s1[i] === s2[i]) matches++;
    }
    return (matches / maxLen) >= threshold;
}

function autoSolve(task) {
    // 1. Direct 98% Trained Match (Exact or 98% Near-Exact Task ID / Media Signature)
    if (trained[task.taskId]) {
        return { ok: true, clicks: trained[task.taskId].clicks || [] };
    }

    let taskPrompt = pKey(task);
    for (let trId in trained) {
        let tr = trained[trId];
        if (tr.conceptKey === taskPrompt) {
            let taskSig = task.media?.map(m => m.stableHash || m.dhash).join('');
            let trSig = tr.media?.map(m => m.stableHash || m.dhash).join('');
            if (isSimilar(taskSig, trSig, 0.98)) {
                return { ok: true, clicks: tr.clicks || [] };
            }
        }
    }

    // 2. AI Concept Bank (3x3 Grid Tasks with 98% DHash Precision)
    if (task.media && task.media.length > 1) {
        let k = taskPrompt, b = bank[k];
        if (b && b.size >= 4) {
            let hits = [];
            task.media.forEach((m, i) => {
                if (!m.dhash || m.dhash === '0000000000000000') return;
                // ham <= 3 means 95.3% to 98.4% visual similarity
                for (let h of b) if (ham(m.dhash, h) <= 3) { hits.push(i); break; }
            });
            if (hits.length >= 2 && hits.length <= 5 && hits.length / task.media.length <= 0.65) {
                return { ok: true, clicks: hits };
            }
        }
    }

    return { ok: false };
}

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
        pending: Object.keys(pending).length,
        trained: Object.keys(trained).length,
        concepts: Object.keys(bank).length,
        browsers: bMap.size,
        dashboards: dSet.size
    };
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('error', () => ws.terminate());

    ws.on('message', raw => {
        try {
            let msg = JSON.parse(raw);
            if (msg.action === 'register' && msg.taskId) {
                ws.type = 'browser';
                ws.taskId = msg.taskId;
                if (!bMap.has(msg.taskId)) bMap.set(msg.taskId, new Set());
                bMap.get(msg.taskId).add(ws);
                return;
            }

            if (msg.action === 'dashboard') {
                ws.type = 'dashboard';
                dSet.add(ws);
                ws.send(JSON.stringify({ type: 'counts', data: getCounts() }));
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

setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.isAlive) { ws.terminate(); return; }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

app.post('/api/new-hcaptcha', (req, res) => {
    let task = req.body;
    if (!task?.taskId) return res.json({ success: false });

    let r = autoSolve(task);
    if (r.ok) {
        pushBrowser(task.taskId, r.clicks);
        return res.json({ success: true, autoSolved: true, clicks: r.clicks });
    }

    let pKeys = Object.keys(pending);
    if (pKeys.length >= MAX_PENDING) pKeys.slice(0, 30).forEach(k => delete pending[k]);

    pending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt || '',
        media: task.media || [],
        timestamp: Date.now()
    };

    pushDash('new_pending', pending[task.taskId]);
    pushDash('counts', getCounts());
    res.json({ success: true, autoSolved: false });
});

app.post('/api/submit-hcaptcha', (req, res) => {
    let { taskId, clicks } = req.body;
    if (!taskId || !clicks?.length) return res.json({ success: false });

    pushBrowser(taskId, clicks);
    pushDash('task_solved', { taskId, clicks });
    res.json({ success: true });

    let src = pending[taskId] || trained[taskId];
    if (src) {
        let lm = (src.media || []).map(m => ({
            dhash: m.dhash || '',
            stableHash: m.stableHash || '',
            type: m.type || 'image',
            index: m.index ?? 0,
            thumb: m.thumb || m.src || ''
        }));

        let k = pKey(src);
        if (lm.length > 1) {
            if (!bank[k]) bank[k] = new Set();
            clicks.forEach(i => {
                let m = lm[i];
                if (m?.dhash && m.dhash !== '0000000000000000') bank[k].add(m.dhash);
            });
        }

        trained[taskId] = {
            id: taskId,
            prompt: src.prompt,
            conceptKey: k,
            media: lm,
            clicks,
            trainedAt: new Date().toISOString()
        };

        delete pending[taskId];
        saveDB();
        pushDash('counts', getCounts());
    }
});

app.get('/api/check-hcaptcha/:id', (req, res) => {
    let t = trained[req.params.id];
    res.json(t ? { status: 'solved', clicks: t.clicks || [] } : { status: 'pending' });
});

app.get('/api/tasks', (req, res) => {
    let tab = req.query.tab === 'trained' ? 'trained' : 'pending';
    let size = tab === 'trained' ? 10 : 200;
    let src = tab === 'trained' ? trained : pending;
    let ids = Object.keys(src);
    if (tab === 'trained') ids = ids.reverse();
    let pageIds = ids.slice(0, size);
    let tasks = {};
    pageIds.forEach(id => { tasks[id] = src[id]; });
    res.json({ tasks, total: ids.length });
});

app.get('/api/counts', (req, res) => res.json(getCounts()));

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete pending[req.params.id];
    delete trained[req.params.id];
    rebuildBank();
    saveDB();
    pushDash('task_deleted', { taskId: req.params.id });
    pushDash('counts', getCounts());
    res.json({ success: true });
});

app.get('/api/ai-brain', (req, res) => res.json(aiBrainDS));
app.post('/api/ai-brain', (req, res) => {
    if (req.body && typeof req.body === 'object') {
        aiBrainDS = req.body;
        try { fs.writeFileSync(AI_FILE, JSON.stringify(aiBrainDS), 'utf8'); } catch(e) {}
        return res.json({ success: true });
    }
    res.json({ success: false });
});
app.delete('/api/ai-brain', (req, res) => {
    aiBrainDS = {};
    try { fs.writeFileSync(AI_FILE, '{}', 'utf8'); } catch(e) {}
    res.json({ success: true });
});

app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    fs.existsSync(f) ? res.sendFile(f) : res.status(404).send('Dashboard not found');
});

initDB();
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 98% Precision Server live on port ${PORT}`);
});
