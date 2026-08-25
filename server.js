// ============================================================
// server.js — Instant Fast-Lane Relay & Video Safe Matching
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
app.use(express.json({ limit: '45mb' }));

const wss = new WS.Server({ server, perMessageDeflate: false });

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE  = path.join(DATA_DIR, 'database.json');
const AI_FILE  = path.join(DATA_DIR, 'ai_brain.json');

const MAX_PENDING = 60;
let pending   = {};
let trained   = {};
let aiBrainDS = {};

const bMap = new Map(); // taskId -> Set of extension websockets
const dSet = new Set(); // Dashboard websockets

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            let d = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
            trained = d.trained || {};
            console.log(`[DB] Loaded: ${Object.keys(trained).length} trained records.`);
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
        try { 
            fs.writeFileSync(DB_FILE, JSON.stringify({ trained }), 'utf8'); 
        } catch(e) {}
    }, 1200);
}

function pushBrowserInstant(taskId, clicks) {
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
        trained: Object.keys(trained).length
    };
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', raw => {
        try {
            let msg = JSON.parse(raw);
            
            // Fast-Lane instant submission directly from Dashboard over WS
            if (msg.action === 'submit' && msg.taskId && msg.clicks) {
                pushBrowserInstant(msg.taskId, msg.clicks);
                return;
            }

            if (msg.action === 'register' && msg.taskId) {
                ws.type = 'browser';
                ws.taskId = msg.taskId;
                if (!bMap.has(msg.taskId)) bMap.set(msg.taskId, new Set());
                bMap.get(msg.taskId).add(ws);

                if (trained[msg.taskId]) {
                    ws.send(JSON.stringify({ 
                        action: 'solve', 
                        taskId: msg.taskId, 
                        clicks: trained[msg.taskId].clicks 
                    }));
                }
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
            if (s) { 
                s.delete(ws); 
                if (!s.size) bMap.delete(ws.taskId); 
            }
        }
        if (ws.type === 'dashboard') dSet.delete(ws);
    });
});

setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 25000);

app.post('/api/new-hcaptcha', (req, res) => {
    let task = req.body;
    if (!task?.taskId) return res.json({ success: false });

    // Exact ID match only
    if (trained[task.taskId] && trained[task.taskId].clicks?.length) {
        pushBrowserInstant(task.taskId, trained[task.taskId].clicks);
        return res.json({ success: true, autoSolved: true, clicks: trained[task.taskId].clicks });
    }

    let pKeys = Object.keys(pending);
    if (pKeys.length >= MAX_PENDING) {
        pKeys.slice(0, 15).forEach(k => delete pending[k]);
    }

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
    if (!taskId || !clicks || !Array.isArray(clicks) || clicks.length === 0) {
        return res.json({ success: false, error: 'Invalid submission' });
    }

    pushBrowserInstant(taskId, clicks);
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

        trained[taskId] = {
            id: taskId,
            prompt: src.prompt,
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
    let src = tab === 'trained' ? trained : pending;
    let ids = Object.keys(src);
    if (tab === 'trained') ids = ids.reverse();
    
    let size = tab === 'trained' ? 15 : 40;
    let pageIds = ids.slice(0, size);
    let tasks = {};
    pageIds.forEach(id => { tasks[id] = src[id]; });
    
    res.json({ tasks, total: ids.length });
});

app.get('/api/counts', (req, res) => res.json(getCounts()));

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete pending[req.params.id];
    delete trained[req.params.id];
    saveDB();
    pushDash('task_deleted', { taskId: req.params.id });
    pushDash('counts', getCounts());
    res.json({ success: true });
});

app.get('/api/ai-brain', (req, res) => res.json(aiBrainDS));
app.post('/api/ai-brain', (req, res) => {
    aiBrainDS = req.body || {};
    try { fs.writeFileSync(AI_FILE, JSON.stringify(aiBrainDS), 'utf8'); } catch(e) {}
    res.json({ success: true });
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
server.listen(PORT, () => console.log(`🚀 Master Server 3.0 Live on Port ${PORT}`));
