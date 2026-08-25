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

const MAX_PENDING = 60;
const MAX_TRAINED = 50000;
const DASHBOARD_PAGE_SIZE = 20;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};

const browserSockets = new Map();
const dashboardSockets = new Set();

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

function broadcastDashboard(payload) {
    const msg = JSON.stringify(payload);
    dashboardSockets.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    });
}

function broadcastCounts() {
    broadcastDashboard({
        action: 'counts_update',
        counts: {
            pending: Object.keys(hcaptchaPending).length,
            trained: Object.keys(hcaptchaTrained).length,
            concepts: Object.keys(conceptBank).length
        }
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

function handleNewTask(task, wsSource) {
    if (!task || !task.taskId) return { success: false };

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
        broadcastDashboard({ action: 'bulk_remove_pending', taskIds: deletedKeys });
    }

    const taskData = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media,
        timestamp: task.timestamp
    };
    hcaptchaPending[task.taskId] = taskData;

    broadcastDashboard({ action: 'new_task', task: taskData });
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

    // Direct Live Micro-Patch Broadcast
    broadcastDashboard({
        action: 'tile_patch',
        taskId,
        index,
        thumb,
        dhash
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
        persistDatabase();
        notifyBrowsers(taskId, clicks);

        broadcastDashboard({ action: 'task_solved', taskId, trainedTask: hcaptchaTrained[taskId] });
        broadcastCounts();
    }
    return { success: true };
}

wss.on('connection', (ws) => {
    let boundTaskId = null;
    let isDashboard = false;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (data.action === 'register_dashboard') {
                isDashboard = true;
                dashboardSockets.add(ws);
                ws.send(JSON.stringify({
                    action: 'init_state',
                    pending: hcaptchaPending,
                    counts: {
                        pending: Object.keys(hcaptchaPending).length,
                        trained: Object.keys(hcaptchaTrained).length,
                        concepts: Object.Aap kis file ya code script ko update karwana chahte hain? Code ya details yahan paste kar dein, aur sath bata dein ke is mein kya changes ya fixes karne hain, main mukammal updated file ready kar deta hoon.
