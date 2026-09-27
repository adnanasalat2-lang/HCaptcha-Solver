const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const Redis = require('ioredis');

// ── CRASH GUARDS — koi bhi error server ko na giraye (restart loop se bache) ──
process.on('uncaughtException', (err) => console.error('[CRASH GUARD] uncaught:', err.message));
process.on('unhandledRejection', (reason) => console.error('[CRASH GUARD] rejection:', reason));

const app = express();
app.use(cors());
app.use(express.json({ limit: '60mb' }));

const server = http.createServer(app);
const wss = new WebSocket.Server({
    server,
    perMessageDeflate: false,
    maxPayload: 12 * 1024 * 1024,   // 12MB max — bade frame se crash na ho
});
wss.on('error', (err) => console.error('[WSS ERROR]', err.message));

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');
const AI_BRAIN_FILE = path.join(DATA_DIR, 'ai_brain.json'); 
const CONCEPT_FILE = path.join(DATA_DIR, 'concepts.json'); // auto-solve memory — alag file

const MAX_PENDING = 10000;      // hazaron profiles ek saath — pehle 60 tha, asli tasks chupchaap delete
                                // hote the. Mare hue tasks ab khud hat-te hain, is liye ye sirf safety cap hai.
const DEAD_TASK_MS = 60000;     // 60s tak koi browser register na ho → task mara hua
const MAX_TRAINED = 3000;       // total trained records — purana khud delete
const MAX_GRID_TRAINED = 10;    // grid records (9 images = bhaari) — sirf 10 retrain ke liye
const MAX_DHASH_PER_CONCEPT = 500; // ek concept mein max 500 dhash — auto-solve tez rahe
                                // NOTE: conceptBank (auto-solve dhash memory) alag file mein,
                                // grid record delete se safe. Reset Brain se saaf hota hai.
const DASHBOARD_PAGE_SIZE = 40;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};
let globalAIBrain = {}; 

const browserSockets = new Map();
const dashboardWorkers = new Map(); 

// Har worker ne kitne task solve kiye — refresh/reconnect pe bhi rahe
// { workerId: { solved: number, mode: 'grid'|'manual' } }
const workerStats = {};

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
            console.log(`[DB] Engine Loaded. Trained: ${Object.keys(hcaptchaTrained).length}`);
        } catch (e) {
            console.error("[DB] Error loading database:", e.message);
        }
    }
    // conceptBank apni file se load karo (auto-solve memory — grid delete se safe)
    if (fs.existsSync(CONCEPT_FILE)) {
        try {
            const raw = JSON.parse(fs.readFileSync(CONCEPT_FILE, 'utf8')) || {};
            conceptBank = {};
            for (const k in raw) conceptBank[k] = new Set(raw[k]); // array → Set
            console.log(`[DB] ConceptBank loaded: ${Object.keys(conceptBank).length} concepts`);
        } catch (e) {
            console.error("[DB] ConceptBank load error:", e.message);
            rebuildConceptBank(); // fallback
        }
    } else {
        // Pehli dafa — trained se banao (migration)
        rebuildConceptBank();
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

// conceptBank alag file mein save — grid record delete hone pe bhi auto-solve bacha rahe
let _conceptTimer = null;
function persistConcepts() {
    if (_conceptTimer) clearTimeout(_conceptTimer);
    _conceptTimer = setTimeout(() => {
        const plain = {};
        for (const k in conceptBank) plain[k] = [...conceptBank[k]]; // Set → array
        fs.writeFile(CONCEPT_FILE, JSON.stringify(plain), 'utf8', () => {});
    }, 2000);
}

let aiSaveTimer = null;
function persistAIBrain() {
    if (aiSaveTimer) clearTimeout(aiSaveTimer);
    aiSaveTimer = setTimeout(() => {
        fs.writeFile(AI_BRAIN_FILE, JSON.stringify(globalAIBrain), 'utf8', () => {});
    }, 5000);
}

// ── TASK TYPE HELPER ──────────────────────────────────────────────────────────
// media array dekh ke task type identify karo.
// Grid  : media.length > 1  (9 tiles — multi-image)
// Video : media[0].type === 'video_frames' ya media[0].frames exist kare
// Point/Drag/Single : media.length === 1, type 'image' ya 'drag' ya 'point'
function isGridTask(media) {
    return Array.isArray(media) && media.length > 1;
}
function isVideoTask(media) {
    if (!Array.isArray(media) || media.length !== 1) return false;
    return media[0].type === 'video_frames' || !!media[0].frames;
}
// Point, drag, single-image — sab canvas/static tasks
function isStaticTask(media) {
    return Array.isArray(media) && media.length === 1 && !isVideoTask(media);
}

// ── DASHBOARD MODE ROUTING ────────────────────────────────────
// Kolotibablo tasks (source:'kolotibablo') → alag 'kolo' stream me jayein taake
// existing hcaptcha grid / manual tasks me koi preshani na ho.
//   source==='kolotibablo' → 'kolo'
//   grid (media>1)         → 'grid'
//   warna                  → 'manual'
function taskModeOf(task) {
    if (task && task.source === 'kolotibablo') return 'kolo';
    return (task && task.media && task.media.length > 1) ? 'grid' : 'manual';
}

function evaluateAutoSolve(task) {
    // ── AUTO-SOLVE RULES ──
    //
    // GRID tasks  → hamesha dashboard se guzarni chahiye, auto-solve NAHI.
    //               Chahe exact taskId match ho ya dhash match — dashboard
    //               se confirm hona zaroori hai.
    //
    // VIDEO tasks → har baar animation alag hoti hai, clicks kabhi bhi
    //               100% sahi nahi ho sakte — auto-solve NAHI.
    //
    // STATIC tasks (point / drag / single-image) → hCaptcha same puzzle
    //               image recycle karta hai. taskId = fnv32(prompt + dhash)
    //               — agar same image aaya to dhash same hoga, taskId same
    //               hoga. Exact taskId match = 100% same task = safe.
    //               Sirf exact match pe auto-solve, koi guess/dhash-matching
    //               NAHI — ek bhi galat task nahi hona chahiye.

    const media = task.media || [];

    // Grid — kabhi auto-solve nahi
    if (isGridTask(media)) return { solved: false };

    // Video — kabhi auto-solve nahi
    if (isVideoTask(media)) return { solved: false };

    // Static (point/drag/single-image) — sirf exact taskId match
    if (isStaticTask(media)) {
        const trained = hcaptchaTrained[task.taskId];
        if (trained) {
            // Double-check: trained record bhi static hona chahiye
            // (collision se safety — ek grid record ka taskId match na kare)
            const tMedia = trained.media || [];
            if (isStaticTask(tMedia)) {
                console.log(`[AUTO-SOLVE] Static task exact match: ${task.taskId}`);
                return { solved: true, clicks: trained.clicks || [] };
            }
        }
        return { solved: false };
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

// ── MARA HUA TASK HATAO ──────────────────────────────────────────────────────
// Jis task ka koi browser (extension) ab wait nahi kar raha — us ko solve
// karna bekaar hai. Pehle ye dashboard pe hamesha pade rehte the aur worker
// in ko ek ek kar ke solve karta, asli naye tasks peeche intezar karte.
function dropDeadTask(taskId, reason) {
    if (!hcaptchaPending[taskId]) return false;
    const socks = browserSockets.get(taskId);
    if (socks && socks.size > 0) return false;   // abhi bhi koi browser wait kar raha
    delete hcaptchaPending[taskId];
    console.log(`[CLEANUP] Dead task removed: ${taskId} (${reason})`);
    broadcastDashboard('task_deleted', { taskId });
    return true;
}

function broadcastDashboard(type, data) {
    if (!dashboardWorkers.size) return;
    const msg = JSON.stringify({ type, data });
    dashboardWorkers.forEach((info, ws) => {
        if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    });
}

// ── LIVE VIDEO — sirf manual mode workers ko forward karo ─────────────
function broadcastToManualWorkers(type, data) {
    if (!dashboardWorkers.size) return;
    const msg = JSON.stringify({ type, data });
    dashboardWorkers.forEach((info, ws) => {
        if (info.mode === 'manual' && ws.readyState === WebSocket.OPEN) {
            ws.send(msg);
        }
    });
}

// ── TARGETED LIVE VIDEO — sirf assigned worker ko bhejo (BANDWIDTH SAVE) ──
const _streamToRealTask = new Map();   // streamTaskId → realTaskId
function _pruneStreamMap() {
    if (_streamToRealTask.size > 500) {
        const first = _streamToRealTask.keys().next().value;
        _streamToRealTask.delete(first);
    }
}
function sendToAssignedWorker(type, data, streamTaskId) {
    if (!dashboardWorkers.size) return;
    const msg = JSON.stringify({ type, data });
    const realTaskId = _streamToRealTask.get(streamTaskId);
    if (realTaskId) {
        const task = hcaptchaPending[realTaskId] || hcaptchaTrained[realTaskId];
        if (task && task.assignedTo) {
            let sent = false;
            dashboardWorkers.forEach((info, ws) => {
                if (info.mode === 'manual'
                    && info.workerId === task.assignedTo
                    && ws.readyState === WebSocket.OPEN) {
                    ws.send(msg);
                    sent = true;
                }
            });
            if (sent) return;
        }
    }
    dashboardWorkers.forEach((info, ws) => {
        if (info.mode === 'manual' && ws.readyState === WebSocket.OPEN) {
            ws.send(msg);
        }
    });
}

function assignTask(taskId) {
    let task = hcaptchaPending[taskId];
    if (!task || task.assignedTo) return;

    let taskMode = taskModeOf(task);

    const countByWorker = {};
    for (let k in hcaptchaPending) {
        const a = hcaptchaPending[k].assignedTo;
        if (a) countByWorker[a] = (countByWorker[a] || 0) + 1;
    }

    const seenWorker = {};
    let bestWs = null;
    let minCount = Infinity;
    let oldestAssignTime = Infinity;

    dashboardWorkers.forEach((info, ws) => {
        if (info.mode !== taskMode || ws.readyState !== WebSocket.OPEN) return;
        if (seenWorker[info.workerId]) return;
        seenWorker[info.workerId] = true;

        const count = countByWorker[info.workerId] || 0;
        const lastTime = info.lastAssigned || 0;

        if (count < minCount || (count === minCount && lastTime < oldestAssignTime)) {
            minCount = count;
            oldestAssignTime = lastTime;
            bestWs = ws;
        }
    });

    if (bestWs) {
        let info = dashboardWorkers.get(bestWs);
        task.assignedTo = info.workerId;
        task.assignedAt = Date.now();
        info.lastAssigned = Date.now();
        bestWs.send(JSON.stringify({ type: 'new_task', data: task }));
    }
}

function reassignTasksFrom(workerId) {
    for (let taskId in hcaptchaPending) {
        if (hcaptchaPending[taskId].assignedTo === workerId) {
            hcaptchaPending[taskId].assignedTo = null;
            hcaptchaPending[taskId].assignedAt = null;
            assignTask(taskId);
        }
    }
}

const STALE_ASSIGN_MS = 45000;
function reclaimStaleTasks() {
    const online = new Set();
    dashboardWorkers.forEach((info, ws) => {
        if (ws.readyState === WebSocket.OPEN) online.add(info.workerId);
    });
    let changed = false;
    // Safety net: 60s se koi browser is task ke liye register nahi → mara hua
    for (let taskId in hcaptchaPending) {
        const t = hcaptchaPending[taskId];
        if (t.receivedAt && Date.now() - t.receivedAt > DEAD_TASK_MS) {
            if (dropDeadTask(taskId, 'no browser for 60s')) changed = true;
        }
    }
    for (let taskId in hcaptchaPending) {
        const t = hcaptchaPending[taskId];
        if (!t.assignedTo) continue;
        const workerGone = !online.has(t.assignedTo);
        const tooOld = t.assignedAt && (Date.now() - t.assignedAt > STALE_ASSIGN_MS);
        if (workerGone || tooOld) {
            t.assignedTo = null;
            t.assignedAt = null;
            assignTask(taskId);
            changed = true;
        }
    }
    if (changed) broadcastDashboard('counts', getCountsData());
}
setInterval(reclaimStaleTasks, 15000);

function getCountsData() {
    let gridPending = 0, manualPending = 0;
    for (let id in hcaptchaPending) {
        const m = hcaptchaPending[id].media;
        if (m && m.length > 1) gridPending++;
        else manualPending++;
    }

    const online = [];
    const seen = new Set();
    dashboardWorkers.forEach((info, ws) => {
        if (ws.readyState === WebSocket.OPEN && !seen.has(info.workerId)) {
            seen.add(info.workerId);
            const st = workerStats[info.workerId] || {};
            online.push({
                workerId: info.workerId,
                mode: info.mode,
                solved:       st.solved       || 0,
                solvedGrid:   st.solvedGrid   || 0,
                solvedManual: st.solvedManual || 0,
            });
        }
    });

    let trainedGrid = 0, trainedManual = 0;
    for (const wid in workerStats) {
        trainedGrid   += (workerStats[wid].solvedGrid   || 0);
        trainedManual += (workerStats[wid].solvedManual || 0);
    }
    const trainedTotal = trainedGrid + trainedManual;

    return {
        pending: Object.keys(hcaptchaPending).length,
        trained: trainedTotal,
        concepts: Object.keys(conceptBank).length,
        gridPending,
        manualPending,
        trainedGrid,
        trainedManual,
        onlineWorkers: online,
    };
}

wss.on('connection', (ws) => {
    let boundTaskId = null;
    let isDashboard = false;

    ws.on('error', (err) => console.error('[WS SOCKET]', err.message));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            if (data.action === 'ping') return; 
            
            if (data.action === 'register' && data.taskId) {
                // ── Is browser ne naya task register kiya → purana task mara hua ──
                // (failure/retry/skip ke baad extension aage badh gayi)
                if (boundTaskId && boundTaskId !== data.taskId) {
                    const oldId = boundTaskId;
                    const oldSet = browserSockets.get(oldId);
                    if (oldSet) {
                        oldSet.delete(ws);
                        if (oldSet.size === 0) browserSockets.delete(oldId);
                    }
                    if (dropDeadTask(oldId, 'browser moved to new task')) {
                        broadcastDashboard('counts', getCountsData());
                    }
                }
                boundTaskId = data.taskId;
                if (!browserSockets.has(boundTaskId)) {
                    browserSockets.set(boundTaskId, new Set());
                }
                browserSockets.get(boundTaskId).add(ws);

                // ── WS REGISTER: sirf static tasks pe trained cache bhejo ──
                // Grid  → hamesha dashboard se, WS pe bhi auto-solve NAHI.
                // Video → animation unique hoti hai, WS pe bhi NAHI.
                // Static (point/drag/single) → exact taskId match = 100% same task → safe.
                const trained = hcaptchaTrained[boundTaskId];
                if (trained) {
                    const tMedia = trained.media || [];
                    if (isStaticTask(tMedia)) {
                        console.log(`[WS AUTO-SOLVE] Static task: ${boundTaskId}`);
                        ws.send(JSON.stringify({
                            action: 'solve',
                            taskId: boundTaskId,
                            clicks: trained.clicks
                        }));
                    }
                    // Grid ya video trained record → kuch mat bhejo
                }
                return;
            }

            if (data.action === 'dashboard') {
                isDashboard = true;
                dashboardWorkers.set(ws, { workerId: data.workerId, mode: data.mode, lastAssigned: 0 });
                if (!workerStats[data.workerId]) workerStats[data.workerId] = { solved: 0 };
                ws.send(JSON.stringify({ type: 'counts', data: getCountsData() }));
                broadcastDashboard('counts', getCountsData());
                
                for (let taskId in hcaptchaPending) {
                    if (!hcaptchaPending[taskId].assignedTo) {
                        let taskMode = taskModeOf(hcaptchaPending[taskId]);
                        if (taskMode === data.mode) assignTask(taskId);
                    }
                }
                return;
            }

            if (data.action === 'live_video_start') {
                sendToAssignedWorker('live_video_start', {
                    taskId: data.taskId, maxFrames: data.maxFrames,
                    isVideo: data.isVideo, timestamp: data.timestamp,
                }, data.taskId);
                return;
            }
            if (data.action === 'live_video_frame') {
                sendToAssignedWorker('live_video_frame', {
                    taskId: data.taskId, frame: data.frame,
                    index: data.index, timestamp: data.timestamp,
                }, data.taskId);
                return;
            }
            if (data.action === 'live_video_end') {
                sendToAssignedWorker('live_video_end', {
                    taskId: data.taskId, totalFrames: data.totalFrames,
                    isVideo: data.isVideo, timestamp: data.timestamp,
                }, data.taskId);
                return;
            }
            if (data.action === 'live_video_link') {
                if (data.streamTaskId && data.realTaskId) {
                    _streamToRealTask.set(data.streamTaskId, data.realTaskId);
                    _pruneStreamMap();
                }
                sendToAssignedWorker('live_video_link', {
                    streamTaskId: data.streamTaskId, realTaskId: data.realTaskId,
                    timestamp: data.timestamp,
                }, data.streamTaskId);
                return;
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        if (boundTaskId && browserSockets.has(boundTaskId)) {
            const set = browserSockets.get(boundTaskId);
            set.delete(ws);
            if (set.size === 0) {
                browserSockets.delete(boundTaskId);
                // Browser gaya (tab band / profile band). 15s grace — WS reconnect
                // pe extension same taskId dobara register karti hai, tab task bachega.
                const deadId = boundTaskId;
                setTimeout(() => {
                    if (dropDeadTask(deadId, 'browser disconnected')) {
                        broadcastDashboard('counts', getCountsData());
                    }
                }, 15000);
            }
        }
        if (isDashboard && dashboardWorkers.has(ws)) {
            let info = dashboardWorkers.get(ws);
            dashboardWorkers.delete(ws);
            
            let stillConnected = false;
            for (let [otherWs, otherInfo] of dashboardWorkers.entries()) {
                if (otherInfo.workerId === info.workerId && otherWs.readyState === WebSocket.OPEN) {
                    stillConnected = true; break;
                }
            }
            if (!stillConnected) reassignTasksFrom(info.workerId);
            broadcastDashboard('counts', getCountsData());
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

    // ── Wahi task dobara aaya (reconnect / re-extract) → assignment mat todo ──
    // Pehle assignedTo null ho ke dobara assign hota → dashboard pe duplicate.
    const existing = hcaptchaPending[task.taskId];
    if (existing) {
        existing.media = task.media;
        existing.prompt = task.prompt;
        existing.receivedAt = Date.now();
        if (!existing.assignedTo) assignTask(task.taskId);
        return res.json({ success: true, autoSolved: false });
    }

    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) {
        // Pehle mare hue (koi browser wait nahi kar raha) hatao, phir sab se purane.
        // Har delete dashboard ko batao — pehle chupchaap delete hote the.
        const isDead = k => !(browserSockets.get(k) && browserSockets.get(k).size);
        const dead  = keys.filter(isDead);                 // O(n) — hazaron tasks pe bhi tez
        const alive = keys.filter(k => !isDead(k));
        [...dead, ...alive].slice(0, 15).forEach(k => {
            delete hcaptchaPending[k];
            broadcastDashboard('task_deleted', { taskId: k });
        });
        console.warn(`[PENDING FULL] ${keys.length} pending — 15 hataye (dead pehle: ${dead.length})`);
    }

    hcaptchaPending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media,
        timestamp: task.timestamp,
        // source: 'kolotibablo' → alag kolo stream me route hoga (taskModeOf)
        source: task.source || undefined,
        // Chhote (≈100 bytes) — dashboard card ka size + iframe sitekey inhi se
        sitekey: task.sitekey || undefined,
        dimensions: task.dimensions || undefined,
        receivedAt: Date.now(),
        assignedTo: null
    };

    assignTask(task.taskId);
    broadcastDashboard('counts', getCountsData()); 

    res.json({ success: true, autoSolved: false });
});

app.post('/api/submit-hcaptcha', (req, res) => {
    const { taskId, clicks, workerId, skip } = req.body;
    if (!taskId) return res.json({ success: false });

    // ⏭ SKIP (kolotibablo no-match): empty clicks ya skip:true aaye to extension ko
    // clicks:[] relay karo (wo Next/Verify daba dega), kuch train mat karo,
    // task ko pending se hata do. Baqi (non-empty clicks) ka flow neeche waisa hi hai.
    if (skip || !clicks || clicks.length === 0) {
        notifyBrowsers(taskId, []);
        if (hcaptchaPending[taskId]) delete hcaptchaPending[taskId];
        broadcastDashboard('task_solved', { taskId });
        broadcastDashboard('counts', getCountsData());
        return res.json({ success: true, skipped: true });
    }

    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    if (source) {
        let lightMedia = (source.media || []).map(m => ({
            dhash: m.dhash || "", stableHash: m.stableHash || "", type: m.type || "image",
            index: m.index !== undefined ? m.index : 0, thumb: m.thumb || (m.frames ? m.frames[0] : "")
        }));

        const media = lightMedia;
        const isGrid   = isGridTask(media);
        const isVideo  = isVideoTask(media);
        const isStatic = isStaticTask(media);

        // conceptBank — sab tasks ke liye dhash learning
        let cKey = getCleanKey(source);
        if (!conceptBank[cKey]) conceptBank[cKey] = new Set();
        clicks.forEach(idx => {
            if (typeof idx === 'number' && media[idx] && media[idx].dhash && media[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(media[idx].dhash);
            }
        });
        if (conceptBank[cKey].size > MAX_DHASH_PER_CONCEPT) {
            const arr = [...conceptBank[cKey]];
            conceptBank[cKey] = new Set(arr.slice(arr.length - MAX_DHASH_PER_CONCEPT));
        }
        persistConcepts();

        // ── TRAINED CACHE ──
        // Grid  → hcaptchaTrained mein save karo RETRAIN ke liye (dashboard trained tab mein
        //         dikhe, AI seekh sake) — lekin auto-solve NAHI hoga (evaluateAutoSolve mein
        //         grid permanently blocked hai). MAX_GRID_TRAINED = 10 limit — purane khud delete.
        // Video → unique animation, cached clicks galat hongi — SKIP.
        // Static (point/drag/single) → exact taskId match safe — save karo, auto-solve hoga.
        if (isGrid) {
            hcaptchaTrained[taskId] = {
                id: taskId, prompt: source.prompt, conceptKey: cKey,
                media: lightMedia, clicks: clicks, trainedAt: new Date().toISOString()
            };
            // Grid records limit — sirf 10 latest rakho (bhaari hain)
            const gridIds = Object.keys(hcaptchaTrained).filter(id => isGridTask(hcaptchaTrained[id].media));
            if (gridIds.length > MAX_GRID_TRAINED) {
                gridIds.slice(0, gridIds.length - MAX_GRID_TRAINED)
                    .forEach(k => delete hcaptchaTrained[k]);
            }
        } else if (isStatic) {
            hcaptchaTrained[taskId] = {
                id: taskId, prompt: source.prompt, conceptKey: cKey,
                media: lightMedia, clicks: clicks, trainedAt: new Date().toISOString()
            };
            // Total trained limit — purana khud delete (3000)
            const trainedKeys = Object.keys(hcaptchaTrained);
            if (trainedKeys.length > MAX_TRAINED) {
                trainedKeys.slice(0, trainedKeys.length - MAX_TRAINED)
                    .forEach(k => delete hcaptchaTrained[k]);
            }
        }
        // Video ke liye hcaptchaTrained mein kuch nahi jaata

        // Worker ka solve count badhao
        if (workerId) {
            if (!workerStats[workerId]) workerStats[workerId] = { solved: 0, solvedGrid: 0, solvedManual: 0 };
            if (workerStats[workerId].solvedGrid == null)   workerStats[workerId].solvedGrid = 0;
            if (workerStats[workerId].solvedManual == null) workerStats[workerId].solvedManual = 0;
            workerStats[workerId].solved++;
            if (isGrid) workerStats[workerId].solvedGrid++;
            else        workerStats[workerId].solvedManual++;
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
            if (!globalAIBrain[label]) {
                globalAIBrain[label] = { data: featureArr, shape: [1, 1280] };
                changed = true;
            } else {
                const old = globalAIBrain[label];
                const MAX_EXAMPLES = 150; 
                const currentN = old.shape[0];
                if (currentN >= MAX_EXAMPLES) {
                    old.data.splice(0, 1280);
                    old.data.push(...featureArr);
                } else {
                    old.data.push(...featureArr);
                    old.shape[0] += 1;
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
    // conceptBank (grid auto-solve memory) bhi saaf karo
    conceptBank = {};
    persistConcepts();
    // Grid trained records bhi hatao
    for (const id in hcaptchaTrained) {
        const m = hcaptchaTrained[id].media;
        if (m && m.length > 1) delete hcaptchaTrained[id];
    }
    persistDatabase();
    broadcastDashboard('counts', getCountsData());
    console.log('[RESET] Brain + conceptBank + grid records saaf');
    res.json({ success: true });
});

app.get('/api/tasks', (req, res) => {
    const tab = req.query.tab === 'trained' ? 'trained' : 'pending';
    const workerId = req.query.workerId;
    const page = Math.max(0, parseInt(req.query.page) || 0);
    const size = Math.min(40, Math.max(1, parseInt(req.query.size) || DASHBOARD_PAGE_SIZE));
    // 🔍 Search: ID ya prompt se poore DB mein dhoondo (sirf current page nahi)
    const search = (req.query.search || '').toString().trim().toLowerCase().replace(/^#/, '');

    let source = tab === 'trained' ? hcaptchaTrained : hcaptchaPending;
    let ids = Object.keys(source);

    if (tab === 'pending' && workerId) {
        ids = ids.filter(id => hcaptchaPending[id].assignedTo === workerId);
    }

    // 🔍 ID ya prompt par filter
    if (search) {
        ids = ids.filter(id => {
            if (id.toLowerCase().includes(search)) return true;
            let p = (source[id] && source[id].prompt ? source[id].prompt : '').toLowerCase();
            return p.includes(search);
        });
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
    persistDatabase();
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
