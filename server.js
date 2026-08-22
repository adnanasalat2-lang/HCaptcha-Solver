const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const app = express();

app.use(cors());
app.use(express.json({ limit: '60mb' }));

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');

const MAX_PENDING = 80;
const MAX_TRAINED = 5000;
// Dashboard page size — har fetch mein itne hi cards aayenge
const DASHBOARD_PAGE_SIZE = 20;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};

function getCleanKey(task) {
    let p = (task.prompt || "").split("|||")[0].trim().toLowerCase();
    return "TXT_" + p;
}

function dhashToBigInt(hexOrBin) {
    if (!hexOrBin || hexOrBin === "0000000000000000") return null;
    if (/^[01]+$/.test(hexOrBin)) {
        return BigInt('0b' + hexOrBin);
    }
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

function _removeFromConceptBank(tr) {
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
                console.log(`[DB] Trimmed ${toDelete.length} old trained entries to enforce limit`);
            }

            rebuildConceptBank();
            console.log(`[DB] Engine Loaded. Trained: ${Object.keys(hcaptchaTrained).length} | Concepts: ${Object.keys(conceptBank).length}`);
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
        try {
            fs.writeFileSync(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8');
        } catch(err) {
            console.error('[DB] PERSIST ERROR:', err.message);
        }
    }, 1000);
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
// ROUTES
// ==========================================

app.post('/api/new-hcaptcha', (req, res) => {
    const task = req.body;
    if (!task || !task.taskId) return res.json({ success: false, error: 'Missing taskId' });

    let autoRes = evaluateAutoSolve(task);
    if (autoRes.solved) {
        persistDatabase();
        return res.json({ success: true, autoSolved: true, clicks: autoRes.clicks });
    }

    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) {
        keys.slice(0, 10).forEach(k => delete hcaptchaPending[k]);
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

// ──────────────────────────────────────────────────────────
// NEW: Paginated API — dashboard ke liye lightweight endpoint
// tab=pending|trained, page=0,1,2..., size=20 (default)
// Thumbs bhi sirf is page ke tasks ke liye bhejta hai
// ──────────────────────────────────────────────────────────
app.get('/api/tasks', (req, res) => {
    const tab    = req.query.tab  === 'trained' ? 'trained' : 'pending';
    const page   = Math.max(0, parseInt(req.query.page)  || 0);
    const size   = Math.min(50, Math.max(1, parseInt(req.query.size) || DASHBOARD_PAGE_SIZE));
    // since= timestamp — naye tasks sirf ye bhejta hai (polling ke liye)
    const since  = req.query.since ? parseInt(req.query.since) : 0;

    let source = tab === 'trained' ? hcaptchaTrained : hcaptchaPending;
    let ids = Object.keys(source);

    // Trained: newest pehle
    if (tab === 'trained') ids = ids.reverse();

    let total = ids.length;

    // since filter — pending tab polling mein use hota hai (sirf naye tasks)
    if (since > 0 && tab === 'pending') {
        ids = ids.filter(id => {
            let ts = source[id].timestamp ? new Date(source[id].timestamp).getTime() : 0;
            return ts > since;
        });
        // Since mode mein pagination nahi — sab naye bhejo (max 20)
        let tasks = {};
        ids.slice(0, 20).forEach(id => { tasks[id] = source[id]; });
        return res.json({ tasks, total, page: 0, pages: 1, tab });
    }

    let pageIds = ids.slice(page * size, (page + 1) * size);
    let tasks = {};
    pageIds.forEach(id => { tasks[id] = source[id]; });

    res.json({ tasks, total, page, pages: Math.ceil(total / size), tab });
});

// Health + counts — dashboard header ke liye (lightweight, no data)
app.get('/api/counts', (req, res) => {
    res.json({
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length
    });
});

// LEGACY: Purana endpoint — extension content.js use karta hai, na hatao
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
            console.log(`[DB] Auto-trimmed ${oldest.length} old trained entries`);
        }

        delete hcaptchaPending[taskId];
        persistDatabase();
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
        _removeFromConceptBank(null); 
        persistDatabase();
        console.log(`[RESTORE] #${taskId} wapas pending mein`);
    }
    res.json({ success: true });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    _removeFromConceptBank(null); 
    persistDatabase();
    res.json({ success: true });
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'ok',
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length
    });
});

app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    if (fs.existsSync(f)) res.sendFile(f);
    else res.status(404).send('hcaptcha-dashboard.html nahi mila. Server folder mein rakho.');
});
app.get('/dashboard', (req, res) => res.redirect('/'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 hCaptcha Engine Running on Port ${PORT}`));
