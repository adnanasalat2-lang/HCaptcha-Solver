const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const app = express();

app.use(cors());
app.use(express.json({ limit: '60mb' }));

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');

// Fix #5: Trained ka bhi limit — max 5000 entries
const MAX_PENDING = 80;
const MAX_TRAINED = 5000;

let hcaptchaPending = {};
let hcaptchaTrained = {};

// Concept Image Bank: { [conceptKey]: Set of valid image dHashes }
let conceptBank = {};

function getCleanKey(task) {
    let p = (task.prompt || "").split("|||")[0].trim().toLowerCase();
    return "TXT_" + p;
}

// Fix #14: Hamming distance — BigInt XOR se O(1) comparison (hex string pe)
function dhashToBigInt(hexOrBin) {
    if (!hexOrBin || hexOrBin === "0000000000000000") return null;
    // Binary string (0/1 chars) ko BigInt mein convert
    if (/^[01]+$/.test(hexOrBin)) {
        return BigInt('0b' + hexOrBin);
    }
    // Hex string fallback
    try { return BigInt('0x' + hexOrBin); } catch(e) { return null; }
}

function getHammingDistance(h1, h2) {
    if (!h1 || !h2 || h1.length !== h2.length) return 999;
    // Fast path: BigInt XOR then popcount
    const b1 = dhashToBigInt(h1);
    const b2 = dhashToBigInt(h2);
    if (b1 !== null && b2 !== null) {
        let xor = b1 ^ b2;
        let diff = 0;
        while (xor > 0n) { diff += Number(xor & 1n); xor >>= 1n; }
        return diff;
    }
    // Fallback: char by char
    let diff = 0;
    for (let i = 0; i < h1.length; i++) { if (h1[i] !== h2[i]) diff++; }
    return diff;
}

// Fix #7: rebuildConceptBank — incremental update ke functions bhi rakhe hain
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
        if (tr.media && tr.media[idx] && tr.media[idx].dhash && tr.media[idx].dhash !== "0000000000000000") {
            conceptBank[cKey].add(tr.media[idx].dhash);
        }
    });
}

function _removeFromConceptBank(tr) {
    // Rebuild karna padega kyunki ek concept mein multiple trained entries hoti hain
    // Sirf tab call karo jab delete/restore ho — warna incremental
    rebuildConceptBank();
}

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            const raw = fs.readFileSync(DB_FILE, 'utf8');
            const data = JSON.parse(raw);
            hcaptchaPending = {};
            hcaptchaTrained = data.trained || {};

            // Fix #5: DB load pe bhi limit enforce karo
            const trainedKeys = Object.keys(hcaptchaTrained);
            if (trainedKeys.length > MAX_TRAINED) {
                const toDelete = trainedKeys.slice(0, trainedKeys.length - MAX_TRAINED);
                toDelete.forEach(k => delete hcaptchaTrained[k]);
                console.log(`[DB] Trimmed ${toDelete.length} old trained entries to enforce limit`);
            }

            rebuildConceptBank();
            console.log(`[DB] Engine Loaded. Trained: ${Object.keys(hcaptchaTrained).length} | Concepts: ${Object.keys(conceptBank).length}`);
        } catch (e) {
            // Fix #13: silent catch nahi — log karo
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
            // Fix #13: disk full / permission error — log karo silently ignore mat karo
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

        if (matchedClicks.length >= 1 && matchedClicks.length <= 5) {
            let lightMedia = task.media.map(m => ({ dhash: m.dhash, type: m.type, index: m.index }));
            hcaptchaTrained[task.taskId] = {
                id: task.taskId,
                prompt: task.prompt,
                refHash: task.refHash,
                media: lightMedia,
                clicks: matchedClicks,
                trainedAt: new Date().toISOString()
            };
            _addToConceptBank(hcaptchaTrained[task.taskId]);
            return { solved: true, clicks: matchedClicks };
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

    // ✅ evaluateAutoSolve — exact match + dhash concept matching dono
    let autoRes = evaluateAutoSolve(task);
    if (autoRes.solved) {
        persistDatabase();
        return res.json({ success: true, autoSolved: true });
    }

    // Pending limit
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
            if (lightMedia[idx] && lightMedia[idx].dhash && lightMedia[idx].dhash !== "0000000000000000") {
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

        // Fix #5: Trained limit enforce karo
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

// Trained → Pending wapas
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
        // Fix #7: Incremental — poora rebuild sirf delete pe
        _removeFromConceptBank(null); // internally rebuild
        persistDatabase();
        console.log(`[RESTORE] #${taskId} wapas pending mein`);
    }
    res.json({ success: true });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    _removeFromConceptBank(null); // rebuild
    persistDatabase();
    res.json({ success: true });
});

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({
        status: 'ok',
        pending: Object.keys(hcaptchaPending).length,
        trained: Object.keys(hcaptchaTrained).length,
        concepts: Object.keys(conceptBank).length
    });
});

// ✅ Dashboard Railway pe serve karo
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'hcaptcha-dashboard.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 hCaptcha Engine Running on Port ${PORT}`));
