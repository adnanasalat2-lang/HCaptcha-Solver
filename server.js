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

let hcaptchaPending = {};
let hcaptchaTrained = {};

// Concept Image Bank: { [conceptKey]: Set of valid image dHashes }
let conceptBank = {};

function getCleanKey(task) {
    let p = (task.prompt || "").split("|||")[0].trim().toLowerCase();
    return "TXT_" + p;
}

// FIX #1: BigInt XOR Hamming distance
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
    if (!tr.media || tr.media.length === 0) return;

    if (tr.media.length === 1) {
        // Single-media task (canvas / point click / drag): sirf ek item hai — uska dhash daalo
        // Clicks yahan coordinate objects hain ({px,py}), index nahi — isliye tr.media[idx] kaam nahi karta tha
        let d = tr.media[0].dhash;
        if (d && d !== "0000000000000000") conceptBank[cKey].add(d);
    } else {
        // Grid task: sirf clicked images ke dhash daalo
        (tr.clicks || []).forEach(idx => {
            if (typeof idx !== 'number') return; // coordinate objects skip karo
            if (tr.media[idx] && tr.media[idx].dhash && tr.media[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(tr.media[idx].dhash);
            }
        });
    }
}

// FIX #7: Incremental remove — sirf us conceptKey ki entries rebuild karo
// Poora rebuild sirf tab jab ek concept mein multiple overlapping entries hoon
function _removeFromConceptBankForTask(tr) {
    if (!tr) { rebuildConceptBank(); return; }
    let cKey = getCleanKey(tr);
    // Is concept ki saari trained entries se fresh rebuild karo — sirf is concept ke liye
    conceptBank[cKey] = new Set();
    for (let id in hcaptchaTrained) {
        let t = hcaptchaTrained[id];
        if (getCleanKey(t) !== cKey) continue;
        (t.clicks || []).forEach(idx => {
            if (t.media && t.media[idx] && t.media[idx].dhash && t.media[idx].dhash !== "0000000000000000") {
                conceptBank[cKey].add(t.media[idx].dhash);
            }
        });
    }
    if (conceptBank[cKey].size === 0) delete conceptBank[cKey];
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
                console.log(`[DB] Trimmed ${toDelete.length} old trained entries`);
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
    if (!targetDhashes || targetDhashes.size === 0 || !task.media || task.media.length === 0) {
        return { solved: false };
    }

    // ── SINGLE-MEDIA TASKS (canvas / point click / drag & drop) ───────────
    // media.length === 1 → clicks coordinate objects hain, index nahi
    // Matching task ko conceptBank se dhundho, phir uske actual coordinate clicks return karo
    if (task.media.length === 1) {
        let itemDhash = task.media[0].dhash;
        if (itemDhash && itemDhash !== "0000000000000000") {
            for (let savedHash of targetDhashes) {
                if (getHammingDistance(itemDhash, savedHash) <= 3) {
                    // Matching trained task dhundho — uske original coordinate clicks chahiye
                    for (let id in hcaptchaTrained) {
                        let tr = hcaptchaTrained[id];
                        if (getCleanKey(tr) !== cKey) continue;
                        if (!tr.media || !tr.media[0]) continue;
                        if (getHammingDistance(tr.media[0].dhash || "", itemDhash) <= 3) {
                            if (tr.clicks && tr.clicks.length > 0) {
                                console.log(`[AutoSolve] Single-media match → taskId: ${id}`);
                                return { solved: true, clicks: tr.clicks };
                            }
                        }
                    }
                }
            }
        }
        return { solved: false };
    }

    // ── GRID TASKS (multiple images) ──────────────────────────────────────
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
            refHash: task.refHash || "",
            media: lightMedia,
            clicks: matchedClicks,
            trainedAt: new Date().toISOString()
        };
        _addToConceptBank(hcaptchaTrained[task.taskId]);
        return { solved: true, clicks: matchedClicks };
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

    // FIX #2: refHash bhi save karo pending mein
    hcaptchaPending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt,
        refHash: task.refHash || "",
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

        if (lightMedia.length === 1) {
            // Single-media task: clicks coordinate objects hain — sirf media[0] ka dhash daalo
            let d = lightMedia[0].dhash;
            if (d && d !== "0000000000000000") conceptBank[cKey].add(d);
        } else {
            // Grid task: clicked image indices se dhash daalo
            (clicks || []).forEach(idx => {
                if (typeof idx !== 'number') return;
                if (lightMedia[idx] && lightMedia[idx].dhash && lightMedia[idx].dhash !== "0000000000000000") {
                    conceptBank[cKey].add(lightMedia[idx].dhash);
                }
            });
        }

        hcaptchaTrained[taskId] = {
            id: taskId,
            prompt: source.prompt,
            refHash: source.refHash || "",
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

// FIX #13: Restore — conceptBank properly update karo, phir pending mein daalo
app.post('/api/restore-hcaptcha', (req, res) => {
    const { taskId } = req.body;
    if (!taskId) return res.json({ success: false, error: 'Missing taskId' });

    if (hcaptchaTrained[taskId]) {
        let tr = hcaptchaTrained[taskId];
        hcaptchaPending[taskId] = {
            id: tr.id,
            prompt: tr.prompt,
            refHash: tr.refHash || "",
            media: tr.media,
            clicks: [],
            restoredAt: new Date().toISOString()
        };
        delete hcaptchaTrained[taskId];
        // FIX #7: Sirf is task ki concept rebuild karo — poori rebuild nahi
        _removeFromConceptBankForTask(tr);
        persistDatabase();
        console.log(`[RESTORE] #${taskId} wapas pending mein`);
    }
    res.json({ success: true });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    let tr = hcaptchaTrained[req.params.id];
    let pe = hcaptchaPending[req.params.id];
    delete hcaptchaPending[req.params.id];
    delete hcaptchaTrained[req.params.id];
    // FIX #7: Incremental remove
    if (tr) _removeFromConceptBankForTask(tr);
    else if (pe) _removeFromConceptBankForTask(pe);
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
