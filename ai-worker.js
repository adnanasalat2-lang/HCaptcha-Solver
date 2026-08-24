// ============================================================
// ai-worker.js — SharedWorker
// Ek baar load hota hai, sab dashboard tabs use karte hain
// TensorFlow + MobileNet + KNN — sab yahan
// ============================================================

/* global importScripts, SharedWorkerGlobalScope */

importScripts(
    'https://cdn.jsdelivr.net/npm/@tensorflow/tfjs@4.20.0/dist/tf.min.js',
    'https://cdn.jsdelivr.net/npm/@tensorflow-models/mobilenet@2.1.1/dist/mobilenet.min.js',
    'https://cdn.jsdelivr.net/npm/@tensorflow-models/knn-classifier@1.2.4/dist/knn-classifier.min.js'
);

// Connected tabs (ports)
const ports = new Set();

let isReady  = false;
let aiModel  = null;
let knn      = null;
let knnStore = {};  // { label: { data, shape } } — IDB se load/save

// ── IDB helpers (worker mein) ──────────────────────────────
const IDB_NAME  = 'hcm_knn';
const IDB_STORE = 'knn';
const IDB_KEY   = 'model_v3';

function idbOpen() {
    return new Promise((res, rej) => {
        let r = indexedDB.open(IDB_NAME, 1);
        r.onupgradeneeded = e => e.target.result.createObjectStore(IDB_STORE);
        r.onsuccess = e => res(e.target.result);
        r.onerror   = () => rej(r.error);
    });
}
async function idbGet() {
    let db = await idbOpen();
    return new Promise((res, rej) => {
        let req = db.transaction(IDB_STORE, 'readonly').objectStore(IDB_STORE).get(IDB_KEY);
        req.onsuccess = () => res(req.result || null);
        req.onerror   = () => rej(req.error);
    });
}
async function idbSet(val) {
    let db = await idbOpen();
    return new Promise((res, rej) => {
        let tx = db.transaction(IDB_STORE, 'readwrite');
        tx.objectStore(IDB_STORE).put(val, IDB_KEY);
        tx.oncomplete = res; tx.onerror = () => rej(tx.error);
    });
}

// ── Broadcast to all tabs ──────────────────────────────────
function broadcast(msg) {
    ports.forEach(p => { try { p.postMessage(msg); } catch(e) {} });
}

// ── AI Init ───────────────────────────────────────────────
async function initAI() {
    broadcast({ type: 'status', text: '🧠 AI Loading...', ready: false });
    try {
        aiModel = await mobilenet.load({ version: 2, alpha: 1.0 });
        knn     = knnClassifier.create();
        await loadKNN();
        isReady = true;
        let concepts = Object.keys(knn.getClassExampleCount()).filter(k => !k.startsWith('negative_')).length;
        broadcast({ type: 'status', text: `🤖 AI Ready ✅ (${concepts} concepts)`, ready: true });
    } catch(e) {
        broadcast({ type: 'status', text: '⚠️ AI Failed — Reload karo', ready: false });
    }
}

async function loadKNN() {
    try {
        let ser = await idbGet();
        if (!ser) return;
        knnStore = ser;
        let ds = {};
        for (let k in ser) ds[k] = tf.tensor(ser[k].data, ser[k].shape);
        knn.setClassifierDataset(ds);
    } catch(e) {}
}

async function saveKNN() {
    if (!knn) return;
    try {
        let ds = knn.getClassifierDataset();
        let ser = {};
        for (let k in ds) ser[k] = { data: Array.from(ds[k].dataSync()), shape: ds[k].shape };
        knnStore = ser;
        await idbSet(ser);
        let concepts = Object.keys(ser).filter(k => !k.startsWith('negative_')).length;
        broadcast({ type: 'status', text: `🤖 AI Ready ✅ (${concepts} concepts)`, ready: true });
    } catch(e) {}
}

// ── Feature Extraction (from ImageBitmap) ─────────────────
async function getFeatures(bitmap) {
    let canvas = new OffscreenCanvas(bitmap.width, bitmap.height);
    let ctx = canvas.getContext('2d');
    ctx.drawImage(bitmap, 0, 0);
    let imgData = ctx.getImageData(0, 0, bitmap.width, bitmap.height);
    let tfImg = tf.browser.fromPixels({ data: imgData.data, width: bitmap.width, height: bitmap.height });
    let features = aiModel.infer(tfImg, true);
    tfImg.dispose();
    return features;
}

// ── Train ─────────────────────────────────────────────────
async function trainTask(task, wrongIndices) {
    if (!isReady || task.type !== 'grid') return;
    let pText    = task.prompt.split('|||')[0].trim().toLowerCase();
    let negLabel = 'negative_' + pText;

    for (let m of (task.media || [])) {
        let src = m.thumb || m.src;
        if (!src) continue;
        try {
            let resp   = await fetch(src);
            let blob   = await resp.blob();
            let bitmap = await createImageBitmap(blob);
            let feat   = await getFeatures(bitmap);
            bitmap.close();

            let isCorrect = (task.clicks || []).includes(m.index);
            let isWrong   = (wrongIndices || []).includes(m.index);

            if (isCorrect)      { knn.addExample(feat, pText); knn.addExample(feat, pText); }
            else if (isWrong)   { knn.addExample(feat, negLabel); knn.addExample(feat, negLabel); knn.addExample(feat, negLabel); }
            else                { knn.addExample(feat, negLabel); }
        } catch(e) {}
    }
    await saveKNN();
    return { type: 'trained', taskId: task.id };
}

// ── Predict ───────────────────────────────────────────────
async function predictTask(task) {
    if (!isReady) return { type: 'predict_result', taskId: task.id, clicks: [] };
    let pText   = task.prompt.split('|||')[0].trim().toLowerCase();
    let dataset = knn.getClassExampleCount();
    if (!dataset[pText]) return { type: 'predict_result', taskId: task.id, clicks: [] };

    let predictedClicks = [];
    for (let m of (task.media || [])) {
        let src = m.thumb || m.src;
        if (!src) continue;
        try {
            let resp   = await fetch(src);
            let blob   = await resp.blob();
            let bitmap = await createImageBitmap(blob);
            let feat   = await getFeatures(bitmap);
            bitmap.close();

            let res     = await knn.predictClass(feat);
            let conf    = res.confidences[pText] || 0;
            let negConf = res.confidences['negative_' + pText] || 0;
            if (res.label === pText && conf > 0.85 && negConf < 0.30) predictedClicks.push(m.index);
        } catch(e) {}
    }
    return { type: 'predict_result', taskId: task.id, clicks: predictedClicks };
}

// ── Port handler ──────────────────────────────────────────
self.onconnect = (e) => {
    let port = e.ports[0];
    ports.add(port);

    // Foran status bhejo
    if (isReady) {
        let concepts = Object.keys(knn.getClassExampleCount()).filter(k => !k.startsWith('negative_')).length;
        port.postMessage({ type: 'status', text: `🤖 AI Ready ✅ (${concepts} concepts)`, ready: true });
    } else {
        port.postMessage({ type: 'status', text: '🧠 AI Loading...', ready: false });
    }

    port.onmessage = async (ev) => {
        let msg = ev.data;
        try {
            if (msg.type === 'train') {
                let result = await trainTask(msg.task, msg.wrongIndices || []);
                if (result) port.postMessage(result);
            }
            else if (msg.type === 'predict') {
                let result = await predictTask(msg.task);
                port.postMessage(result);
            }
            else if (msg.type === 'get_status') {
                let concepts = isReady ? Object.keys(knn.getClassExampleCount()).filter(k => !k.startsWith('negative_')).length : 0;
                port.postMessage({ type: 'status', text: isReady ? `🤖 AI Ready ✅ (${concepts} concepts)` : '🧠 AI Loading...', ready: isReady });
            }
        } catch(err) {
            port.postMessage({ type: 'error', msg: err.message });
        }
    };

    port.onmessageerror = () => {};
    port.start();

    // Tab disconnect detect
    port.addEventListener('close', () => ports.delete(port));
};

// ── Start ─────────────────────────────────────────────────
initAI();
