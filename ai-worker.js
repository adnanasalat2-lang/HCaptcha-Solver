// ============================================================
// ai-worker.js — SharedWorker (Normalized & Fast Learning Edition)
// ============================================================

/* global importScripts, SharedWorkerGlobalScope */

importScripts(
    'https://cdn.jsdelivr.net/npm/@tensorflow/tfjs@4.20.0/dist/tf.min.js',
    'https://cdn.jsdelivr.net/npm/@tensorflow-models/mobilenet@2.1.1/dist/mobilenet.min.js',
    'https://cdn.jsdelivr.net/npm/@tensorflow-models/knn-classifier@1.2.4/dist/knn-classifier.min.js'
);

const ports = new Set();

let isReady  = false;
let aiModel  = null;
let knn      = null;
let knnStore = {};

const IDB_NAME  = 'hcm_knn';
const IDB_STORE = 'knn';
const IDB_KEY   = 'model_v3';

function cleanLabel(raw) {
    return (raw || '').split('|||')[0].replace(/[\n\r\t]+/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
}

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

function broadcast(msg) {
    ports.forEach(p => { try { p.postMessage(msg); } catch(e) {} });
}

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

async function trainTask(task, wrongIndices) {
    if (!isReady || task.type !== 'grid') return;
    let pText    = cleanLabel(task.prompt);
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

            if (isCorrect) {
                for (let i = 0; i < 4; i++) knn.addExample(feat, pText);
            } else if (isWrong) {
                for (let i = 0; i < 6; i++) knn.addExample(feat, negLabel);
            } else {
                knn.addExample(feat, negLabel);
                knn.addExample(feat, negLabel);
            }
        } catch(e) {}
    }
    await saveKNN();
    return { type: 'trained', taskId: task.id };
}

async function predictTask(task) {
    if (!isReady || task.type !== 'grid') return { type: 'predict_result', taskId: task.id, clicks: [] };
    let pText   = cleanLabel(task.prompt);
    let dataset = knn.getClassExampleCount();
    
    // Fuzzy/Clean match agar prompt formatting mein halka farq ho
    if (!dataset[pText]) {
        let keys = Object.keys(dataset).filter(k => !k.startsWith('negative_'));
        let matched = keys.find(k => k === pText || k.includes(pText) || pText.includes(k));
        if (matched) pText = matched;
        else return { type: 'predict_result', taskId: task.id, clicks: [] };
    }

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

            if (res.label === pText && conf >= 0.45 && negConf < 0.40) {
                predictedClicks.push(m.index);
            }
        } catch(e) {}
    }
    return { type: 'predict_result', taskId: task.id, clicks: predictedClicks };
}

self.onconnect = (e) => {
    let port = e.ports[0];
    ports.add(port);

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
                if (result) {
                    port.postMessage(result);
                    broadcast({ type: 'trained' });
                }
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
    port.addEventListener('close', () => ports.delete(port));
};

initAI();
