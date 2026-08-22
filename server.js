const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');

const tf = require('@tensorflow/tfjs-node');
const mobilenet = require('@tensorflow-models/mobilenet');
const knnClassifier = require('@tensorflow-models/knn-classifier');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' }, maxHttpBufferSize: 1e8 });

app.use(cors());
app.use(express.json({ limit: '60mb' }));

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');
const KNN_FILE = path.join(DATA_DIR, 'knn_dataset.json');

const MAX_PENDING = 200;
const MAX_TRAINED = 10000;

let hcaptchaPending = {};
let hcaptchaTrained = {};

let aiModel = null;
let knn = knnClassifier.create();
let isAIReady = false;

// ==========================================
// 🧠 CENTRALIZED AI ENGINE (FIXED IMAGE PARSER)
// ==========================================
async function initAI() {
    try {
        console.log("[AI] Loading MobileNet Model on Server...");
        aiModel = await mobilenet.load({ version: 2, alpha: 1.0 });
        
        if (fs.existsSync(KNN_FILE)) {
            const raw = fs.readFileSync(KNN_FILE, 'utf8');
            const dataset = JSON.parse(raw);
            const tensors = {};
            for (let k in dataset) {
                tensors[k] = tf.tensor(dataset[k].data, dataset[k].shape);
            }
            knn.setClassifierDataset(tensors);
            console.log(`[AI] KNN Dataset Loaded. Training data exists!`);
        }
        isAIReady = true;
        console.log("[AI] System is Fully Operational 🚀");
    } catch (e) {
        console.error("[AI] Error loading AI:", e);
    }
}

function saveKNN() {
    if (!isAIReady) return;
    try {
        let ds = knn.getClassifierDataset();
        let dataset = {};
        for (let k in ds) {
            dataset[k] = { data: Array.from(ds[k].dataSync()), shape: ds[k].shape };
        }
        fs.writeFileSync(KNN_FILE, JSON.stringify(dataset), 'utf8');
    } catch (e) {}
}

// FIX: Node.js ke liye proper Image downloader aur Base64 parser
async function getTensorFromSource(source) {
    if (!source) return null;
    try {
        let buffer;
        if (source.startsWith('data:image')) {
            const base64Data = source.split(',')[1];
            buffer = Buffer.from(base64Data, 'base64');
        } else if (source.startsWith('http')) {
            const response = await fetch(source);
            const arrayBuffer = await response.arrayBuffer();
            buffer = Buffer.from(arrayBuffer);
        } else {
            return null;
        }
        return tf.node.decodeImage(buffer, 3);
    } catch (e) {
        return null;
    }
}

async function predictWithServerAI(task) {
    if (!isAIReady || !task.media) return [];
    let pText = task.prompt.split('|||')[0].trim().toLowerCase();
    let dataset = knn.getClassExampleCount();
    if (!dataset[pText]) return []; // AI ko abhi ye lafz nahi pata

    let predictedClicks = [];
    for (let m of task.media) {
        try {
            let tfImg = await getTensorFromSource(m.thumb || m.src);
            if (tfImg) {
                let features = aiModel.infer(tfImg, true);
                let result = await knn.predictClass(features);
                
                let conf = result.confidences[pText] || 0;
                let negConf = result.confidences['negative_' + pText] || 0;
                
                if (result.label === pText && conf > 0.85 && negConf < 0.30) {
                    predictedClicks.push(m.index);
                }
                tfImg.dispose();
            }
        } catch(e) {}
    }
    return predictedClicks;
}

async function trainServerAI(task, clicks, wrongIndices = []) {
    if (!isAIReady || !task.media) return;
    let pText = task.prompt.split('|||')[0].trim().toLowerCase();
    let negLabel = 'negative_' + pText;

    let learnedSomething = false;
    for (let m of task.media) {
        try {
            let tfImg = await getTensorFromSource(m.thumb || m.src);
            if (tfImg) {
                let features = aiModel.infer(tfImg, true);
                let isCorrect = clicks.includes(m.index);
                let isWrong = wrongIndices.includes(m.index);

                if (isCorrect) { knn.addExample(features, pText); knn.addExample(features, pText); }
                else if (isWrong) { knn.addExample(features, negLabel); knn.addExample(features, negLabel); knn.addExample(features, negLabel); }
                else { knn.addExample(features, negLabel); }
                
                tfImg.dispose();
                learnedSomething = true;
            }
        } catch(e) {}
    }
    if (learnedSomething) saveKNN();
}

// ==========================================
// DB ENGINE
// ==========================================
function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            const data = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
            hcaptchaTrained = data.trained || {};
        } catch (e) {}
    }
}
initDB();

let saveTimeout = null;
function persistDatabase() {
    if (saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
        fs.writeFileSync(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8');
    }, 1000);
}

// ==========================================
// ROUTES & SOCKETS
// ==========================================
app.post('/api/new-hcaptcha', async (req, res) => {
    const task = req.body;
    if (!task || !task.taskId) return res.json({ success: false });

    if (hcaptchaTrained[task.taskId]) {
        return res.json({ success: true, autoSolved: true, clicks: hcaptchaTrained[task.taskId].clicks });
    }

    const isGrid = task.media && task.media.length > 1;
    task.type = isGrid ? 'grid' : 'coord';
    
    let aiPredictedClicks = [];
    let aiPredicted = false;
    
    if (isGrid) {
        aiPredictedClicks = await predictWithServerAI(task);
        if (aiPredictedClicks.length >= 2 && aiPredictedClicks.length <= 6) {
            aiPredicted = true;
        } else {
            aiPredictedClicks = [];
        }
    }

    hcaptchaPending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media,
        type: task.type,
        timestamp: task.timestamp,
        clicks: aiPredictedClicks,
        aiPredicted: aiPredicted
    };

    res.json({ success: true, autoSolved: false }); 
    io.emit('newTask', hcaptchaPending[task.taskId]);
    
    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) delete hcaptchaPending[keys[0]];
});

app.post('/api/submit-hcaptcha', async (req, res) => {
    const { taskId, clicks, wrongIndices } = req.body;
    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    
    if (source) {
        if (source.type === 'grid') {
            await trainServerAI(source, clicks, wrongIndices || []);
        }

        hcaptchaTrained[taskId] = { ...source, clicks: clicks, trainedAt: new Date().toISOString() };
        delete hcaptchaPending[taskId];
        persistDatabase();
        io.emit('taskCompleted', hcaptchaTrained[taskId]);
    }
    res.json({ success: true });
});

app.delete('/api/delete-hcaptcha/:id', (req, res) => {
    const tid = req.params.id;
    delete hcaptchaPending[tid];
    delete hcaptchaTrained[tid];
    persistDatabase();
    io.emit('taskDeleted', tid);
    res.json({ success: true });
});

app.get('/api/check-hcaptcha/:id', (req, res) => {
    const tid = req.params.id;
    if (hcaptchaTrained[tid]) res.json({ status: 'solved', clicks: hcaptchaTrained[tid].clicks || [] });
    else res.json({ status: 'pending' });
});

io.on('connection', (socket) => {
    socket.emit('initialState', { 
        pending: hcaptchaPending, 
        trained: hcaptchaTrained, 
        aiStatus: isAIReady 
    });
});

app.get('/', (req, res) => { res.sendFile(path.join(__dirname, 'hcaptcha-dashboard.html')); });

initAI().then(() => {
    server.listen(process.env.PORT || 3000, () => console.log(`🚀 Master Server Running on Port ${process.env.PORT || 3000}`));
});
