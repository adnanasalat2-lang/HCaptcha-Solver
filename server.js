const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' }, maxHttpBufferSize: 1e8 });

app.use(cors());
app.use(express.json({ limit: '60mb' }));

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');

const MAX_PENDING = 200;
const MAX_TRAINED = 10000;

let hcaptchaPending = {};
let hcaptchaTrained = {};

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            const data = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
            hcaptchaTrained = data.trained || {};
            console.log(`[DB] Engine Loaded. Trained: ${Object.keys(hcaptchaTrained).length}`);
        } catch (e) {}
    }
}
initDB();

function persistDatabase() {
    fs.writeFileSync(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8');
}

app.post('/api/new-hcaptcha', (req, res) => {
    const task = req.body;
    if (!task || !task.taskId) return res.json({ success: false });

    // Exact Match (DB check)
    if (hcaptchaTrained[task.taskId]) {
        return res.json({ success: true, autoSolved: true, clicks: hcaptchaTrained[task.taskId].clicks });
    }

    task.type = (task.media && task.media.length > 1) ? 'grid' : 'coord';
    task.clicks = [];
    task.aiPredicted = false;

    hcaptchaPending[task.taskId] = task;

    // Send to all Dashboards (Master will predict, Workers will display)
    io.emit('newTask', hcaptchaPending[task.taskId]);

    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) delete hcaptchaPending[keys[0]];

    res.json({ success: true, autoSolved: false });
});

app.post('/api/submit-hcaptcha', (req, res) => {
    const { taskId, clicks, wrongIndices } = req.body;
    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    
    if (source) {
        hcaptchaTrained[taskId] = { ...source, clicks: clicks, trainedAt: new Date().toISOString() };
        delete hcaptchaPending[taskId];
        persistDatabase();
        
        // Broadcast completion (Master Node will catch this and Train the AI)
        io.emit('taskCompleted', { task: hcaptchaTrained[taskId], wrongIndices });
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

io.on('connection', (socket) => {
    socket.emit('initialState', { pending: hcaptchaPending, trained: hcaptchaTrained });

    // Receive Prediction from Master Node
    socket.on('aiPredictionResult', (data) => {
        const { taskId, clicks } = data;
        if (hcaptchaPending[taskId]) {
            hcaptchaPending[taskId].clicks = clicks;
            hcaptchaPending[taskId].aiPredicted = true;
            // Send updated task (with pink borders) to all Workers
            io.emit('updateTask', hcaptchaPending[taskId]);
        }
    });
});

app.get('/', (req, res) => { res.sendFile(path.join(__dirname, 'hcaptcha-dashboard.html')); });
server.listen(process.env.PORT || 3000, () => console.log(`🚀 Relay Server Running on Port ${process.env.PORT || 3000}`));
