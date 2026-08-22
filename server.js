const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json({ limit: '60mb' }));

const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');

const MAX_PENDING = 200;
const MAX_TRAINED = 5000;

let hcaptchaPending = {};
let hcaptchaTrained = {};

function evaluateAutoSolve(task) {
    if (hcaptchaTrained[task.taskId]) {
        return { solved: true, clicks: hcaptchaTrained[task.taskId].clicks || [] };
    }
    return { solved: false };
}

function initDB() {
    if (fs.existsSync(DB_FILE)) {
        try {
            const data = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
            hcaptchaTrained = data.trained || {};
            console.log(`[DB] Loaded. Trained: ${Object.keys(hcaptchaTrained).length}`);
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

// ---------------------------
// ROUTES & SOCKETS
// ---------------------------
app.post('/api/new-hcaptcha', (req, res) => {
    const task = req.body;
    if (!task || !task.taskId) return res.json({ success: false });

    let autoRes = evaluateAutoSolve(task);
    if (autoRes.solved) {
        return res.json({ success: true, autoSolved: true, clicks: autoRes.clicks });
    }

    const isGrid = task.media && task.media.length > 1;
    hcaptchaPending[task.taskId] = {
        id: task.taskId,
        prompt: task.prompt,
        media: task.media,
        type: isGrid ? 'grid' : 'coord',
        timestamp: task.timestamp
    };

    // Naya task aate hi sab connected dashboards ko bhej do
    io.emit('newTask', hcaptchaPending[task.taskId]);

    const keys = Object.keys(hcaptchaPending);
    if (keys.length >= MAX_PENDING) delete hcaptchaPending[keys[0]];

    res.json({ success: true, autoSolved: false });
});

app.post('/api/submit-hcaptcha', (req, res) => {
    const { taskId, clicks } = req.body;
    let source = hcaptchaPending[taskId] || hcaptchaTrained[taskId];
    if (source) {
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

app.get('/api/get-hcaptcha', (req, res) => {
    res.json({ pending: hcaptchaPending, trained: hcaptchaTrained });
});

io.on('connection', (socket) => {
    // Jab koi worker dashboard khole to usay current state bhej do
    socket.emit('initialState', { pending: hcaptchaPending, trained: hcaptchaTrained });
});

app.get('/', (req, res) => {
    let f = path.join(__dirname, 'hcaptcha-dashboard.html');
    if (fs.existsSync(f)) res.sendFile(f);
    else res.status(404).send('hcaptcha-dashboard.html not found.');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Server Running with WebSockets on Port ${PORT}`));
