
const app = express();
app.use(cors());
app.use(express.json({ limit: '100mb' }));
app.use(express.json({ limit: '60mb' }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
@@ -17,14 +17,14 @@ const DATA_DIR = fs.existsSync('/data') ? '/data' : __dirname;
const DB_FILE = path.join(DATA_DIR, 'database.json');
const AI_BRAIN_FILE = path.join(DATA_DIR, 'ai_brain.json');

const MAX_PENDING = 60;
const MAX_PENDING = 40;
const MAX_TRAINED = 50000;
const DASHBOARD_PAGE_SIZE = 20;
const DASHBOARD_PAGE_SIZE = 15;

let hcaptchaPending = {};
let hcaptchaTrained = {};
let conceptBank = {};
let aiBrainDataset = {}; // Persistent Server-Side AI Brain
let aiBrainDataset = {};

const browserSockets = new Map();

@@ -91,16 +91,16 @@ function initDB() {
hcaptchaPending = {};
hcaptchaTrained = data.trained || {};
rebuildConceptBank();
            console.log(`[DB] Engine Loaded. Trained: ${Object.keys(hcaptchaTrained).length}`);
            console.log(`[DB] Loaded. Trained: ${Object.keys(hcaptchaTrained).length}`);
} catch (e) {
            console.error("[DB] Error loading database:", e.message);
            console.error("[DB] Error loading DB:", e.message);
}
}
if (fs.existsSync(AI_BRAIN_FILE)) {
try {
const rawBrain = fs.readFileSync(AI_BRAIN_FILE, 'utf8');
aiBrainDataset = JSON.parse(rawBrain);
            console.log(`[AI-BRAIN] Server Memory Restored. Concepts: ${Object.keys(aiBrainDataset).length}`);
            console.log(`[AI-BRAIN] Restored: ${Object.keys(aiBrainDataset).length} concepts`);
} catch(e) {
aiBrainDataset = {};
}
@@ -112,20 +112,16 @@ let saveTimeout = null;
function persistDatabase() {
if (saveTimeout) clearTimeout(saveTimeout);
saveTimeout = setTimeout(() => {
        fs.writeFile(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8', (err) => {
            if (err) console.error('[DB] PERSIST ERROR:', err.message);
        });
    }, 2000);
        fs.writeFile(DB_FILE, JSON.stringify({ trained: hcaptchaTrained }), 'utf8', () => {});
    }, 2500);
}

let aiSaveTimeout = null;
function persistAIBrain() {
if (aiSaveTimeout) clearTimeout(aiSaveTimeout);
aiSaveTimeout = setTimeout(() => {
        fs.writeFile(AI_BRAIN_FILE, JSON.stringify(aiBrainDataset), 'utf8', (err) => {
            if (err) console.error('[AI-BRAIN] PERSIST ERROR:', err.message);
        });
    }, 2000);
        fs.writeFile(AI_BRAIN_FILE, JSON.stringify(aiBrainDataset), 'utf8', () => {});
    }, 3000);
}

function evaluateAutoSolve(task) {
@@ -213,7 +209,6 @@ function notifyBrowsers(taskId, clicks) {
}
}

// ── Server-Side AI Brain Sync Routes ──
app.get('/api/ai-brain', (req, res) => {
res.json(aiBrainDataset || {});
});
@@ -245,7 +240,7 @@ app.post('/api/new-hcaptcha', (req, res) => {

const keys = Object.keys(hcaptchaPending);
if (keys.length >= MAX_PENDING) {
        keys.slice(0, 15).forEach(k => delete hcaptchaPending[k]);
        keys.slice(0, 10).forEach(k => delete hcaptchaPending[k]);
}

hcaptchaPending[task.taskId] = {
@@ -271,12 +266,13 @@ app.post('/api/submit-hcaptcha', (req, res) => {
if (source) {
let isVideo = source.media && (source.media.length === 1 || source.media[0].type === 'video_frames');

        // Light storage: strip large video base64 arrays to keep DB super light
let lightMedia = (source.media || []).map(m => ({
dhash: m.dhash || "",
stableHash: m.stableHash || "",
type: m.type || "image",
index: m.index !== undefined ? m.index : 0,
            thumb: m.thumb || (m.frames ? m.frames[0] : "")
            thumb: m.thumb || (m.frames ? m.frames[0] : (m.src ? m.src.substring(0, 500) : ""))
}));

if (!isVideo) {
@@ -312,10 +308,11 @@ app.post('/api/submit-hcaptcha', (req, res) => {
res.json({ success: true });
});

// Fast Paged Tasks API (Super Quick Load)
app.get('/api/tasks', (req, res) => {
const tab = req.query.tab === 'trained' ? 'trained' : 'pending';
const page = Math.max(0, parseInt(req.query.page) || 0);
    const size = Math.min(30, Math.max(1, parseInt(req.query.size) || DASHBOARD_PAGE_SIZE));
    const size = tab === 'trained' ? DASHBOARD_PAGE_SIZE : 40;

let source = tab === 'trained' ? hcaptchaTrained : hcaptchaPending;
let ids = Object.keys(source);
