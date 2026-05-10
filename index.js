const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const https = require('https');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

app.use(cors());
app.use(express.json());
app.use(morgan('dev'));

// Add a landing page for the root URL
app.get('/', (req, res) => {
    res.send('<h1>WApp Clone API</h1><p>Status: 🟢 Running</p>');
});

const TURSO_DB_URL = "sqlite-lifeisperpetual.aws-ap-south-1.turso.io";
const TURSO_AUTH_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzgwNTk2MjcsImlkIjoiMDE5ZGZjOWItMDAwMS03ZGFmLTljNGEtZTJiYjI1ZDY2YjRlIiwicmlkIjoiOTJlMjJiYmYtZTcwYS00NjEwLWE0MDUtODZmYWQ0NTYwODk2In0.g98Eu3QDgZRmIPRG72gS0Xpem8IZwRYEU8dMzjdIveOe5R-ejwo0IRNUWvOB-9SZbMGyT1b4RdG0uFWVdq7yCw";

function tursoExecute(sql, args = []) {
    return new Promise((resolve, reject) => {
        const body = JSON.stringify({
            requests: [
                { type: 'execute', stmt: { sql, args: args.map(a => {
                    if (typeof a === 'boolean') return { type: 'integer', value: a ? 1 : 0 };
                    if (typeof a === 'number') return { type: 'integer', value: a };
                    return { type: 'text', value: String(a) };
                }) } },
                { type: 'close' }
            ]
        });

        const req = https.request({
            hostname: TURSO_DB_URL,
            path: '/v2/pipeline',
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${TURSO_AUTH_TOKEN}`,
                'Content-Type': 'application/json',
                'Content-Length': body.length
            }
        }, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                const json = JSON.parse(data);
                if (json.error) return reject(new Error(json.error.message));
                const result = json.results[0].response.result;
                const rows = result.rows.map(row => {
                    const obj = {};
                    result.cols.forEach((col, i) => obj[col.name] = row[i].value);
                    return obj;
                });
                resolve({ rows });
            });
        });

        req.on('error', (e) => reject(e));
        req.write(body);
        req.end();
    });
}

// Socket.io Connection Logic
io.on('connection', (socket) => {
    console.log('User connected:', socket.id);

    // Users join a room named after their userId
    socket.on('join', (userId) => {
        socket.join(userId);
        console.log(`User ${userId} joined room`);
    });

    socket.on('disconnect', () => {
        console.log('User disconnected');
    });
});

async function initDb() {
    try {
        console.log("Connecting to Remote Turso...");
        await tursoExecute("CREATE TABLE IF NOT EXISTS Users (id TEXT PRIMARY KEY, name TEXT, phoneNumber TEXT UNIQUE)");
        await tursoExecute("CREATE TABLE IF NOT EXISTS Messages (id INTEGER PRIMARY KEY AUTOINCREMENT, senderId TEXT, receiverId TEXT, text TEXT, imagePath TEXT, timestamp TEXT)");
        await tursoExecute("CREATE TABLE IF NOT EXISTS ChatSummaries (id INTEGER PRIMARY KEY AUTOINCREMENT, userId TEXT, contactId TEXT, contactName TEXT, lastMessage TEXT, updatedAt TEXT, UNIQUE(userId, contactId))");
        console.log("Connected to Remote Turso Database.");

        const PORT = process.env.PORT || 4000;
        server.listen(PORT, () => console.log(`API & Socket running on ${PORT}`));
    } catch (err) {
        console.error("Database error:", err.message);
    }
}

initDb();

app.get('/users/search', async (req, res) => {
    const { phone } = req.query;
    try {
        const result = await tursoExecute('SELECT id, name, phoneNumber FROM Users WHERE phoneNumber = ?', [phone]);
        if (result.rows.length > 0) {
            res.json({ user: result.rows[0] });
        } else {
            res.status(404).json({ error: 'User not found' });
        }
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/users/register', async (req, res) => {
    const { id, name, phoneNumber } = req.body;
    try {
        await tursoExecute('INSERT OR REPLACE INTO Users (id, name, phoneNumber) VALUES (?, ?, ?)', [id, name, phoneNumber]);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/users/:userId/chats', async (req, res) => {
    try {
        const result = await tursoExecute('SELECT * FROM ChatSummaries WHERE userId = ? ORDER BY updatedAt DESC', [req.params.userId]);
        res.json({ chats: result.rows });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/messages/:userId/:contactId', async (req, res) => {
    const { userId, contactId } = req.params;
    try {
        const result = await tursoExecute(`SELECT * FROM Messages WHERE (senderId = ? AND receiverId = ?) OR (senderId = ? AND receiverId = ?) ORDER BY timestamp ASC`, [userId, contactId, contactId, userId]);
        const messages = result.rows.map(r => ({
            ...r,
            isMe: r.senderId === userId ? 1 : 0
        }));
        res.json({ messages });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/messages', async (req, res) => {
    const { senderId, receiverId, text, imagePath, timestamp, contactName } = req.body;
    const ts = timestamp || new Date().toISOString();
    
    try {
        await tursoExecute(`INSERT INTO Messages (senderId, receiverId, text, imagePath, timestamp) VALUES (?, ?, ?, ?, ?)`, [senderId, receiverId, text, imagePath, ts]);
        const lastMsg = text || (imagePath ? '[image]' : '');

        const upsertSummary = async (uId, cId, cName, last, updated) => {
            await tursoExecute(`INSERT INTO ChatSummaries (userId, contactId, contactName, lastMessage, updatedAt)
                        VALUES (?, ?, ?, ?, ?) ON CONFLICT(userId, contactId) DO UPDATE SET
                        lastMessage = excluded.lastMessage, updatedAt = excluded.updatedAt, contactName = excluded.contactName`, [uId, cId, cName, last, updated]);
        };

        const userRes = await tursoExecute('SELECT name FROM Users WHERE id = ?', [senderId]);
        const myName = userRes.rows.length > 0 ? userRes.rows[0].name : senderId;

        await upsertSummary(senderId, receiverId, contactName || receiverId, lastMsg, ts);
        await upsertSummary(receiverId, senderId, myName, lastMsg, ts);

        // EMIT REAL-TIME EVENTS
        const newMessage = { senderId, receiverId, text, imagePath, timestamp: ts };

        // Notify the receiver
        io.to(receiverId).emit('new_message', newMessage);
        // Also notify the receiver about the summary update
        io.to(receiverId).emit('update_chats');

        // Notify the sender (for multi-device sync if needed, or just to refresh summary)
        io.to(senderId).emit('update_chats');

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});
