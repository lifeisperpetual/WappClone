const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const https = require('https');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

app.use(cors());
app.use(express.json());
app.use(morgan('dev'));

const TURSO_DB_URL = "sqlite-lifeisperpetual.aws-ap-south-1.turso.io";
const TURSO_AUTH_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzgwNTk2MjcsImlkIjoiMDE5ZGZjOWItMDAwMS03ZGFmLTljNGEtZTJiYjI1ZDY2YjRlIiwicmlkIjoiOTJlMjJiYmYtZTcwYS00NjEwLWE0MDUtODZmYWQ0NTYwODk2In0.g98Eu3QDgZRmIPRG72gS0Xpem8IZwRYEU8dMzjdIveOe5R-ejwo0IRNUWvOB-9SZbMGyT1b4RdG0uFWVdq7yCw";

app.get('/', (req, res) => res.send('<h1>WApp Clone API</h1><p>Status: 🟢 Running</p>'));

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
                try {
                    const json = JSON.parse(data);
                    if (json.error) return reject(new Error(json.error.message));
                    const step = json.results[0];
                    if (step.type === 'error') return reject(new Error(step.error.message));

                    const result = step.response.result;
                    const rows = result.rows.map(row => {
                        const obj = {};
                        result.cols.forEach((col, i) => obj[col.name] = row[i].value);
                        return obj;
                    });
                    resolve({ rows });
                } catch (e) { reject(e); }
            });
        });
        req.on('error', (e) => reject(e));
        req.write(body);
        req.end();
    });
}

io.on('connection', (socket) => {
    socket.on('join', (userId) => socket.join(userId));
});

// DEBUG ENDPOINT: Visit https://wappclone.onrender.com/debug/users to see everyone registered
app.get('/debug/users', async (req, res) => {
    try {
        const result = await tursoExecute('SELECT * FROM Users');
        res.json(result.rows);
    } catch (err) { res.status(500).send(err.message); }
});

app.get('/users/search', async (req, res) => {
    const { phone } = req.query;
    if (!phone) return res.status(400).json({ error: 'Phone required' });

    // Remove all non-digits for a cleaner search
    const cleanPhone = phone.replace(/\D/g, '');

    try {
        // Search using LIKE to handle numbers stored with or without the '+' sign
        const result = await tursoExecute("SELECT id, name, phoneNumber FROM Users WHERE phoneNumber LIKE ?", [`%${cleanPhone}%`]);
        if (result.rows.length > 0) {
            res.json({ user: result.rows[0] });
        } else {
            res.status(404).json({ error: 'User not found' });
        }
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/users/register', async (req, res) => {
    const { id, name, phoneNumber } = req.body;
    try {
        await tursoExecute('INSERT OR REPLACE INTO Users (id, name, phoneNumber) VALUES (?, ?, ?)', [id, name, phoneNumber]);
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/users/:userId/chats', async (req, res) => {
    try {
        const result = await tursoExecute('SELECT * FROM ChatSummaries WHERE userId = ? ORDER BY updatedAt DESC', [req.params.userId]);
        res.json({ chats: result.rows });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/messages/:userId/:contactId', async (req, res) => {
    const { userId, contactId } = req.params;
    try {
        const result = await tursoExecute(`SELECT * FROM Messages WHERE (senderId = ? AND receiverId = ?) OR (senderId = ? AND receiverId = ?) ORDER BY timestamp ASC`, [userId, contactId, contactId, userId]);
        const messages = result.rows.map(r => ({ ...r, isMe: r.senderId === userId ? 1 : 0 }));
        res.json({ messages });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/messages', async (req, res) => {
    const { senderId, receiverId, text, imagePath, timestamp, contactName } = req.body;
    const ts = timestamp || new Date().toISOString();
    try {
        await tursoExecute(`INSERT INTO Messages (senderId, receiverId, text, imagePath, timestamp) VALUES (?, ?, ?, ?, ?)`, [senderId, receiverId, text, imagePath, ts]);
        const lastMsg = text || (imagePath ? '[image]' : '');
        const upsertSummary = async (uId, cId, cName, last, updated) => {
            await tursoExecute(`INSERT INTO ChatSummaries (userId, contactId, contactName, lastMessage, updatedAt) VALUES (?, ?, ?, ?, ?) ON CONFLICT(userId, contactId) DO UPDATE SET lastMessage = excluded.lastMessage, updatedAt = excluded.updatedAt, contactName = excluded.contactName`, [uId, cId, cName, last, updated]);
        };
        const userRes = await tursoExecute('SELECT name FROM Users WHERE id = ?', [senderId]);
        const myName = userRes.rows.length > 0 ? userRes.rows[0].name : senderId;
        await upsertSummary(senderId, receiverId, contactName || receiverId, lastMsg, ts);
        await upsertSummary(receiverId, senderId, myName, lastMsg, ts);

        io.to(receiverId).emit('new_message', { senderId, receiverId, text, imagePath, timestamp: ts });
        io.to(receiverId).emit('update_chats');
        io.to(senderId).emit('update_chats');
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => console.log('API Running...'));
