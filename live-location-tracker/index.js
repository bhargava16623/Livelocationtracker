import './env.js';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { Server } from 'socket.io';
import { registerAuthRoutes, requireAuth, socketAuthMiddleware } from './auth.js';
import { producer, consumer } from './kafka-client.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const PORT = process.env.PORT || 4000;
const TOPIC = 'location-updates';

// In-memory store of last known positions: userId -> { lat, lng, name, timestamp }
const activeUsers = new Map();

// Deduplication: track last event per user to ignore duplicates
const lastEventTimestamps = new Map();

async function main() {
    const app = express();
    const server = http.createServer(app);
    const io = new Server(server);

    app.use(express.json());

    // ---- Auth Routes ----
    registerAuthRoutes(app);

    // ---- Public Routes ----
    app.get('/login.html', (req, res) => {
        res.sendFile(path.join(__dirname, 'public', 'login.html'));
    });

    app.get('/health', (req, res) => res.json({ healthy: true }));

    // ---- Protected Routes ----
    app.get('/', requireAuth, (req, res) => {
        res.sendFile(path.join(__dirname, 'public', 'index.html'));
    });

    // Get all currently active users' positions
    app.get('/api/locations', requireAuth, (req, res) => {
        const locations = [];
        for (const [userId, data] of activeUsers) {
            locations.push({ userId, ...data });
        }
        res.json(locations);
    });

    // Serve static files
    app.use(express.static(path.join(__dirname, 'public')));

    // ---- Kafka Producer ----
    await producer.connect();
    console.log('Kafka producer connected');

    // ---- Kafka Consumer (for broadcasting) ----
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const event = JSON.parse(message.value.toString());

                // Deduplicate: skip if same timestamp already processed
                const lastTs = lastEventTimestamps.get(event.userId);
                if (lastTs && lastTs >= event.timestamp) return;
                lastEventTimestamps.set(event.userId, event.timestamp);

                // Update in-memory store
                activeUsers.set(event.userId, {
                    lat: event.lat,
                    lng: event.lng,
                    name: event.name,
                    timestamp: event.timestamp,
                });

                // Broadcast to all connected clients
                io.emit('server:location-update', {
                    userId: event.userId,
                    lat: event.lat,
                    lng: event.lng,
                    name: event.name,
                    timestamp: event.timestamp,
                });
            } catch (err) {
                console.error('Error processing Kafka message:', err);
            }
        },
    });
    console.log('Kafka consumer running');

    // ---- Socket.IO ----
    io.use(socketAuthMiddleware);

    io.on('connection', (socket) => {
        const userId = socket.user.sub;
        const userName = socket.user.name || socket.user.email || 'Anonymous';
        console.log('Socket connected', { id: socket.id, user: userId });

        // Send current active users to newly connected client
        const locations = [];
        for (const [uid, data] of activeUsers) {
            locations.push({ userId: uid, ...data });
        }
        socket.emit('server:initial-locations', locations);

        // Handle location update from client
        socket.on('client:location-update', async (data) => {
            const { lat, lng } = data;

            // Validate input
            if (typeof lat !== 'number' || typeof lng !== 'number') {
                socket.emit('server:error', { error: 'Invalid location data.' });
                return;
            }
            if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
                socket.emit('server:error', { error: 'Coordinates out of range.' });
                return;
            }

            const event = {
                userId,
                name: userName,
                lat,
                lng,
                timestamp: Date.now(),
            };

            // Publish to Kafka
            try {
                await producer.send({
                    topic: TOPIC,
                    messages: [
                        {
                            key: userId,
                            value: JSON.stringify(event),
                        },
                    ],
                });
            } catch (err) {
                console.error('Failed to publish to Kafka:', err);
                socket.emit('server:error', { error: 'Failed to process location.' });
            }
        });

        // Handle disconnect
        socket.on('disconnect', () => {
            console.log('Socket disconnected', { id: socket.id, user: userId });
            activeUsers.delete(userId);
            io.emit('server:user-disconnected', { userId });
        });
    });

    // ---- Start Server ----
    server.listen(PORT, () => {
        console.log(`Live Location Tracker running on http://localhost:${PORT}`);
    });
}

main().catch((err) => {
    console.error('Failed to start server:', err);
    process.exit(1);
});
