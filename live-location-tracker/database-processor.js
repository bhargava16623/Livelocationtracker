import './env.js';
import { dbConsumer } from './kafka-client.js';

const TOPIC = 'location-updates';

// In-memory location history store (simulates a database)
const locationHistory = [];

async function main() {
    await dbConsumer.connect();
    console.log('Database processor consumer connected');

    await dbConsumer.subscribe({ topic: TOPIC, fromBeginning: false });

    await dbConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const event = JSON.parse(message.value.toString());

                // Simulate database insert — store location history
                const record = {
                    id: locationHistory.length + 1,
                    userId: event.userId,
                    name: event.name,
                    lat: event.lat,
                    lng: event.lng,
                    timestamp: event.timestamp,
                    storedAt: Date.now(),
                };

                locationHistory.push(record);

                console.log(
                    `[DB] Stored location #${record.id}: ` +
                    `user=${record.name} (${record.userId.slice(0, 8)}...) ` +
                    `lat=${record.lat.toFixed(6)} lng=${record.lng.toFixed(6)} ` +
                    `at ${new Date(record.timestamp).toISOString()}`
                );

                // Periodic stats
                if (locationHistory.length % 100 === 0) {
                    const uniqueUsers = new Set(locationHistory.map(r => r.userId)).size;
                    console.log(
                        `[DB] Stats: ${locationHistory.length} total records, ` +
                        `${uniqueUsers} unique users tracked`
                    );
                }
            } catch (err) {
                console.error('[DB] Error processing message:', err);
            }
        },
    });

    console.log('Database processor running — consuming location events...');
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n[DB] Shutting down...');
    console.log(`[DB] Total records stored: ${locationHistory.length}`);
    await dbConsumer.disconnect();
    process.exit(0);
});

main().catch((err) => {
    console.error('Database processor failed to start:', err);
    process.exit(1);
});
