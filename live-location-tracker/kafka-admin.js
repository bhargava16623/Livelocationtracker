import './env.js';
import { admin } from './kafka-client.js';

const TOPIC = 'location-updates';

async function setupKafka() {
    await admin.connect();
    console.log('Connected to Kafka admin');

    const existingTopics = await admin.listTopics();

    if (!existingTopics.includes(TOPIC)) {
        await admin.createTopics({
            topics: [
                {
                    topic: TOPIC,
                    numPartitions: 3,
                    replicationFactor: 1,
                },
            ],
        });
        console.log(`Topic "${TOPIC}" created with 3 partitions`);
    } else {
        console.log(`Topic "${TOPIC}" already exists`);
    }

    await admin.disconnect();
    console.log('Kafka admin disconnected');
}

setupKafka().catch((err) => {
    console.error('Failed to set up Kafka:', err);
    process.exit(1);
});
