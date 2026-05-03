import { Kafka } from 'kafkajs';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

const kafka = new Kafka({
    clientId: 'live-location-tracker',
    brokers: KAFKA_BROKERS,
});

export const producer = kafka.producer();
export const consumer = kafka.consumer({ groupId: 'location-broadcast' });
export const dbConsumer = kafka.consumer({ groupId: 'location-db-processor' });
export const admin = kafka.admin();

export default kafka;
