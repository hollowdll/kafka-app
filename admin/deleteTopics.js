import { Kafka, logLevel } from 'kafkajs';
import { kafkaTopicName } from '../const.js';
console.log("*** Admin starts... (Run this only once for each created Kafka server) ***");

const kafka = new Kafka({
    clientId: 'my-admin-delete-app',
    brokers: ['localhost:9092'],
    logLevel: logLevel.INFO
});
const admin = kafka.admin();

const run = async () => {
    await admin.connect();
    const topics = await admin.listTopics();

    if(topics.includes(kafkaTopicName)) {
        await admin.deleteTopics({
            topics: [kafkaTopicName],
            timeout: 5000, // default: 5000
        });
    } else {
        console.log("Topics already deleted!");
    }

    await admin.disconnect()
    console.log("*** Admin delete topic steps completed ***");
};

run().catch(console.error);