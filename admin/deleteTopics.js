import { Kafka, logLevel } from 'kafkajs';
import { kafkaTopic, convertedResultTopic } from '../const.js';

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

    if(topics.includes(kafkaTopic)) {
        console.log(`Deleting topic ${kafkaTopic} ...`);

        await admin.deleteTopics({
            topics: [kafkaTopic],
        });
    } else {
        console.log(`${kafkaTopic} already deleted!`);
    }

    if(topics.includes(convertedResultTopic)) {
        console.log(`Deleting topic ${convertedResultTopic} ...`);

        await admin.deleteTopics({
            topics: [convertedResultTopic],
        });
    } else {
        console.log(`${convertedResultTopic} already deleted!`);
    }

    await admin.disconnect()
    console.log("*** Admin delete topic steps completed ***");
};

run().catch(console.error);