import { Kafka } from 'kafkajs';
import { kafkaTopicName } from '../const.js';

console.log('*** Consumer starts... ***');

const kafka = new Kafka({
    clientId: 'kafka-app-consumer',
    brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'kafka-consumers1' });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: kafkaTopicName, fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                topic: topic,
                key: message.key.toString(),
                offset: message.offset,
                value: message.value.toString(),
                timestamp: new Date(parseInt(message.timestamp)).toISOString(),
            });
        }
    })
}

run().catch(console.error);