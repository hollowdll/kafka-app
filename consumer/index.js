import { Kafka } from 'kafkajs';
import { kafkaTopic, convertedResultTopic } from '../const.js';

console.log('*** Consumer starts... ***');

const kafka = new Kafka({
    clientId: 'kafka-app-consumer',
    brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'kafka-consumers1' });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const celcius = (message.value - 32) * (5/9);
            console.log({
                topic: topic,
                key: message.key.toString(),
                offset: message.offset,
                fahrenheit: message.value.toString(),
                celsius: celcius.toFixed(2),
                timestamp: new Date(parseInt(message.timestamp)).toISOString(),
            });
        }
    })
}

run().catch(console.error);