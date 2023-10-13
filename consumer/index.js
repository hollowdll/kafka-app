import { Kafka } from 'kafkajs';
import { kafkaTopicName } from '../const';

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
        eachMessage: async ({ _, _, message }) => {
            console.log({
                key: message.key.toString(),
                partition: message.partition,
                offset: message.offset,
                value: message.value.toString(),
                size: message.size,
                timestamp: message.timestamp.toString(),
            });
        }
    })
}

run().catch(console.error);