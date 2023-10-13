import { Kafka, Partitioners } from 'kafkajs';
import { v4 as UUID } from 'uuid';
import { kafkaTopicName } from '../const.js';

console.log('*** Consumer starts... ***');

const kafka = new Kafka({
    clientId: 'kafka-app-producer',
    brokers: ['localhost:9092'],
});
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const idNumbers = [
    '69',
    '420',
    '69420',
    '42069',
];

function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from + 1))) + from;
}

async function queueMessage() {
    const uuidFraction = UUID().substring(0,8);
    const success = await producer.send({
        topic: kafkaTopicName,
        messages: [
            {
                key: uuidFraction,
                value: Buffer.from(idNumbers[randomizeIntegerBetween(0, idNumbers.length - 1)]),
            }
        ]
    });

    if (success) {
        console.log(`Message ${uuidFraction} sent to the stream`);
    } else {
        console.log('Failed to write message to the stream');
    }
}

const run = async () => {
    await producer.connect();
    setInterval(() => queueMessage(), 2500);
}

run().catch(console.error);