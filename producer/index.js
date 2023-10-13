import { Kafka, Partitioners } from 'kafkajs';
import { v4 as UUID } from 'uuid';
import { kafkaTopic, convertedResultTopic } from '../const.js';

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

// Returns a random fahrenheit degree number between min and max
function generateRandomizedFahrenheit(minFahrenheit, maxFahrenheit) {
    return (Math.random() * (maxFahrenheit - minFahrenheit) + minFahrenheit).toFixed(2)
}

async function queueMessage() {
    const uuidFraction = UUID().substring(0,8);
    const success = await producer.send({
        topic: kafkaTopic,
        messages: [
            {
                key: uuidFraction,
                // value: Buffer.from(idNumbers[randomizeIntegerBetween(0, idNumbers.length - 1)]),
                value: generateRandomizedFahrenheit(-75, 150),
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