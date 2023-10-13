// Original producer that generates random fahrenheit degrees
// and receives converted celcius degrees.

import { Kafka, Partitioners } from 'kafkajs';
import { v4 as UUID } from 'uuid';
import { kafkaTopic, convertedResultTopic } from '../const.js';

console.log('*** Producer starts... ***');

const kafka = new Kafka({
    clientId: 'kafka-app-producer',
    brokers: ['localhost:9092'],
});
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const consumer = kafka.consumer({ groupId: 'kafka-consumers2' });

// Returns a random fahrenheit degree number between min and max
function generateRandomizedFahrenheit(minFahrenheit, maxFahrenheit) {
    return (Math.random() * (maxFahrenheit - minFahrenheit) + minFahrenheit).toFixed(2)
}

// Sends messages to topic 'mytopic'
async function queueMessage() {
    const uuidFraction = UUID().substring(0,8);
    const fahrenheit = generateRandomizedFahrenheit(-75, 150);
    const success = await producer.send({
        topic: kafkaTopic,
        messages: [
            {
                key: uuidFraction,
                value: fahrenheit,
            }
        ]
    });

    if (success) {
        console.log(`Message '${uuidFraction}' sent to topic '${kafkaTopic}' with fahrenheit '${fahrenheit}'`);
    } else {
        console.log(`Failed to send message to topic '${kafkaTopic}'`);
    }
}

const run = async () => {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: convertedResultTopic, fromBeginning: true });

    setInterval(() => queueMessage(), 2500);

    // Consume messages in topic 'convertedresult'
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Message '${message.key}' received from topic '${topic}'`);
            console.log({
                topic: topic,
                key: message.key.toString(),
                offset: message.offset,
                celcius: message.value.toString(),
                timestamp: new Date(parseInt(message.timestamp)).toISOString(),
            });
        }
    });
}

run().catch(console.error);