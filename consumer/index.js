// Original consumer that converts received fahrenheit degrees to celcius degrees
// and sends the converted celcius degrees back.

import { Kafka, Partitioners } from 'kafkajs';
import { kafkaTopic, convertedResultTopic } from '../const.js';

console.log('*** Consumer starts... ***');

const kafka = new Kafka({
    clientId: 'kafka-app-consumer',
    brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'kafka-consumers1' });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
    await producer.connect();

    // Consume messages in topic 'mytopic' and send response to topic 'convertedresult'
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const celcius = ((message.value - 32) * (5/9)).toFixed(2);

            console.log(`Message '${message.key}' received from topic '${topic}'`);
            console.log({
                topic: topic,
                key: message.key.toString(),
                offset: message.offset,
                fahrenheit: message.value.toString(),
                celsius: celcius,
                timestamp: new Date(parseInt(message.timestamp)).toISOString(),
            });

            const success = await producer.send({
                topic: convertedResultTopic,
                messages: [
                    {
                        key: message.key,
                        value: celcius
                    }
                ]
            });
        
            if (success) {
                console.log(`Message '${message.key}' sent to topic '${convertedResultTopic}'`);
            } else {
                console.log(`Failed to send message to topic '${convertedResultTopic}'`);
            }
        }
    });
}

run().catch(console.error);