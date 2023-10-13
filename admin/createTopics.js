import { Kafka, logLevel } from 'kafkajs'; 
import { kafkaTopic, convertedResultTopic } from '../const.js';
console.log("*** Admin starts... (Only needed once for each created Kafka server, actually) ***");

const kafka = new Kafka({
    clientId: 'my-admin-create-app',
    brokers: ['localhost:9092'],
    logLevel: logLevel.INFO
});
const admin = kafka.admin();

// Creates topics if they don't exist
const run = async () => {
    await admin.connect();
    const topics = await admin.listTopics();

    if(!topics.includes(kafkaTopic) && !topics.includes(convertedResultTopic)) {
        console.log(`Creating topic ${kafkaTopic} ...`);
        console.log(`Creating topic ${convertedResultTopic} ...`);

        await admin.createTopics({
            validateOnly: false,
            waitForLeaders: true,
            timeout: 5000,
            topics: [
                {
                    topic: kafkaTopic,
                    numPartitions: 1,     // default: -1 (uses broker `num.partitions` configuration)
                    replicationFactor: 1, // default: -1 (uses broker `default.replication.factor` configuration)
                    replicaAssignment: [],  // Example: [{ partition: 0, replicas: [0,1,2] }] - default: []
                    configEntries: []       // Example: [{ name: 'cleanup.policy', value: 'compact' }] - default: []
                },
                {
                    topic: convertedResultTopic,
                    numPartitions: 1,
                    replicationFactor: 1,
                    replicaAssignment: [],
                    configEntries: []
                }
            ]
        });
    } else {
        console.log("Topics already created!");
    }

    await admin.disconnect()
    console.log("*** Admin create topic steps completed ***");
};

run().catch(console.error);