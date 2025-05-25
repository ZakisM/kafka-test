const { Kafka, CompressionTypes, logLevel } = require("kafkajs");
const { SchemaRegistry, SchemaType } = require("@kafkajs/confluent-schema-registry");

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: ["localhost:9092"],
  clientId: "example-producer",
});

const topic = "individual-record";
const producer = kafka.producer();
const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_HOST || "http://localhost:8081",
});

// Define a simple Avro schema (replace with your actual schema)
const schema = {
  type: "record",
  name: "RandomNumber",
  fields: [
    { name: "key", type: "string" },
    { name: "value", type: "string" },
  ],
};

let registryId;

const getRandomNumber = () => Math.round(Math.random(10) * 1000);
const createMessage = async (num) => {
  const message = {
    key: `key-${num}`,
    value: `value-${num}-${new Date().toISOString()}`,
  };
  // Encode the message payload using the schema registry
  const payload = await registry.encode(registryId, message);
  return { key: message.key, value: payload };
};


const sendMessage = async () => {
  try {
    const num = getRandomNumber();
    const message = await createMessage(num); // Updated to use async createMessage

    const data = await producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [message], // Ensure message is in the correct format
    });

    return console.log(data);
  } catch (e) {
    return console.error(`[example/producer] ${e.message}`, e);
  }
};

const run = async () => {
  await producer.connect();
  // Register the schema and get the ID
  const { id } = await registry.register({
    type: SchemaType.AVRO,
    schema: JSON.stringify(schema),
  });
  registryId = id;

  setInterval(sendMessage, 3000);
};

run().catch((e) => console.error(`[example/producer] ${e.message}`, e));

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

for (const type of errorTypes) {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await producer.disconnect();
      process.exit(0);
    } catch {
      process.exit(1);
    }
  });
}

for (const type of signalTraps) {
  process.once(type, async () => {
    try {
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
}
