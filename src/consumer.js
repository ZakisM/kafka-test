const { Kafka, logLevel } = require("kafkajs");
const { SchemaRegistry, MAGIC_BYTE } = require("@kafkajs/confluent-schema-registry");
const avsc = require("avsc");

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: ["localhost:9092"],
  clientId: "example-consumer",
});

const topic = "individual-record";
const consumer = kafka.consumer({ groupId: "test-group" });
const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_HOST || "http://localhost:8081",
});

const validatePayloads = process.env.VALIDATE_PAYLOADS !== "false";


const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      let decodedValue;

      if (validatePayloads) {
        // Decode (and validate) the message payload using the schema registry
        decodedValue = await registry.decode(message.value);
      } else {
        // Manual decoding without schema registry validation
        try {
          if (!message.value) {
            throw new Error("Message value is null or undefined.");
          }
          const buffer = Buffer.from(message.value); // Ensure it's a buffer
          // Extract schema ID (bytes 1-4)
          const magicByte = buffer.readUInt8(0);
          if (magicByte !== MAGIC_BYTE) { // MAGIC_BYTE is typically 0
             console.warn(`[example/consumer] Unknown magic byte: ${magicByte}`);
          }
          const schemaId = buffer.readInt32BE(1);
          // Fetch schema from registry
          const schema = await registry.getSchema(schemaId);
          // Deserialize payload (bytes after first 5)
          const actualPayload = buffer.slice(5);
          decodedValue = avsc.Type.forSchema(JSON.parse(schema.schema)).fromBuffer(actualPayload);
        } catch (e) {
          console.error(`[example/consumer] Error decoding message manually: ${e.message}`, e);
          // Fallback or error logging
          decodedValue = "Error decoding message";
        }
      }
      console.log(`- ${prefix} ${message.key}#${JSON.stringify(decodedValue)}`);
    },
  });
};

run().catch((e) => console.error(`[example/consumer] ${e.message}`, e));

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

for (const type of errorTypes) {
  process.on(type, async (e) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await consumer.disconnect();
      process.exit(0);
    } catch {
      process.exit(1);
    }
  });
}

for (const type of signalTraps) {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
}
