const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: ["localhost:9092"],
  clientId: "example-consumer",
});

const topic = "individual-record";
const consumer = kafka.consumer({ groupId: "test-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;

      console.log(`- ${prefix} ${message.key}#${message.value}`);
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
