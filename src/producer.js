const { Kafka, CompressionTypes, logLevel } = require("kafkajs");

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: ["localhost:9092"],
  clientId: "example-producer",
});

const topic = "individual-record";
const producer = kafka.producer();

const getRandomNumber = () => Math.round(Math.random(10) * 1000);
const createMessage = (num) => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
});

const sendMessage = async () => {
  try {
    const data = await producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [
        {
          key: `key-${getRandomNumber()}`,
          value: `value-${new Date().toISOString()}`,
        },
      ],
    });

    return console.log(data);
  } catch (e) {
    return console.error(`[example/producer] ${e.message}`, e);
  }
};

const run = async () => {
  await producer.connect();

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
