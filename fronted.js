const { Kafka, CompressionTypes } = require("kafkajs");

const kafka = new Kafka({
  clientId: "test_producer",
  brokers: ["localhost:9092"], // replace with your broker address
});

const producer = kafka.producer();

const run = async () => {
  await producer.connect();

  const userId = "user123";
  const chatId = "chat456";
  const key = `${userId}:${chatId}`;

  const payload = {
    messageId: "msg001",
    text: "diocane",
    status: "sent",
    attempt: 1,
    timestamp: new Date().toISOString(),
    // errorMsg is optional
  };

  await producer.send({
    topic: "chat-messages",
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key,
        value: JSON.stringify(payload),
      },
    ],
  });

  console.log(`✅ Sent message with key "${key}"`);
  await producer.disconnect();
};

run().catch(console.error);
