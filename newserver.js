const { Kafka } = require("kafkajs");
const axios = require("axios");
const FormData = require("form-data");
const fs = require("fs");

const kafka = new Kafka({
  clientId: "chat_backend",
  brokers: ["localhost:9092"],
});

const ChatAI = "http://localhost:9000/chat";
const PYTHON_API = "http://localhost:1011";

const getPythonEndpoint = (modelNumber) => {
  switch (modelNumber) {
    case 101: return { endpoint: "/analyze_full_pipeline", expectsFile: true };
    case 102: return { endpoint: "/analyze_speech_to_text", expectsFile: true };
    case 103: return { endpoint: "/analyze_sentiment", field: "sentiment", expectsFile: false };
    case 104: return { endpoint: "/analyze_summary", field: "summury", expectsFile: false };
    case 105: return { endpoint: "/analyze_translation", field: "translate", expectsFile: false };
    case 106: return { endpoint: "/analyze_cover_topic", field: "topic", expectsFile: false };
    case 107: return { endpoint: "/analyze_speech_to_text_summary_sentiment", expectsFile: true };
    default: return null;
  }
};

const consumer = kafka.consumer({ groupId: "chat_reader_group" });
const producer = kafka.producer();

(async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "chat-messages", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString(); // e.g., "user123:chat456"
      const [userId, chatId] = key ? key.split(":") : [null, null];
      let payload;

      try {
        payload = JSON.parse(message.value.toString());
      } catch (err) {
        console.error("❌ Failed to parse Kafka message:", err);
        return;
      }

      console.log(`📩 Processing message ${payload.messageId}`);

      let aiResult = null;
      let status = "processed";
      let errorMsg = null;

      try {
        if (payload.modelNumber < 100 && payload.modelNumber > 0) {
          // Chat AI processing
          const res = await axios.post(ChatAI, {
            text: payload.text,
            modelNumber: payload.modelNumber,
            history: false,
          });
          aiResult = res.data;
        } else {
          // Python pipeline processing
          const pythonConfig = getPythonEndpoint(payload.modelNumber);
          if (!pythonConfig) throw new Error("Invalid model number");

          const form = new FormData();
          if (pythonConfig.expectsFile) {
            if (!payload.filePath) throw new Error("File path required for this model");
            form.append("audio", fs.createReadStream(payload.filePath));
          } else {
            form.append(pythonConfig.field, payload.text);
          }

          const res = await axios.post(`${PYTHON_API}${pythonConfig.endpoint}`, form, {
            headers: form.getHeaders(),
          });
          aiResult = res.data;
        }
      } catch (error) {
        status = "error";
        errorMsg = error.message || "Unknown error during AI processing";
      }

      // ✅ Publish back to Kafka with updated result
      await producer.send({
        topic: topic, // same topic
        messages: [
          {
            key,
            value: JSON.stringify({
              messageId: payload.messageId,
              text: payload.text,
              modelNumber: payload.modelNumber,
              aiResponse: aiResult,
              status,
              attempt: (payload.attempt || 0) + 1,
              timestamp: Date.now(),
              errorMsg,
            }),
          },
        ],
      });

      console.log(`✅ Message ${payload.messageId} processed and re-published with status: ${status}`);
    },
  });
})();











// TODO update for new kafka
async function getUserHistory(username, n = 2) {
  const consumer = kafka.consumer({ groupId: `history-reader-${username}-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: username, fromBeginning: true });

  const messages = [];
  await consumer.run({
    eachMessage: async ({ message }) => {
      messages.push(JSON.parse(message.value.toString()));
    }
  });

  // Wait a short time to gather messages
  await new Promise(resolve => setTimeout(resolve, 1000));
  await consumer.disconnect();

  const totalNeeded = n * 2;
  return messages.slice(-totalNeeded);
}
