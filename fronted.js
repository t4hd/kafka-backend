const { Kafka } = require('kafkajs');
const fs = require('fs');

// Kafka config
const kafka = new Kafka({
  clientId: 'js-file-producer',
  brokers: ['localhost:9092'], // change if needed
});

const producer = kafka.producer();

async function sendFileMessage(topic, key, filePath, modelNumber) {
  try {
    await producer.connect();

    // Read file and convert to base64
    const fileBuffer = fs.readFileSync(filePath);
    const base64File = fileBuffer.toString('base64');

    const message = {
      status: 'incoming',
      pipeline: true,
      modelNumber: modelNumber,
      audio: base64File,  // This key must match your backend's expected field for the file
      timestamp: Math.floor(Date.now() / 1000),
      text: '', // optional
    };

    await producer.send({
      topic: topic,
      messages: [
        {
          key: key,
          value: JSON.stringify(message),
        },
      ],
    });

    console.log(`Sent file ${filePath} as base64 to topic ${topic} with key ${key}`);
  } catch (err) {
    console.error('Error sending message:', err);
  } finally {
    await producer.disconnect();
  }
}

// Usage example:
sendFileMessage('chat-messages', 'user123:chat456', './audio.ogg', 101);
