const express = require('express');
const axios = require('axios');
const { Kafka, logLevel } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const multer = require('multer'); // To handle file uploads
const FormData = require('form-data'); // Import FormData to handle file uploads in multipart/form-data
const fs = require('fs'); // Import fs for file handling

const app = express();
app.use(express.json());

// Configure multer for file upload
const upload = multer({ dest: 'uploads/' });

const AI_SERVER_URL = 'http://localhost:9000/chat';
const PYTHON_API = 'http://localhost:1011';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  logLevel: logLevel.ERROR
});

const admin = kafka.admin();
const producer = kafka.producer();

let isProducerConnected = false;

async function ensureProducerConnected() {
  if (!isProducerConnected) {
    await producer.connect();
    isProducerConnected = true;
  }
}

async function ensureTopicExists(username) {
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(username)) {
    await admin.createTopics({
      topics: [{ topic: username, numPartitions: 1, replicationFactor: 1 }]
    });
  }
  await admin.disconnect();
}


  

app.post('/request-ai', async (req, res) => {
  const { username, prompt, modelNumber, body } = req.body;
  if (!username || !prompt || modelNumber === undefined) {
    return res.status(400).json({ error: 'Missing username, prompt or modelNumber' });
  }

  try {
    await ensureTopicExists(username);
    const history = await getUserHistory(username);

    const messagesForAI = [
      ...history,
    ];

    await ensureProducerConnected();

    const aiResponse = await axios.post(AI_SERVER_URL, {
      prompt,
      modelNumber,
      username,
      //history: messagesForAI // send full context
      history: false,
      body,
    });

    await producer.send({
      topic: username,
      messages: [
        { value: JSON.stringify({ role: 'user', content: prompt }) },
        { value: JSON.stringify({role: 'assistant', content: aiResponse.data.iaAnswer}) }
      ]
    });

    res.json(aiResponse.data);
  } catch (err) {
    console.error('❌ Error handling request:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


// 📌 Unified /otherIA endpoint to handle both strings and files
app.post('/otherIA', upload.single('file'), async (req, res) => {
  let { modelNumber } = req.body;
  let prompt = req.body.prompt || req.file;
  // Ensure modelNumber is parsed as an integer
  modelNumber = parseInt(modelNumber, 10);

  // Basic validation
  if (!prompt || isNaN(modelNumber)) {
    return res.status(400).json({ error: 'Missing or invalid prompt or modelNumber' });
  }

  // Reject Ollama-reserved models (1–100)
  if (modelNumber <= 100) {
    return res.status(403).json({ error: 'Model number reserved for Ollama. Not supported in this route.' });
  }

  const requestId = uuidv4(); // Generate a unique requestId using UUID
  let pythonApiEndpoint;
  const form = new FormData(); // Initialize a FormData object to handle both string and file data

  // Check if the prompt is a file or a string
  if (req.file) {
    form.append('file', fs.createReadStream(req.file.path), { filename: req.file.originalname });
  } else if (typeof prompt === 'string') {
    prompt = prompt.trim();
  } else {
    return res.status(400).json({ error: 'Invalid prompt type' });
  }

  // Determine which Python API endpoint to call based on the model number
  switch (modelNumber) {
    case 101:
      pythonApiEndpoint = '/analyze_full_pipeline';
      if (req.file) {
        form.append('audio', fs.createReadStream(req.file.path), { filename: req.file.originalname });
      } else {
        return res.status(400).json({ error: 'Model 101 requires a file input' });
      }
      break;
    case 102:
      pythonApiEndpoint = '/analyze_speech_to_text';
      if (req.file) {
        form.append('audio', fs.createReadStream(req.file.path), { filename: req.file.originalname });
      } else {
        return res.status(400).json({ error: 'Model 102 requires a file input' });
      }
      break;
    case 103:
      pythonApiEndpoint = '/analyze_sentiment';
      if (typeof prompt === 'string') {
        form.append('sentiment', prompt);
      } else {
        return res.status(400).json({ error: 'Model 103 requires a string input' });
      }
      break;
    case 104:
      pythonApiEndpoint = '/analyze_summary';
      if (typeof prompt === 'string') {
        form.append('summury', prompt);
      } else {
        return res.status(400).json({ error: 'Model 104 requires a string input' });
      }
      break;
    case 105:
      pythonApiEndpoint = '/analyze_translation';
      if (typeof prompt === 'string') {
        form.append('translate', prompt);
      } else {
        return res.status(400).json({ error: 'Model 105 requires a string input' });
      }
      break;
    case 106:
      pythonApiEndpoint = '/analyze_cover_topic';
      if (typeof prompt === 'string') {
        form.append('topic', prompt);
      } else {
        return res.status(400).json({ error: 'Model 106 requires a string input' });
      }
      break;
    case 107:
      pythonApiEndpoint = '/analyze_speech_to_text_summary_sentiment';
      if (req.file) {
        form.append('audio', fs.createReadStream(req.file.path), { filename: req.file.originalname });
      } else {
        return res.status(400).json({ error: 'Model 107 requires a file input' });
      }
      break;
    default:
      return res.status(400).json({ error: 'Invalid model number' });
  }

  // Send the request to the Python API
  try {
    const response = await axios.post(`${PYTHON_API}${pythonApiEndpoint}`, form, {
      headers: form.getHeaders(), // Automatically set the appropriate headers for multipart data
    });

    // Respond with the Python API result
    res.json({ aiResponse: response.data });
    // Now add the AI response to the IA_Pipeline Kafka topic
    await ensureProducerConnected();

    await producer.send({
      topic: 'IA_Pipeline',
      messages: [
        {
          value: JSON.stringify({
            modelNumber,
            prompt,
            aiResponse: response.data,
            requestId,  // Optionally include requestId for tracking
          })
        }
      ]
    });
  } catch (error) {
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// 📌 Start server on Port 2021 (Frontend communication)
app.listen(2020, () => console.log('✅ Main Server listening on port 2020'));
