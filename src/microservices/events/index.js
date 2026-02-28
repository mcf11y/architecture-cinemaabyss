const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8082;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

// Separate Kafka clients for producer and consumer to avoid lock contention
const producerKafka = new Kafka({
  clientId: 'events-producer',
  brokers: KAFKA_BROKERS,
  requestTimeout: 10000,
  retry: { initialRetryTime: 1000, retries: 5 },
});

const consumerKafka = new Kafka({
  clientId: 'events-consumer',
  brokers: KAFKA_BROKERS,
  requestTimeout: 15000,
  retry: { initialRetryTime: 1000, retries: 5 },
});

const producer = producerKafka.producer();
const consumer = consumerKafka.consumer({
  groupId: 'events-service-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

const TOPICS = ['movie-events', 'user-events', 'payment-events'];

let producerReady = false;

app.get('/api/events/health', (req, res) => {
  res.json({ status: true });
});

async function publishEvent(topic, event, res) {
  if (!producerReady) {
    return res.status(503).json({ error: 'Producer not connected to Kafka' });
  }

  try {
    const result = await producer.send({
      topic,
      messages: [{ key: event.id, value: JSON.stringify(event) }],
      timeout: 10000,
    });

    const recordMetadata = result[0];
    console.log(`[PRODUCER] Sent event to ${topic}: ${JSON.stringify(event)}`);

    res.status(201).json({
      status: 'success',
      partition: recordMetadata.partition,
      offset: parseInt(recordMetadata.baseOffset, 10),
      event,
    });
  } catch (err) {
    console.error(`[PRODUCER] Error sending to ${topic}:`, err.message);
    res.status(500).json({ error: err.message });
  }
}

app.post('/api/events/movie', async (req, res) => {
  const { movie_id, title, action, user_id, rating, genres, description } = req.body;

  if (!movie_id || !title || !action) {
    return res.status(400).json({ error: 'movie_id, title, and action are required' });
  }

  const event = {
    id: `movie-${movie_id}-${action}`,
    type: 'movie',
    timestamp: new Date().toISOString(),
    payload: { movie_id, title, action, user_id, rating, genres, description },
  };

  await publishEvent('movie-events', event, res);
});

app.post('/api/events/user', async (req, res) => {
  const { user_id, username, email, action, timestamp } = req.body;

  if (!user_id || !action || !timestamp) {
    return res.status(400).json({ error: 'user_id, action, and timestamp are required' });
  }

  const event = {
    id: `user-${user_id}-${action}`,
    type: 'user',
    timestamp,
    payload: { user_id, username, email, action },
  };

  await publishEvent('user-events', event, res);
});

app.post('/api/events/payment', async (req, res) => {
  const { payment_id, user_id, amount, status, timestamp, method_type } = req.body;

  if (!payment_id || !user_id || amount === undefined || !status || !timestamp) {
    return res.status(400).json({ error: 'payment_id, user_id, amount, status, and timestamp are required' });
  }

  const event = {
    id: `payment-${payment_id}-${status}`,
    type: 'payment',
    timestamp,
    payload: { payment_id, user_id, amount, status, method_type },
  };

  await publishEvent('payment-events', event, res);
});

async function startConsumer() {
  try {
    await consumer.connect();
    console.log('[CONSUMER] Connected to Kafka');

    await consumer.subscribe({ topics: TOPICS, fromBeginning: false });
    console.log(`[CONSUMER] Subscribed to topics: ${TOPICS.join(', ')}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        console.log(`[CONSUMER] Received event from topic: ${topic}, partition: ${partition}, offset: ${message.offset}`);
        console.log(`[CONSUMER] Event data: ${value}`);
      },
    });
  } catch (err) {
    console.error('[CONSUMER] Error:', err.message);
    setTimeout(startConsumer, 5000);
  }
}

async function connectProducer() {
  for (let attempt = 1; attempt <= 20; attempt++) {
    try {
      await producer.connect();
      producerReady = true;
      console.log('[PRODUCER] Connected to Kafka');
      return;
    } catch (err) {
      console.error(`[PRODUCER] Connection attempt ${attempt} failed:`, err.message);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
  console.error('[PRODUCER] Could not connect to Kafka after 20 attempts');
}

async function start() {
  // Start HTTP server immediately so health checks work
  app.listen(PORT, () => {
    console.log(`Events service listening on port ${PORT}`);
    console.log(`  KAFKA_BROKERS: ${KAFKA_BROKERS.join(', ')}`);
  });

  await connectProducer();
  // Consumer runs independently — its crashes won't affect the producer
  startConsumer();
}

start();
