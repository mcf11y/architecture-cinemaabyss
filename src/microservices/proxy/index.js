const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');

const app = express();

const PORT = process.env.PORT || 8000;
const MONOLITH_URL = process.env.MONOLITH_URL || 'http://localhost:8080';
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL || 'http://localhost:8081';
const EVENTS_SERVICE_URL = process.env.EVENTS_SERVICE_URL || 'http://localhost:8082';
const GRADUAL_MIGRATION = process.env.GRADUAL_MIGRATION === 'true';
const MOVIES_MIGRATION_PERCENT = parseInt(process.env.MOVIES_MIGRATION_PERCENT || '0', 10);

app.get('/health', (req, res) => {
  res.send('Strangler Fig Proxy is healthy');
});

app.use('/api/events', createProxyMiddleware({
  target: EVENTS_SERVICE_URL,
  changeOrigin: true,
}));

app.use('/api/movies', (req, res, next) => {
  let target;

  if (GRADUAL_MIGRATION) {
    const roll = Math.random() * 100;
    if (roll < MOVIES_MIGRATION_PERCENT) {
      target = MOVIES_SERVICE_URL;
      console.log(`[PROXY] /api/movies -> movies-service (roll=${roll.toFixed(1)}, threshold=${MOVIES_MIGRATION_PERCENT})`);
    } else {
      target = MONOLITH_URL;
      console.log(`[PROXY] /api/movies -> monolith (roll=${roll.toFixed(1)}, threshold=${MOVIES_MIGRATION_PERCENT})`);
    }
  } else {
    target = MOVIES_SERVICE_URL;
    console.log('[PROXY] /api/movies -> movies-service (migration complete)');
  }

  const proxy = createProxyMiddleware({
    target,
    changeOrigin: true,
  });

  proxy(req, res, next);
});

app.use('/api', createProxyMiddleware({
  target: MONOLITH_URL,
  changeOrigin: true,
}));

app.listen(PORT, () => {
  console.log(`Strangler Fig Proxy listening on port ${PORT}`);
  console.log(`  MONOLITH_URL: ${MONOLITH_URL}`);
  console.log(`  MOVIES_SERVICE_URL: ${MOVIES_SERVICE_URL}`);
  console.log(`  EVENTS_SERVICE_URL: ${EVENTS_SERVICE_URL}`);
  console.log(`  GRADUAL_MIGRATION: ${GRADUAL_MIGRATION}`);
  console.log(`  MOVIES_MIGRATION_PERCENT: ${MOVIES_MIGRATION_PERCENT}`);
});
