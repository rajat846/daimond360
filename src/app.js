require("node:dns/promises").setServers(["1.1.1.1", "8.8.8.8"]);
'use strict';

require('dotenv').config();

const express   = require('express');
const morgan    = require('morgan');
const mongoose  = require('mongoose');
const path      = require('path');
const fs        = require('fs');

const routes = require('./routes/index');
const logger = require('./utils/logger');

/* ──────────────────────────────────────────────────────────────
   Ensure required directories exist
────────────────────────────────────────────────────────────── */
['uploads', 'logs'].forEach((dir) => {
  const full = path.join(process.cwd(), dir);
  if (!fs.existsSync(full)) fs.mkdirSync(full, { recursive: true });
});

/* ──────────────────────────────────────────────────────────────
   Express app
────────────────────────────────────────────────────────────── */
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// HTTP request logging (skip in test)
if (process.env.NODE_ENV !== 'test') {
  app.use(morgan('dev'));
}

// Routes
app.use('/', routes);

// 404 handler
app.use((_req, res) => {
  res.status(404).json({ success: false, error: 'Route not found' });
});

// Global error handler
app.use((err, _req, res, _next) => {
  logger.error(`Unhandled error: ${err.message}`);
  res.status(err.status || 500).json({
    success: false,
    error: err.message || 'Internal server error',
  });
});

/* ──────────────────────────────────────────────────────────────
   MongoDB + server startup
────────────────────────────────────────────────────────────── */
const PORT       = parseInt(process.env.PORT, 10) || 3000;
const MONGO_URI  = process.env.MONGODB_URI || 'mongodb://localhost:27017/diamond360';

async function start() {
  try {
    await mongoose.connect(MONGO_URI);
    logger.info(`MongoDB connected: ${MONGO_URI}`);

    app.listen(PORT, () => {
      logger.info(`🚀 Diamond 360 API running on http://localhost:${PORT}`);
    });
  } catch (err) {
    logger.error(`Failed to start server: ${err.message}`);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received — shutting down gracefully');
  await mongoose.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received — shutting down gracefully');
  await mongoose.disconnect();
  process.exit(0);
});

start();

module.exports = app; // export for testing
