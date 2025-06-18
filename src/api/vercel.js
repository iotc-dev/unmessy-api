// src/api/vercel.js
// Dedicated export file for Vercel that ensures clean ES module export

// Import the app setup but don't execute server-specific code
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import compression from 'compression';
import { config, validateConfig } from '../core/config.js';
import logger, { correlationIdMiddleware } from '../core/logger.js';
import { requestLogger } from '../api/middleware/request-logger.js';
import db from '../core/db.js';
import { errorHandler, notFoundHandler } from './middleware/error-handler.js';
import validateRoutes from './routes/validate.js';
import hubspotWebhookRoutes from './routes/hubspot-webhook.js';
import healthRoutes from './routes/health.js';
import adminRoutes from './routes/admin.js';
import cronRoutes from './routes/cron.js';

// Create Express app
const app = express();

// Validate configuration
try {
  validateConfig();
  logger.info('Configuration validated successfully');
} catch (error) {
  logger.error('Configuration validation failed', error);
}

// Apply global middleware
app.use(helmet({
  contentSecurityPolicy: config.security.helmet.contentSecurityPolicy
}));

app.use(cors({
  origin: config.security.corsOrigins,
  credentials: config.security.corsCredentials
}));

app.use(compression());
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

// IMPORTANT: Set up request ID first before any logging
app.use((req, res, next) => {
  req.id = req.headers['x-request-id'] || `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  res.setHeader('X-Request-ID', req.id);
  next();
});

// Then add correlation ID
app.use(correlationIdMiddleware);

// Now add request logging (after req.id is set)
app.use(requestLogger());

// Database connection middleware
app.use(async (req, res, next) => {
  if (req.path.includes('/api/health') && !req.path.includes('/detailed')) {
    return next();
  }
  
  try {
    await db.initialize();
    res.on('finish', async () => {
      if (Date.now() - db.getLastUsedTimestamp() > 10000) {
        await db.cleanup();
      }
    });
    next();
  } catch (error) {
    next(error);
  }
});

// Set up API routes
app.use('/api/validate', validateRoutes);

app.use('/api/hubspot', hubspotWebhookRoutes);
app.use('/api/health', healthRoutes);
app.use('/api/admin', adminRoutes);
app.use('/api/cron', cronRoutes);

// Root path handler
app.get('/', (req, res) => {
  res.status(200).json({
    service: 'Unmessy API',
    version: config.unmessy.version,
    status: 'online',
    environment: config.env
  });
});

// Apply error handling middleware
app.use(notFoundHandler);
app.use(errorHandler);

// Clean ES module export
export default app;