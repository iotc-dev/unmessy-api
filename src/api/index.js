// src/api/index.js
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import compression from 'compression';
import { createServer } from 'http';
import { config, validateConfig } from '../core/config.js';
import logger, { requestLogger, correlationIdMiddleware } from '../core/logger.js';
import db from '../core/db.js';
import { errorHandler, notFoundHandler, setupUncaughtErrorHandlers } from './middleware/error-handler.js';
import validateRoutes from './routes/validate.js';
import hubspotWebhookRoutes from './routes/hubspot-webhook.js';
import healthRoutes from './routes/health.js';
import adminRoutes from './routes/admin.js';
import cronRoutes from './routes/cron.js';

// Initialize uncaught error handlers
setupUncaughtErrorHandlers();

// Create Express app
const app = express();

// Validate configuration
try {
  validateConfig();
  logger.info('Configuration validated successfully');
} catch (error) {
  logger.error('Configuration validation failed', error);
  // Don't exit process in serverless environment
  if (!config.isVercel) {
    process.exit(1);
  }
}

// Initialize database - with connection optimization for serverless
async function initializeDatabase() {
  try {
    await db.initialize();
    logger.info('Database initialized successfully');
  } catch (error) {
    logger.error('Database initialization failed', error);
    // Don't exit process in serverless environment
    if (!config.isVercel) {
      process.exit(1);
    }
    throw error; // Re-throw for handling in request context
  }
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

// Raw body capture for HubSpot webhooks (MUST be before express.json())
app.use('/api/hubspot/webhook', (req, res, next) => {
  if (req.method !== 'POST') {
    return next();
  }
  
  let rawBody = '';
  req.setEncoding('utf8');
  
  req.on('data', chunk => {
    rawBody += chunk;
  });
  
  req.on('end', () => {
    req.rawBody = rawBody;
    
    // Parse the body manually
    try {
      req.body = JSON.parse(rawBody);
    } catch (e) {
      logger.error('Failed to parse webhook body', { error: e.message });
      // Let express.json() handle it
    }
    
    next();
  });
  
  req.on('error', (err) => {
    logger.error('Error reading webhook body', err);
    next(err);
  });
});

// Body parsing middleware (MUST come after raw body capture)
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

// Add request logging and correlation ID
app.use(correlationIdMiddleware);
app.use(requestLogger());

// Database connection middleware for serverless environment
if (config.isVercel) {
  app.use(async (req, res, next) => {
    // Skip for health checks to avoid unnecessary DB connections
    if (req.path.includes('/api/health') && !req.path.includes('/detailed')) {
      return next();
    }
    
    try {
      // Ensure DB is initialized for this request
      await db.initialize();
      // Add cleanup handler for when request is complete
      res.on('finish', async () => {
        // Don't immediately close connections - allow for connection reuse
        // within the serverless function instance lifetime
        if (Date.now() - db.getLastUsedTimestamp() > 10000) {
          await db.cleanup();
        }
      });
      next();
    } catch (error) {
      next(error);
    }
  });
}

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

// For traditional Node.js environments (non-Vercel)
if (!config.isVercel) {
  // Create HTTP server
  const server = createServer(app);
  const port = config.port;
  
  // Graceful shutdown handler
  function gracefulShutdown() {
    logger.info('Shutting down server...');
    
    server.close(async () => {
      logger.info('HTTP server closed');
      
      try {
        // Clean up database connections
        await db.cleanup();
        logger.info('Database connections closed');
        
        // Give time for final logs to be written
        setTimeout(() => {
          logger.info('Shutdown complete');
          process.exit(0);
        }, 1000);
      } catch (error) {
        logger.error('Error during shutdown', error);
        process.exit(1);
      }
    });
    
    // Force close if graceful shutdown takes too long
    setTimeout(() => {
      logger.error('Shutdown timeout exceeded, forcing exit');
      process.exit(1);
    }, 30000);
  }
  
  // Handle shutdown signals
  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);
  
  // Start the server
  async function startServer() {
    // Initialize database first
    await initializeDatabase();
    
    // Start HTTP server
    server.listen(port, () => {
      logger.info(`Unmessy API server running`, {
        port,
        environment: config.env,
        nodeEnv: process.env.NODE_ENV,
        region: config.isVercel ? config.vercelRegion : 'local',
        version: config.unmessy.version
      });
    });
    
    // Log startup message
    logger.info('Unmessy API startup complete', {
      validationServices: {
        email: config.unmessy.features.emailValidation,
        name: config.unmessy.features.nameValidation,
        phone: config.unmessy.features.phoneValidation,
        address: config.unmessy.features.addressValidation
      },
      externalServices: {
        zeroBounce: config.services.zeroBounce.enabled,
        openCage: config.services.openCage.enabled,
        hubspot: config.unmessy.features.hubspotIntegration
      },
      asyncProcessing: config.unmessy.features.asyncProcessing,
      cronEnabled: !!config.security.cronSecret
    });
  }
  
  // Detect if this is the main module (not imported)
  const isMainModule = import.meta.url === `file://${process.argv[1]}`;
  
  // Only start the server if this is the main module
  if (isMainModule) {
    startServer().catch(error => {
      logger.error('Failed to start server', error);
      process.exit(1);
    });
  }
  
  // Export for testing (traditional environment)
  export { app, server, startServer };
} else {
  // For Vercel serverless environment, just initialize once on cold start
  (async () => {
    try {
      logger.info('Initializing serverless instance');
      // Note: We don't await this in serverless to avoid blocking the first request
      // The middleware will ensure DB connection for each request
      initializeDatabase().catch(err => {
        logger.error('Background initialization error', err);
      });
      
      logger.info('Serverless instance ready', {
        environment: config.env,
        region: config.vercelRegion,
        version: config.unmessy.version,
        cronEnabled: !!config.security.cronSecret
      });
    } catch (error) {
      logger.error('Failed to initialize serverless instance', error);
    }
  })();
  
  // Export the Express app for Vercel serverless
  export default app;
}