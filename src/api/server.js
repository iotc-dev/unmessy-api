// src/api/server.js
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import { v4 as uuidv4 } from 'uuid';
import { config } from '../core/config.js';
import db from '../core/db.js';
import logger from '../core/logger.js';
import { errorHandler, notFoundHandler, setupUncaughtErrorHandlers } from './middleware/error-handler.js';
import { requestLogger } from './middleware/request-logger.js';

// Import routes
import validateRoutes from './routes/validate.js';
import hubspotWebhookRoutes from './routes/hubspot-webhook.js';
import healthRoutes from './routes/health.js';
import adminRoutes from './routes/admin.js';

// Initialize express app
const app = express();

// Set up uncaught error handlers
setupUncaughtErrorHandlers();

// Create service logger
const serverLogger = logger.createServiceLogger('server');

/**
 * Initialize the server with middleware and routes
 * @returns {Object} Express app instance
 */
function initializeServer() {
  serverLogger.info('Initializing server...');
  
  // Security middleware
  app.use(helmet({
    contentSecurityPolicy: config.security.helmet.contentSecurityPolicy,
  }));
  
  // Set up CORS
  app.use(cors({
    origin: config.security.corsOrigins,
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', config.clients.apiKeyHeader],
    credentials: config.security.corsCredentials
  }));
  
  // Parse JSON bodies
  app.use(express.json({ limit: '1mb' }));
  
  // Parse URL-encoded bodies
  app.use(express.urlencoded({ extended: true, limit: '1mb' }));
  
  // Request ID middleware
  app.use((req, res, next) => {
    req.id = req.headers['x-request-id'] || uuidv4();
    res.setHeader('X-Request-ID', req.id);
    next();
  });
  
  // Enable compression
  app.use(compression());
  
  // Request logging
  app.use(requestLogger());
  
  // Mount routes
  app.use('/api/validate', validateRoutes);
  app.use('/api/hubspot', hubspotWebhookRoutes);
  app.use('/api/health', healthRoutes);
  app.use('/api/admin', adminRoutes);
  
  // Root path handler
  app.get('/', (req, res) => {
    res.status(200).json({
      service: 'Unmessy API',
      version: config.unmessy.version,
      status: 'online',
      environment: config.env
    });
  });
  
  // 404 handler
  app.use(notFoundHandler);
  
  // Error handler middleware (must be last)
  app.use(errorHandler);
  
  return app;
}

/**
 * Connect to database and other services
 * @returns {Promise<void>}
 */
async function connectServices() {
  serverLogger.info('Connecting to services...');
  
  try {
    // Initialize database connection
    await db.initialize();
    serverLogger.info('Database connection initialized');
    
    // Validate configuration
    const configValid = config.validateConfig();
    if (!configValid && config.isProduction) {
      throw new Error('Invalid configuration in production environment');
    }
    
    serverLogger.info('Services connected successfully');
  } catch (error) {
    serverLogger.error('Error connecting to services', error);
    throw error;
  }
}

/**
 * Start the server
 * @returns {Promise<http.Server>}
 */
async function startServer() {
  try {
    // Connect to services
    await connectServices();
    
    // Initialize server
    const server = initializeServer();
    
    // Determine port
    const port = process.env.PORT || config.port;
    
    // Start listening
    return new Promise((resolve) => {
      const httpServer = server.listen(port, () => {
        serverLogger.info(`Server started on port ${port}`);
        serverLogger.info(`Environment: ${config.env}`);
        serverLogger.info(`API Version: ${config.unmessy.version}`);
        
        // Log Vercel-specific info if applicable
        if (config.isVercel) {
          serverLogger.info(`Vercel Environment: ${config.vercelEnv}`);
          serverLogger.info(`Vercel Region: ${config.vercelRegion}`);
        }
        
        resolve(httpServer);
      });
      
      // Handle graceful shutdown
      setupGracefulShutdown(httpServer);
    });
  } catch (error) {
    serverLogger.error('Failed to start server', error);
    process.exit(1);
  }
}

/**
 * Set up graceful shutdown handlers
 * @param {http.Server} server - HTTP server instance
 */
function setupGracefulShutdown(server) {
  // Handle termination signals
  const shutdownHandler = async (signal) => {
    serverLogger.info(`${signal} received, shutting down gracefully...`);
    
    // Stop accepting new connections
    server.close(async () => {
      serverLogger.info('HTTP server closed');
      
      try {
        // Clean up database connections
        await db.cleanup();
        serverLogger.info('Database connections closed');
        
        // Exit process
        serverLogger.info('Shutdown complete');
        process.exit(0);
      } catch (error) {
        serverLogger.error('Error during shutdown', error);
        process.exit(1);
      }
    });
    
    // Force exit after timeout
    setTimeout(() => {
      serverLogger.error('Shutdown timed out, forcing exit');
      process.exit(1);
    }, 10000); // 10 seconds
  };
  
  // Register signal handlers
  process.on('SIGTERM', () => shutdownHandler('SIGTERM'));
  process.on('SIGINT', () => shutdownHandler('SIGINT'));
}

// For testing exports
export {
  initializeServer,
  connectServices,
  startServer
};

// Auto-start server if this file is run directly
// Check if this module is the main module
if (import.meta.url === `file://${process.argv[1]}`) {
  startServer();
}