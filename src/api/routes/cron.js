// src/api/routes/cron.js
import express from 'express';
import { asyncHandler } from '../middleware/error-handler.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import clientService from '../../services/client-service.js';
import db from '../../core/db.js';
import { triggerAlert, ALERT_TYPES } from '../../monitoring/alerts.js';

// Create logger instance
const logger = createServiceLogger('cron-api');

const router = express.Router();

/**
 * Validate cron job security token
 * @param {Object} req - Express request object
 * @returns {boolean} Whether the token is valid
 */
function validateCronToken(req) {
  const token = req.headers['x-cron-token'] || req.query.token;
  return token === config.security.cronSecret;
}

/**
 * Validate IP address if IP allowlist is configured
 * @param {Object} req - Express request object
 * @returns {boolean} Whether the IP is allowed
 */
function validateIPAddress(req) {
  // If no allowlist is configured, allow all IPs
  if (!config.security.cronAllowedIPs || config.security.cronAllowedIPs.length === 0) {
    return true;
  }
  
  const clientIP = req.ip || req.connection.remoteAddress;
  return config.security.cronAllowedIPs.includes(clientIP);
}

/**
 * Middleware to secure cron endpoints
 */
function secureCronEndpoint(req, res, next) {
  // Check token
  if (!validateCronToken(req)) {
    logger.warn('Unauthorized cron job access attempt - invalid token', {
      ip: req.ip,
      path: req.path
    });
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Invalid or missing cron security token'
    });
  }
  
  // Check IP address if configured
  if (!validateIPAddress(req)) {
    logger.warn('Unauthorized cron job access attempt - IP not allowed', {
      ip: req.ip,
      path: req.path
    });
    return res.status(403).json({
      error: 'Forbidden',
      message: 'IP address not allowed'
    });
  }
  
  // If we get here, authorization is successful
  next();
}

/**
 * GET /api/cron/queue-processor
 * Comprehensive queue processor that handles multiple queue operations
 */
router.get('/queue-processor', 
  secureCronEndpoint,
  asyncHandler(async (req, res) => {
    const operations = req.query.ops ? req.query.ops.split(',') : ['process'];
    const results = {};
    const limit = parseInt(req.query.limit) || config.queue.batchSize || 25;
    
    try {
      logger.info('Starting scheduled queue operations', { operations });
      
      // Import the queue service
      const queueService = (await import('../../services/queue-service.js')).default;
      
      // Run requested operations
      for (const op of operations) {
        switch (op) {
          case 'process':
            // Process pending items
            results.process = await queueService.processPendingItems({
              batchSize: limit
            });
            break;
            
          case 'monitor':
            // Check queue status with alerts
            results.monitor = await queueService.checkQueueStatus();
            break;
            
          case 'reset-stalled':
            // Reset stalled items
            results.resetStalled = await queueService.resetStalledItems();
            break;
            
          case 'cleanup':
            // Clean up old completed items
            results.cleanup = await queueService.cleanupCompletedItems();
            break;
            
          default:
            logger.warn(`Unknown queue operation: ${op}`);
        }
      }
      
      logger.info('Scheduled queue operations completed', {
        operations,
        processed: results.process?.processed || 0,
        failed: results.process?.failed || 0
      });
      
      return res.json({
        success: true,
        operations,
        results,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      logger.error('Error in scheduled queue operations', error);
      
      // Send alert
      triggerAlert(ALERT_TYPES.APPLICATION.QUEUE_PROCESSING_ERROR, {
        error: error.message,
        operations
      });
      
      throw error;
    }
  })
);

/**
 * GET /api/cron/reset-limits
 * Reset client rate limits at midnight
 */
router.get('/reset-limits',
  secureCronEndpoint,
  asyncHandler(async (req, res) => {
    try {
      logger.info('Starting scheduled rate limit reset');
      
      const result = await clientService.resetAllRateLimits();
      
      logger.info('Rate limit reset completed', {
        clientsReset: result.count
      });
      
      return res.json({
        success: true,
        clientsReset: result.count,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      logger.error('Error in scheduled rate limit reset', error);
      
      // Send alert
      triggerAlert(ALERT_TYPES.APPLICATION.RATE_LIMIT_ERROR, {
        error: error.message
      });
      
      throw error;
    }
  })
);

/**
 * GET /api/cron/clean-old-logs
 * Clean up old log entries and metrics
 */
router.get('/clean-old-logs',
  secureCronEndpoint,
  asyncHandler(async (req, res) => {
    try {
      logger.info('Starting scheduled log cleanup');
      
      const daysToKeep = parseInt(req.query.days) || 30;
      
      // Clean up old API request logs
      const apiLogsResult = await db.query(
        `DELETE FROM api_request_logs
         WHERE created_at < NOW() - INTERVAL $1
         RETURNING id`,
        [`${daysToKeep} days`]
      );
      const apiLogsDeleted = apiLogsResult.rowCount || 0;
      
      // Clean up old validation metrics
      const metricsResult = await db.query(
        `DELETE FROM validation_metrics
         WHERE date < CURRENT_DATE - INTERVAL $1
         RETURNING id`,
        [`${daysToKeep} days`]
      );
      const metricsDeleted = metricsResult.rowCount || 0;
      
      const result = {
        apiLogsDeleted,
        metricsDeleted,
        daysKept: daysToKeep
      };
      
      logger.info('Log cleanup completed', result);
      
      return res.json({
        success: true,
        ...result,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      logger.error('Error in scheduled log cleanup', error);
      throw error;
    }
  })
);

/**
 * GET /api/cron/ping
 * Simple endpoint to verify cron configuration
 */
router.get('/ping', 
  secureCronEndpoint,
  (req, res) => {
    logger.info('Cron ping received', {
      ip: req.ip,
      userAgent: req.headers['user-agent']
    });
    
    res.json({
      success: true,
      message: 'Cron configuration is valid',
      timestamp: new Date().toISOString(),
      environment: config.env
    });
  }
);

export default router;