// src/api/routes/health.js
const express = require('express');
const router = express.Router();
const { asyncHandler } = require('../middleware/error-handler');
const { authMiddleware } = require('../middleware/auth');
const db = require('../../core/db');
const config = require('../../core/config');
const { createServiceLogger } = require('../../core/logger');
const { zeroBounceService } = require('../../services/external/zerobounce');
const { openCageService } = require('../../services/external/opencage');
const clientService = require('../../services/client-service');

// Create logger instance
const logger = createServiceLogger('health-api');

/**
 * Basic health check endpoint
 * Returns 200 OK if the API is running
 */
router.get('/', asyncHandler(async (req, res) => {
  const response = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    version: config.unmessy.version,
    environment: {
      nodeEnv: config.env,
      isVercel: config.isVercel,
      vercelRegion: config.vercelRegion
    }
  };
  
  res.status(200).json(response);
}));

/**
 * Detailed health check endpoint
 * Checks database and external services
 * Requires authentication for detailed information
 */
router.get('/detailed', authMiddleware({ required: false }), asyncHandler(async (req, res) => {
  const startTime = Date.now();
  const isAuthenticated = !!req.clientId;
  
  // Basic information (always included)
  const response = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    version: config.unmessy.version,
    environment: {
      nodeEnv: config.env,
      isVercel: config.isVercel,
      vercelRegion: config.vercelRegion
    }
  };
  
  // Check database connection
  try {
    const dbStatus = await checkDatabaseStatus();
    response.database = dbStatus;
    
    // If database is not connected, mark overall status as degraded
    if (dbStatus.status !== 'connected') {
      response.status = 'degraded';
    }
  } catch (error) {
    logger.error('Health check database error', error);
    response.database = {
      status: 'error',
      message: error.message
    };
    response.status = 'degraded';
  }
  
  // Add external service status checks
  response.services = {};
  
  // Check ZeroBounce if enabled
  if (config.services.zeroBounce.enabled) {
    try {
      const zeroBounceStatus = await checkZeroBounceStatus();
      response.services.zeroBounce = zeroBounceStatus;
      
      if (zeroBounceStatus.status !== 'available' && config.services.zeroBounce.enabled) {
        response.status = 'degraded';
      }
    } catch (error) {
      logger.error('Health check ZeroBounce error', error);
      response.services.zeroBounce = {
        status: 'error',
        message: error.message
      };
      response.status = 'degraded';
    }
  } else {
    response.services.zeroBounce = {
      status: 'disabled',
      enabled: false
    };
  }
  
  // Check OpenCage if enabled
  if (config.services.openCage.enabled) {
    try {
      const openCageStatus = await checkOpenCageStatus();
      response.services.openCage = openCageStatus;
      
      if (openCageStatus.status !== 'available' && config.services.openCage.enabled) {
        response.status = 'degraded';
      }
    } catch (error) {
      logger.error('Health check OpenCage error', error);
      response.services.openCage = {
        status: 'error',
        message: error.message
      };
      response.status = 'degraded';
    }
  } else {
    response.services.openCage = {
      status: 'disabled',
      enabled: false
    };
  }
  
  // Include queue stats if authenticated
  if (isAuthenticated) {
    try {
      const queueStats = await getQueueStats();
      response.queue = queueStats;
      
      // If queue has a high pending count, mark as warning
      if (queueStats.pending > 50) {
        response.queue.status = 'warning';
        if (response.status === 'ok') {
          response.status = 'warning';
        }
      } else {
        response.queue.status = 'ok';
      }
    } catch (error) {
      logger.error('Health check queue error', error);
      response.queue = {
        status: 'error',
        message: error.message
      };
      // Only degrade if it's a critical error
      if (error.isCritical) {
        response.status = 'degraded';
      }
    }
    
    // Include client statistics if authenticated
    try {
      const clientStats = await getClientStats();
      response.clients = clientStats;
    } catch (error) {
      logger.error('Health check client stats error', error);
      response.clients = {
        status: 'error',
        message: error.message
      };
    }
  }
  
  // Add response time
  response.responseTime = `${Date.now() - startTime}ms`;
  
  res.status(200).json(response);
}));

/**
 * Readiness probe endpoint for Kubernetes/container orchestration
 * Checks if the service is ready to receive traffic
 */
router.get('/ready', asyncHandler(async (req, res) => {
  const isReady = await checkReadiness();
  
  if (isReady) {
    return res.status(200).json({ status: 'ready' });
  }
  
  return res.status(503).json({ status: 'not ready' });
}));

/**
 * Liveness probe endpoint for Kubernetes/container orchestration
 * Checks if the service is alive and should be restarted if not
 */
router.get('/live', (req, res) => {
  // Simple check that the process is running
  res.status(200).json({ status: 'alive' });
});

/**
 * Check database connection status
 * @returns {Object} Database status object
 */
async function checkDatabaseStatus() {
  try {
    const result = await db.testConnection();
    
    // Get database statistics
    const stats = await db.getStats();
    
    return {
      status: 'connected',
      lastCheck: new Date().toISOString(),
      pool: stats.pool,
      connections: stats.connections
    };
  } catch (error) {
    logger.error('Database connection test failed', error);
    throw error;
  }
}

/**
 * Check ZeroBounce service status
 * @returns {Object} ZeroBounce status object
 */
async function checkZeroBounceStatus() {
  if (!config.services.zeroBounce.apiKey) {
    return {
      status: 'not_configured',
      enabled: config.services.zeroBounce.enabled,
      message: 'API key not configured'
    };
  }
  
  try {
    const result = await zeroBounceService.checkCredits();
    
    return {
      status: 'available',
      enabled: config.services.zeroBounce.enabled,
      credits: result.credits,
      lastCheck: new Date().toISOString()
    };
  } catch (error) {
    logger.warn('ZeroBounce service check failed', error);
    
    return {
      status: 'unavailable',
      enabled: config.services.zeroBounce.enabled,
      message: error.message,
      lastCheck: new Date().toISOString()
    };
  }
}

/**
 * Check OpenCage service status
 * @returns {Object} OpenCage status object
 */
async function checkOpenCageStatus() {
  if (!config.services.openCage.apiKey) {
    return {
      status: 'not_configured',
      enabled: config.services.openCage.enabled,
      message: 'API key not configured'
    };
  }
  
  try {
    const result = await openCageService.checkStatus();
    
    return {
      status: 'available',
      enabled: config.services.openCage.enabled,
      rateLimit: result.rateLimit,
      lastCheck: new Date().toISOString()
    };
  } catch (error) {
    logger.warn('OpenCage service check failed', error);
    
    return {
      status: 'unavailable',
      enabled: config.services.openCage.enabled,
      message: error.message,
      lastCheck: new Date().toISOString()
    };
  }
}

/**
 * Get queue statistics
 * @returns {Object} Queue statistics object
 */
async function getQueueStats() {
  try {
    const stats = await db.query(`
      SELECT 
        status, 
        COUNT(*) as count 
      FROM 
        hubspot_webhook_queue 
      GROUP BY 
        status
    `);
    
    // Parse queue statistics
    const queueStats = {
      pending: 0,
      processing: 0,
      completed: 0,
      failed: 0,
      total: 0
    };
    
    stats.rows.forEach(row => {
      queueStats[row.status.toLowerCase()] = parseInt(row.count, 10);
      queueStats.total += parseInt(row.count, 10);
    });
    
    return queueStats;
  } catch (error) {
    logger.error('Failed to get queue statistics', error);
    throw error;
  }
}

/**
 * Get client statistics
 * @returns {Object} Client statistics object
 */
async function getClientStats() {
  try {
    const stats = await db.query(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN active = true THEN 1 ELSE 0 END) as active,
        SUM(CASE WHEN hubspot_enabled = true THEN 1 ELSE 0 END) as hubspot_enabled
      FROM 
        clients
    `);
    
    // Get clients with high usage
    const highUsageClients = await db.query(`
      SELECT 
        client_id,
        name,
        remaining_email,
        daily_email_limit,
        ROUND((daily_email_limit - remaining_email) * 100.0 / NULLIF(daily_email_limit, 0), 2) as usage_percent
      FROM 
        clients
      WHERE 
        active = true AND
        (daily_email_limit - remaining_email) * 100.0 / NULLIF(daily_email_limit, 0) > 80
      ORDER BY 
        usage_percent DESC
      LIMIT 5
    `);
    
    return {
      total: parseInt(stats.rows[0].total, 10),
      active: parseInt(stats.rows[0].active, 10),
      hubspotEnabled: parseInt(stats.rows[0].hubspot_enabled, 10),
      highUsage: highUsageClients.rows.map(client => ({
        clientId: client.client_id,
        name: client.name,
        usagePercent: parseFloat(client.usage_percent)
      }))
    };
  } catch (error) {
    logger.error('Failed to get client statistics', error);
    throw error;
  }
}

/**
 * Check if the service is ready to receive traffic
 * @returns {boolean} Whether the service is ready
 */
async function checkReadiness() {
  try {
    // Check database connection
    const dbConnected = await db.testConnection().then(() => true).catch(() => false);
    
    if (!dbConnected) {
      logger.warn('Readiness check failed: Database not connected');
      return false;
    }
    
    // Additional checks can be added here
    
    return true;
  } catch (error) {
    logger.error('Readiness check error', error);
    return false;
  }
}

module.exports = router;