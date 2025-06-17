// src/api/routes/admin.js
const express = require('express');
const router = express.Router();
const { asyncHandler } = require('../middleware/error-handler');
const { authMiddleware } = require('../middleware/auth');
const { validate } = require('../middleware/validate-input');
const rateLimit = require('../middleware/rate-limit');
const clientService = require('../../services/client-service');
const queueService = require('../../services/queue-service');
const db = require('../../core/db');
const { NotFoundError, AuthorizationError } = require('../../core/errors');

// Apply authentication to all admin routes
// Require admin privileges
router.use(authMiddleware({ adminOnly: true }));

// Apply rate limiting to prevent abuse
router.use(rateLimit.ip({ 
  ipLimit: 60,          // 60 requests per minute
  ipWindowMs: 60 * 1000 // 1 minute window
}));

/**
 * GET /api/admin/stats
 * Retrieve system-wide statistics
 */
router.get('/stats', asyncHandler(async (req, res) => {
  const stats = await getSystemStats();
  res.json(stats);
}));

/**
 * GET /api/admin/clients
 * List all clients with pagination
 */
router.get('/clients', 
  validate.pagination(),
  asyncHandler(async (req, res) => {
    const { page, limit } = req.query;
    const clients = await clientService.listClients(page, limit);
    res.json(clients);
  })
);

/**
 * GET /api/admin/clients/:id
 * Get a specific client by ID
 */
router.get('/clients/:id',
  validate.id(),
  asyncHandler(async (req, res) => {
    const client = await clientService.getClient(req.params.id);
    
    if (!client) {
      throw new NotFoundError('Client', req.params.id);
    }
    
    res.json(client);
  })
);

/**
 * PUT /api/admin/clients/:id
 * Update a client
 */
router.put('/clients/:id',
  validate.id(),
  validate.custom({
    body: clientSchema
  }),
  asyncHandler(async (req, res) => {
    const updatedClient = await clientService.updateClient(req.params.id, req.body);
    
    if (!updatedClient) {
      throw new NotFoundError('Client', req.params.id);
    }
    
    res.json(updatedClient);
  })
);

/**
 * POST /api/admin/clients
 * Create a new client
 */
router.post('/clients',
  validate.custom({
    body: clientSchema
  }),
  asyncHandler(async (req, res) => {
    const newClient = await clientService.createClient(req.body);
    res.status(201).json(newClient);
  })
);

/**
 * DELETE /api/admin/clients/:id
 * Deactivate a client (soft delete)
 */
router.delete('/clients/:id',
  validate.id(),
  asyncHandler(async (req, res) => {
    // Prevent self-deactivation
    if (req.params.id === req.clientId) {
      throw new AuthorizationError('Cannot deactivate your own account');
    }
    
    const result = await clientService.deactivateClient(req.params.id);
    
    if (!result) {
      throw new NotFoundError('Client', req.params.id);
    }
    
    res.json({ success: true, message: 'Client deactivated successfully' });
  })
);

/**
 * GET /api/admin/queue/status
 * Get current queue status
 */
router.get('/queue/status', asyncHandler(async (req, res) => {
  const status = await queueService.getQueueStatus();
  res.json(status);
}));

/**
 * POST /api/admin/queue/process
 * Manually trigger queue processing
 */
router.post('/queue/process',
  validate.custom({
    body: Joi.object({
      limit: Joi.number().integer().min(1).max(50).default(10)
    })
  }),
  asyncHandler(async (req, res) => {
    const result = await queueService.processQueue(req.body.limit);
    res.json(result);
  })
);

/**
 * GET /api/admin/queue/failed
 * Get failed queue items with pagination
 */
router.get('/queue/failed',
  validate.pagination(),
  asyncHandler(async (req, res) => {
    const { page, limit } = req.query;
    const failedItems = await queueService.getFailedItems(page, limit);
    res.json(failedItems);
  })
);

/**
 * POST /api/admin/queue/retry/:id
 * Retry a failed queue item
 */
router.post('/queue/retry/:id',
  validate.id(),
  asyncHandler(async (req, res) => {
    const result = await queueService.retryItem(req.params.id);
    
    if (!result) {
      throw new NotFoundError('Queue item', req.params.id);
    }
    
    res.json({ success: true, message: 'Item queued for retry' });
  })
);

/**
 * GET /api/admin/metrics
 * Get system metrics
 */
router.get('/metrics',
  validate.custom({
    query: Joi.object({
      period: Joi.string().valid('hour', 'day', 'week', 'month').default('day'),
      type: Joi.string().valid('all', 'email', 'name', 'phone', 'address').default('all')
    })
  }),
  asyncHandler(async (req, res) => {
    const { period, type } = req.query;
    const metrics = await getMetrics(period, type);
    res.json(metrics);
  })
);

/**
 * GET /api/admin/db/status
 * Get database status
 */
router.get('/db/status', asyncHandler(async (req, res) => {
  const dbStatus = await db.getStats();
  res.json(dbStatus);
}));

/**
 * POST /api/admin/reset-rate-limits
 * Manually reset rate limits for a client
 */
router.post('/reset-rate-limits/:id',
  validate.id(),
  asyncHandler(async (req, res) => {
    const result = await clientService.resetRateLimits(req.params.id);
    
    if (!result) {
      throw new NotFoundError('Client', req.params.id);
    }
    
    res.json({ success: true, message: 'Rate limits reset successfully' });
  })
);

// Utility functions

/**
 * Get system-wide statistics
 */
async function getSystemStats() {
  const [
    clientStats,
    validationStats,
    queueStats,
    dbStats
  ] = await Promise.all([
    clientService.getClientStats(),
    getValidationStats(),
    queueService.getQueueStatus(),
    db.getStats()
  ]);
  
  return {
    timestamp: new Date().toISOString(),
    clients: clientStats,
    validations: validationStats,
    queue: queueStats,
    database: dbStats
  };
}

/**
 * Get validation statistics
 */
async function getValidationStats() {
  // Query the validation_metrics table for aggregate stats
  const result = await db.query(`
    SELECT 
      validation_type,
      SUM(total_requests) as total_requests,
      SUM(successful_validations) as successful,
      SUM(failed_validations) as failed,
      ROUND(AVG(avg_response_time_ms), 2) as avg_response_time_ms
    FROM validation_metrics
    WHERE date = CURRENT_DATE
    GROUP BY validation_type
  `);
  
  // Format the results
  const statsByType = {};
  let totalRequests = 0;
  
  result.rows.forEach(row => {
    statsByType[row.validation_type] = {
      requests: parseInt(row.total_requests, 10),
      successful: parseInt(row.successful, 10),
      failed: parseInt(row.failed, 10),
      avgResponseTime: parseFloat(row.avg_response_time_ms)
    };
    
    totalRequests += parseInt(row.total_requests, 10);
  });
  
  return {
    total: totalRequests,
    byType: statsByType,
    period: 'today'
  };
}

/**
 * Get metrics data
 */
async function getMetrics(period, type) {
  // Build time range for the query
  let timeCondition;
  switch (period) {
    case 'hour':
      timeCondition = `date = CURRENT_DATE AND hour = EXTRACT(HOUR FROM CURRENT_TIMESTAMP)`;
      break;
    case 'day':
      timeCondition = `date = CURRENT_DATE`;
      break;
    case 'week':
      timeCondition = `date >= CURRENT_DATE - INTERVAL '7 days'`;
      break;
    case 'month':
      timeCondition = `date >= CURRENT_DATE - INTERVAL '30 days'`;
      break;
    default:
      timeCondition = `date = CURRENT_DATE`;
  }
  
  // Build type condition
  const typeCondition = type === 'all' ? '' : `AND validation_type = '${type}'`;
  
  // Query for metrics
  const query = `
    SELECT 
      validation_type,
      date,
      hour,
      SUM(total_requests) as total_requests,
      SUM(successful_validations) as successful_validations,
      SUM(failed_validations) as failed_validations,
      ROUND(AVG(avg_response_time_ms), 2) as avg_response_time_ms,
      SUM(timeout_errors) as timeout_errors,
      SUM(external_api_errors) as external_api_errors
    FROM validation_metrics
    WHERE ${timeCondition} ${typeCondition}
    GROUP BY validation_type, date, hour
    ORDER BY date DESC, hour DESC
  `;
  
  const result = await db.query(query);
  
  // Format results based on period
  let formattedResults;
  
  if (period === 'hour') {
    // Hourly breakdown
    formattedResults = result.rows.map(row => ({
      type: row.validation_type,
      date: row.date,
      hour: row.hour,
      requests: parseInt(row.total_requests, 10),
      successful: parseInt(row.successful_validations, 10),
      failed: parseInt(row.failed_validations, 10),
      avgResponseTime: parseFloat(row.avg_response_time_ms),
      errors: {
        timeout: parseInt(row.timeout_errors, 10),
        externalApi: parseInt(row.external_api_errors, 10)
      }
    }));
  } else {
    // Daily aggregation
    const dailyData = {};
    
    result.rows.forEach(row => {
      const dateKey = row.date.toISOString().split('T')[0];
      
      if (!dailyData[dateKey]) {
        dailyData[dateKey] = {
          date: dateKey,
          types: {}
        };
      }
      
      if (!dailyData[dateKey].types[row.validation_type]) {
        dailyData[dateKey].types[row.validation_type] = {
          requests: 0,
          successful: 0,
          failed: 0,
          avgResponseTime: 0,
          hours: 0,
          errors: {
            timeout: 0,
            externalApi: 0
          }
        };
      }
      
      const typeData = dailyData[dateKey].types[row.validation_type];
      typeData.requests += parseInt(row.total_requests, 10);
      typeData.successful += parseInt(row.successful_validations, 10);
      typeData.failed += parseInt(row.failed_validations, 10);
      typeData.avgResponseTime = (typeData.avgResponseTime * typeData.hours + parseFloat(row.avg_response_time_ms)) / (typeData.hours + 1);
      typeData.hours += 1;
      typeData.errors.timeout += parseInt(row.timeout_errors, 10);
      typeData.errors.externalApi += parseInt(row.external_api_errors, 10);
    });
    
    formattedResults = Object.values(dailyData);
  }
  
  return {
    period,
    type,
    data: formattedResults
  };
}

// Client schema for validation
const clientSchema = Joi.object({
  name: Joi.string().required(),
  active: Joi.boolean().default(true),
  daily_email_limit: Joi.number().integer().min(1).default(10000),
  daily_name_limit: Joi.number().integer().min(1).default(10000),
  daily_phone_limit: Joi.number().integer().min(1).default(10000),
  daily_address_limit: Joi.number().integer().min(1).default(10000),
  hubspot_enabled: Joi.boolean().default(false),
  hubspot_private_key: Joi.string().allow('', null),
  hubspot_portal_id: Joi.string().allow('', null),
  hubspot_form_guid: Joi.string().allow('', null),
  hubspot_webhook_secret: Joi.string().allow('', null),
  is_admin: Joi.boolean().default(false)
});

module.exports = router;