// src/api/routes/admin.js
import express from 'express';
import Joi from 'joi';
import { asyncHandler } from '../middleware/error-handler.js';
import { authMiddleware } from '../middleware/auth.js';
import { validate } from '../middleware/validate-input.js';
import rateLimit from '../middleware/rate-limit.js';
import clientService from '../../services/client-service.js';
import queueService from '../../services/queue-service.js';
import db from '../../core/db.js';
import { NotFoundError, AuthorizationError } from '../../core/errors.js';

const router = express.Router();

// Client schema for validation - MOVED TO TOP
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
    clients: clientStats,
    validations: validationStats,
    queue: queueStats,
    database: dbStats,
    timestamp: new Date().toISOString()
  };
}

/**
 * Get validation statistics
 */
async function getValidationStats() {
  try {
    const { rows } = await db.query(`
      SELECT 
        SUM(email_count) as total_emails,
        SUM(name_count) as total_names,
        SUM(phone_count) as total_phones,
        SUM(address_count) as total_addresses
      FROM clients
    `);
    
    return rows[0];
  } catch (error) {
    return {
      total_emails: 0,
      total_names: 0,
      total_phones: 0,
      total_addresses: 0
    };
  }
}

/**
 * Get metrics for a specific period and type
 */
async function getMetrics(period, type) {
  let timeRange;
  let groupBy;
  
  switch (period) {
    case 'hour':
      timeRange = "date >= NOW() - INTERVAL '1 hour'";
      groupBy = 'minute';
      break;
    case 'week':
      timeRange = "date >= NOW() - INTERVAL '7 days'";
      groupBy = 'day';
      break;
    case 'month':
      timeRange = "date >= NOW() - INTERVAL '30 days'";
      groupBy = 'day';
      break;
    default: // day
      timeRange = "date >= NOW() - INTERVAL '24 hours'";
      groupBy = 'hour';
  }
  
  const typeFilter = type === 'all' ? '' : `AND validation_type = '${type}'`;
  
  const query = `
    SELECT 
      DATE_TRUNC('${groupBy}', date) as period,
      validation_type,
      SUM(total_requests) as requests,
      SUM(successful_validations) as successes,
      SUM(failed_validations) as failures,
      AVG(avg_response_time_ms) as avg_response_time_ms,
      SUM(timeout_errors) as timeout_errors,
      SUM(external_api_errors) as external_api_errors
    FROM validation_metrics
    WHERE ${timeRange} ${typeFilter}
    GROUP BY period, validation_type
    ORDER BY period DESC
  `;
  
  const { rows } = await db.query(query);
  
  // Format results
  const formattedResults = [];
  
  if (type === 'all') {
    // Group by period
    const periodData = {};
    
    rows.forEach(row => {
      const periodKey = row.period.toISOString();
      if (!periodData[periodKey]) {
        periodData[periodKey] = {
          period: periodKey,
          email: { requests: 0, successes: 0, failures: 0, avgResponseTime: 0 },
          name: { requests: 0, successes: 0, failures: 0, avgResponseTime: 0 },
          phone: { requests: 0, successes: 0, failures: 0, avgResponseTime: 0 },
          address: { requests: 0, successes: 0, failures: 0, avgResponseTime: 0 }
        };
      }
      
      if (row.validation_type in periodData[periodKey]) {
        periodData[periodKey][row.validation_type] = {
          requests: parseInt(row.requests, 10),
          successes: parseInt(row.successes, 10),
          failures: parseInt(row.failures, 10),
          avgResponseTime: parseFloat(row.avg_response_time_ms)
        };
      }
    });
    
    formattedResults = Object.values(periodData);
  } else {
    // Single type
    const dailyData = {};
    
    rows.forEach(row => {
      const hourKey = row.period.toISOString();
      if (!dailyData[hourKey]) {
        dailyData[hourKey] = {
          period: hourKey,
          requests: 0,
          successes: 0,
          failures: 0,
          avgResponseTime: 0,
          hours: 0,
          errors: {
            timeout: 0,
            externalApi: 0
          }
        };
      }
      
      const typeData = dailyData[hourKey];
      typeData.requests += parseInt(row.requests, 10);
      typeData.successes += parseInt(row.successes, 10);
      typeData.failures += parseInt(row.failures, 10);
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

export default router;