// src/monitoring/metrics.js
import os from 'os';
import { createServiceLogger } from '../core/logger.js';
import { ErrorRecovery } from '../core/errors.js';
import { config } from '../core/config.js';
import db from '../core/db.js';
import * as alerts from './alerts.js';

// Create logger instance
const logger = createServiceLogger('metrics');

/**
 * Metric types for different aspects of the system
 */
export const METRIC_TYPES = {
  // API metrics
  API: {
    REQUEST_COUNT: 'api.request.count',
    REQUEST_DURATION: 'api.request.duration',
    ERROR_COUNT: 'api.error.count',
    RATE_LIMIT_EXCEEDED: 'api.rate_limit.exceeded',
    STATUS_CODE_COUNT: 'api.status_code.count'
  },

  // Validation metrics
  VALIDATION: {
    EMAIL_COUNT: 'validation.email.count',
    EMAIL_DURATION: 'validation.email.duration',
    EMAIL_SUCCESS_RATE: 'validation.email.success_rate',
    EMAIL_CORRECTION_RATE: 'validation.email.correction_rate',
    
    NAME_COUNT: 'validation.name.count',
    NAME_DURATION: 'validation.name.duration',
    NAME_SUCCESS_RATE: 'validation.name.success_rate',
    NAME_CORRECTION_RATE: 'validation.name.correction_rate',
    
    PHONE_COUNT: 'validation.phone.count',
    PHONE_DURATION: 'validation.phone.duration',
    PHONE_SUCCESS_RATE: 'validation.phone.success_rate',
    PHONE_CORRECTION_RATE: 'validation.phone.correction_rate',
    
    ADDRESS_COUNT: 'validation.address.count',
    ADDRESS_DURATION: 'validation.address.duration',
    ADDRESS_SUCCESS_RATE: 'validation.address.success_rate',
    ADDRESS_CORRECTION_RATE: 'validation.address.correction_rate'
  },

  // External service metrics
  EXTERNAL: {
    ZEROBOUNCE_REQUEST_COUNT: 'external.zerobounce.request.count',
    ZEROBOUNCE_REQUEST_DURATION: 'external.zerobounce.request.duration',
    ZEROBOUNCE_ERROR_COUNT: 'external.zerobounce.error.count',
    
    OPENCAGE_REQUEST_COUNT: 'external.opencage.request.count',
    OPENCAGE_REQUEST_DURATION: 'external.opencage.request.duration',
    OPENCAGE_ERROR_COUNT: 'external.opencage.error.count',
    
    HUBSPOT_REQUEST_COUNT: 'external.hubspot.request.count',
    HUBSPOT_REQUEST_DURATION: 'external.hubspot.request.duration',
    HUBSPOT_ERROR_COUNT: 'external.hubspot.error.count'
  },

  // Database metrics
  DATABASE: {
    QUERY_COUNT: 'database.query.count',
    QUERY_DURATION: 'database.query.duration',
    ERROR_COUNT: 'database.error.count',
    CONNECTION_COUNT: 'database.connection.count',
    POOL_SIZE: 'database.pool.size',
    POOL_ACTIVE: 'database.pool.active'
  },

  // Queue metrics
  QUEUE: {
    PENDING_COUNT: 'queue.pending.count',
    PROCESSING_COUNT: 'queue.processing.count',
    COMPLETED_COUNT: 'queue.completed.count',
    FAILED_COUNT: 'queue.failed.count',
    PROCESSING_DURATION: 'queue.processing.duration',
    RETRY_COUNT: 'queue.retry.count'
  },

  // System metrics
  SYSTEM: {
    CPU_USAGE: 'system.cpu.usage',
    MEMORY_USAGE: 'system.memory.usage',
    HEAP_USAGE: 'system.memory.heap.usage',
    UPTIME: 'system.uptime'
  },

  // Client metrics
  CLIENT: {
    USAGE_EMAIL: 'client.usage.email',
    USAGE_NAME: 'client.usage.name',
    USAGE_PHONE: 'client.usage.phone',
    USAGE_ADDRESS: 'client.usage.address',
    LIMIT_REMAINING_EMAIL: 'client.limit.remaining.email',
    LIMIT_REMAINING_NAME: 'client.limit.remaining.name',
    LIMIT_REMAINING_PHONE: 'client.limit.remaining.phone',
    LIMIT_REMAINING_ADDRESS: 'client.limit.remaining.address'
  }
};

/**
 * Aggregation types for metrics
 */
export const AGGREGATION_TYPES = {
  COUNT: 'count',
  GAUGE: 'gauge',
  HISTOGRAM: 'histogram',
  SUMMARY: 'summary'
};

/**
 * Time windows for aggregating metrics
 */
export const TIME_WINDOWS = {
  MINUTE: 60,
  FIVE_MINUTES: 300,
  FIFTEEN_MINUTES: 900,
  HOUR: 3600,
  DAY: 86400
};

// In-memory storage for metrics
// In a production system, this would be replaced with a proper time-series database
const metricsStorage = {
  // Store raw metrics with timestamps
  raw: [],
  
  // Store aggregated metrics by type, window, and dimensions
  aggregated: {
    [TIME_WINDOWS.MINUTE]: {},
    [TIME_WINDOWS.FIVE_MINUTES]: {},
    [TIME_WINDOWS.FIFTEEN_MINUTES]: {},
    [TIME_WINDOWS.HOUR]: {},
    [TIME_WINDOWS.DAY]: {}
  },
  
  // Maximum number of raw metrics to store
  maxRawMetrics: 10000,
  
  // Current aggregation timestamp windows
  currentWindows: {
    [TIME_WINDOWS.MINUTE]: Math.floor(Date.now() / 1000 / TIME_WINDOWS.MINUTE),
    [TIME_WINDOWS.FIVE_MINUTES]: Math.floor(Date.now() / 1000 / TIME_WINDOWS.FIVE_MINUTES),
    [TIME_WINDOWS.FIFTEEN_MINUTES]: Math.floor(Date.now() / 1000 / TIME_WINDOWS.FIFTEEN_MINUTES),
    [TIME_WINDOWS.HOUR]: Math.floor(Date.now() / 1000 / TIME_WINDOWS.HOUR),
    [TIME_WINDOWS.DAY]: Math.floor(Date.now() / 1000 / TIME_WINDOWS.DAY)
  }
};

/**
 * Record a metric
 * @param {string} metricType - Type of metric from METRIC_TYPES
 * @param {number} value - Metric value
 * @param {Object} dimensions - Additional dimensions for the metric
 * @param {string} aggregationType - Type of aggregation from AGGREGATION_TYPES
 */
export function recordMetric(metricType, value, dimensions = {}, aggregationType = AGGREGATION_TYPES.COUNT) {
  // Skip metrics in test environment
  if (config.isTest) {
    return;
  }
  
  const timestamp = Date.now();
  
  // Create metric object
  const metric = {
    type: metricType,
    value: value,
    timestamp: timestamp,
    dimensions: {
      environment: config.env,
      ...dimensions
    },
    aggregationType: aggregationType
  };
  
  // Store raw metric (if enabled)
  if (config.monitoring.enabled) {
    storeRawMetric(metric);
  }
  
  // Aggregate metric for different time windows
  for (const window of Object.values(TIME_WINDOWS)) {
    aggregateMetric(metric, window);
  }
  
  // Persist metrics to database if enabled
  if (config.monitoring.persistMetrics && shouldPersistMetric(metricType)) {
    persistMetric(metric).catch(error => {
      logger.error('Failed to persist metric', error, { metricType });
    });
  }
  
  // Check for anomalies or alert conditions
  checkAlertConditions(metric);
  
  return metric;
}

/**
 * Store a raw metric in memory
 * @param {Object} metric - Metric object
 */
function storeRawMetric(metric) {
  // Add to raw metrics
  metricsStorage.raw.push(metric);
  
  // Trim if over max size
  if (metricsStorage.raw.length > metricsStorage.maxRawMetrics) {
    metricsStorage.raw.shift();
  }
}

/**
 * Aggregate a metric for a specific time window
 * @param {Object} metric - Metric object
 * @param {number} window - Time window in seconds
 */
function aggregateMetric(metric, window) {
  const { type, value, timestamp, dimensions, aggregationType } = metric;
  
  // Calculate the window key
  const windowKey = Math.floor(timestamp / 1000 / window);
  
  // Check if window has changed
  if (windowKey !== metricsStorage.currentWindows[window]) {
    // Reset aggregation for the new window
    metricsStorage.aggregated[window] = {};
    metricsStorage.currentWindows[window] = windowKey;
  }
  
  // Create dimension key
  const dimensionKey = JSON.stringify(dimensions);
  
  // Create metric key
  const metricKey = `${type}:${dimensionKey}`;
  
  // Initialize if not exists
  if (!metricsStorage.aggregated[window][metricKey]) {
    metricsStorage.aggregated[window][metricKey] = {
      type,
      dimensions,
      count: 0,
      sum: 0,
      min: Number.MAX_VALUE,
      max: Number.MIN_VALUE,
      avg: 0,
      values: []
    };
  }
  
  // Update aggregation
  const agg = metricsStorage.aggregated[window][metricKey];
  agg.count++;
  agg.sum += value;
  agg.min = Math.min(agg.min, value);
  agg.max = Math.max(agg.max, value);
  agg.avg = agg.sum / agg.count;
  
  // For histograms, store individual values (up to 100)
  if (aggregationType === AGGREGATION_TYPES.HISTOGRAM && agg.values.length < 100) {
    agg.values.push(value);
  }
}

/**
 * Determine if a metric should be persisted to the database
 * @param {string} metricType - Type of metric
 * @returns {boolean} Whether to persist the metric
 */
function shouldPersistMetric(metricType) {
  // Don't persist high-frequency metrics
  if (metricType.startsWith('api.request.') || 
      metricType.startsWith('database.query.')) {
    return false;
  }
  
  // Persist all other metrics
  return true;
}

/**
 * Persist a metric to the database
 * @param {Object} metric - Metric object
 */
async function persistMetric(metric) {
  try {
    const { type, value, timestamp, dimensions } = metric;
    
    // Convert dimensions to database format
    const dimensionsJson = JSON.stringify(dimensions);
    
    // Determine category and name
    const [category, ...nameParts] = type.split('.');
    const name = nameParts.join('.');
    
    // Insert into database
    await db.query(
      `INSERT INTO metrics (
        category, 
        name, 
        value, 
        dimensions, 
        timestamp
      ) VALUES ($1, $2, $3, $4, $5)`,
      [category, name, value, dimensionsJson, new Date(timestamp)]
    );
  } catch (error) {
    logger.error('Failed to persist metric', error, { metric });
  }
}

/**
 * Check if a metric should trigger an alert
 * @param {Object} metric - Metric object
 */
function checkAlertConditions(metric) {
  const { type, value, dimensions } = metric;
  
  // Check error rate
  if (type === METRIC_TYPES.API.ERROR_COUNT && dimensions.endpoint) {
    // Get corresponding request count
    const requestType = METRIC_TYPES.API.REQUEST_COUNT;
    const requestMetric = getAggregatedMetric(requestType, dimensions, TIME_WINDOWS.FIVE_MINUTES);
    
    if (requestMetric && requestMetric.count > 0) {
      const errorRate = (value / requestMetric.count) * 100;
      
      // Alert if error rate is high
      if (errorRate > 5) {
        alerts.triggerAlert(alerts.ALERT_TYPES.APPLICATION.API_ERROR_RATE_HIGH, {
          endpoint: dimensions.endpoint,
          errorRate: errorRate.toFixed(2),
          errorCount: value,
          requestCount: requestMetric.count,
          value: errorRate
        });
      }
    }
  }
  
  // Check ZeroBounce errors
  if (type === METRIC_TYPES.EXTERNAL.ZEROBOUNCE_ERROR_COUNT && value > 0) {
    alerts.triggerAlert(alerts.ALERT_TYPES.EXTERNAL.ZEROBOUNCE_ERROR, {
      errorCount: value,
      errorMessage: dimensions.errorMessage || 'Multiple errors'
    });
  }
  
  // Check OpenCage errors
  if (type === METRIC_TYPES.EXTERNAL.OPENCAGE_ERROR_COUNT && value > 0) {
    alerts.triggerAlert(alerts.ALERT_TYPES.EXTERNAL.OPENCAGE_ERROR, {
      errorCount: value,
      errorMessage: dimensions.errorMessage || 'Multiple errors'
    });
  }
  
  // Check queue backlog
  if (type === METRIC_TYPES.QUEUE.PENDING_COUNT && value > 50) {
    alerts.triggerAlert(alerts.ALERT_TYPES.APPLICATION.QUEUE_BACKED_UP, {
      queueSize: value,
      value: value
    });
  }
  
  // Check client usage
  if (type.startsWith('client.usage.') && dimensions.clientId) {
    const validationType = type.split('.').pop();
    const usagePercent = value;
    
    // Alert for high usage
    if (usagePercent >= 80) {
      alerts.triggerAlert(alerts.ALERT_TYPES.BUSINESS.CLIENT_USAGE_HIGH, {
        clientId: dimensions.clientId,
        validationType,
        usagePercent: usagePercent.toFixed(2),
        value: usagePercent
      });
    }
    
    // Alert for approaching limit
    if (usagePercent >= 90) {
      alerts.triggerAlert(alerts.ALERT_TYPES.BUSINESS.CLIENT_APPROACHING_LIMIT, {
        clientId: dimensions.clientId,
        validationType,
        usagePercent: usagePercent.toFixed(2),
        value: usagePercent
      });
    }
    
    // Alert for reached limit
    if (usagePercent >= 99.5) {
      alerts.triggerAlert(alerts.ALERT_TYPES.BUSINESS.CLIENT_REACHED_LIMIT, {
        clientId: dimensions.clientId,
        validationType,
        value: usagePercent
      });
    }
  }
  
  // Check system metrics
  if (type === METRIC_TYPES.SYSTEM.MEMORY_USAGE && value > 80) {
    alerts.triggerAlert(alerts.ALERT_TYPES.SYSTEM.MEMORY_HIGH, {
      usagePercent: value.toFixed(2),
      value: value
    });
  }
  
  if (type === METRIC_TYPES.SYSTEM.CPU_USAGE && value > 80) {
    alerts.triggerAlert(alerts.ALERT_TYPES.SYSTEM.CPU_HIGH, {
      usagePercent: value.toFixed(2),
      value: value
    });
  }
}

/**
 * Get an aggregated metric for a specific type, dimensions, and window
 * @param {string} metricType - Type of metric
 * @param {Object} dimensions - Dimensions to filter by
 * @param {number} window - Time window in seconds
 * @returns {Object} Aggregated metric or null if not found
 */
export function getAggregatedMetric(metricType, dimensions, window) {
  const dimensionKey = JSON.stringify(dimensions);
  const metricKey = `${metricType}:${dimensionKey}`;
  
  return metricsStorage.aggregated[window][metricKey] || null;
}

/**
 * Get all aggregated metrics for a specific window
 * @param {number} window - Time window in seconds
 * @returns {Object[]} Array of aggregated metrics
 */
export function getAllAggregatedMetrics(window) {
  return Object.values(metricsStorage.aggregated[window] || {});
}

/**
 * Get filtered metrics based on criteria
 * @param {Object} filters - Filters to apply
 * @param {number} window - Time window in seconds
 * @returns {Object[]} Filtered metrics
 */
export function getFilteredMetrics(filters, window = TIME_WINDOWS.FIVE_MINUTES) {
  const metrics = getAllAggregatedMetrics(window);
  
  return metrics.filter(metric => {
    // Filter by type prefix
    if (filters.typePrefix && !metric.type.startsWith(filters.typePrefix)) {
      return false;
    }
    
    // Filter by exact type
    if (filters.type && metric.type !== filters.type) {
      return false;
    }
    
    // Filter by dimension values
    if (filters.dimensions) {
      for (const [key, value] of Object.entries(filters.dimensions)) {
        if (metric.dimensions[key] !== value) {
          return false;
        }
      }
    }
    
    return true;
  });
}

/**
 * Record API request metric
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {number} duration - Request duration in milliseconds
 */
export function recordApiRequest(req, res, duration) {
  const endpoint = req.path;
  const method = req.method;
  const statusCode = res.statusCode;
  const clientId = req.clientId || 'anonymous';
  
  // Record request count
  recordMetric(METRIC_TYPES.API.REQUEST_COUNT, 1, {
    endpoint,
    method,
    clientId
  });
  
  // Record request duration
  recordMetric(METRIC_TYPES.API.REQUEST_DURATION, duration, {
    endpoint,
    method,
    clientId
  }, AGGREGATION_TYPES.HISTOGRAM);
  
  // Record status code
  recordMetric(METRIC_TYPES.API.STATUS_CODE_COUNT, 1, {
    endpoint,
    method,
    statusCode,
    clientId
  });
  
  // Record error if status >= 400
  if (statusCode >= 400) {
    recordMetric(METRIC_TYPES.API.ERROR_COUNT, 1, {
      endpoint,
      method,
      statusCode,
      clientId,
      errorType: res.locals.errorType || 'unknown'
    });
  }
}

/**
 * Record validation metric
 * @param {string} validationType - Type of validation (email, name, phone, address)
 * @param {boolean} success - Whether validation was successful
 * @param {boolean} corrected - Whether value was corrected
 * @param {number} duration - Validation duration in milliseconds
 * @param {string} clientId - Client ID
 */
export function recordValidation(validationType, success, corrected, duration, clientId) {
  // Record count
  recordMetric(METRIC_TYPES.VALIDATION[`${validationType.toUpperCase()}_COUNT`], 1, {
    clientId,
    success
  });
  
  // Record duration
  recordMetric(METRIC_TYPES.VALIDATION[`${validationType.toUpperCase()}_DURATION`], duration, {
    clientId,
    success
  }, AGGREGATION_TYPES.HISTOGRAM);
  
  // Record success rate (1 for success, 0 for failure)
  recordMetric(METRIC_TYPES.VALIDATION[`${validationType.toUpperCase()}_SUCCESS_RATE`], success ? 1 : 0, {
    clientId
  }, AGGREGATION_TYPES.GAUGE);
  
  // Record correction rate (1 for corrected, 0 for not corrected)
  recordMetric(METRIC_TYPES.VALIDATION[`${validationType.toUpperCase()}_CORRECTION_RATE`], corrected ? 1 : 0, {
    clientId
  }, AGGREGATION_TYPES.GAUGE);
}

/**
 * Record external service request metric
 * @param {string} service - Service name (zerobounce, opencage, hubspot)
 * @param {string} operation - Operation name
 * @param {boolean} success - Whether request was successful
 * @param {number} duration - Request duration in milliseconds
 * @param {string} errorMessage - Error message if failed
 */
export function recordExternalRequest(service, operation, success, duration, errorMessage = null) {
  const serviceName = service.toUpperCase();
  
  // Record request count
  recordMetric(METRIC_TYPES.EXTERNAL[`${serviceName}_REQUEST_COUNT`], 1, {
    operation,
    success
  });
  
  // Record request duration
  recordMetric(METRIC_TYPES.EXTERNAL[`${serviceName}_REQUEST_DURATION`], duration, {
    operation,
    success
  }, AGGREGATION_TYPES.HISTOGRAM);
  
  // Record error if failed
  if (!success) {
    recordMetric(METRIC_TYPES.EXTERNAL[`${serviceName}_ERROR_COUNT`], 1, {
      operation,
      errorMessage: errorMessage ? errorMessage.substring(0, 100) : 'Unknown error'
    });
  }
}

/**
 * Record database query metric
 * @param {string} operation - Query operation type
 * @param {string} table - Table name
 * @param {boolean} success - Whether query was successful
 * @param {number} duration - Query duration in milliseconds
 */
export function recordDatabaseQuery(operation, table, success, duration) {
  // Record query count
  recordMetric(METRIC_TYPES.DATABASE.QUERY_COUNT, 1, {
    operation,
    table,
    success
  });
  
  // Record query duration
  recordMetric(METRIC_TYPES.DATABASE.QUERY_DURATION, duration, {
    operation,
    table,
    success
  }, AGGREGATION_TYPES.HISTOGRAM);
  
  // Record error if failed
  if (!success) {
    recordMetric(METRIC_TYPES.DATABASE.ERROR_COUNT, 1, {
      operation,
      table
    });
  }
}

/**
 * Record queue metrics
 * @param {Object} queueStats - Queue statistics
 */
export function recordQueueMetrics(queueStats) {
  // Record queue sizes
  recordMetric(METRIC_TYPES.QUEUE.PENDING_COUNT, queueStats.pending || 0, {}, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.QUEUE.PROCESSING_COUNT, queueStats.processing || 0, {}, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.QUEUE.COMPLETED_COUNT, queueStats.completed || 0, {}, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.QUEUE.FAILED_COUNT, queueStats.failed || 0, {}, AGGREGATION_TYPES.GAUGE);
  
  // Record processing duration if available
  if (queueStats.avgProcessingTime) {
    recordMetric(METRIC_TYPES.QUEUE.PROCESSING_DURATION, queueStats.avgProcessingTime, {}, AGGREGATION_TYPES.GAUGE);
  }
  
  // Record retry count if available
  if (queueStats.retryCount) {
    recordMetric(METRIC_TYPES.QUEUE.RETRY_COUNT, queueStats.retryCount, {}, AGGREGATION_TYPES.GAUGE);
  }
}

/**
 * Record system metrics
 */
export function recordSystemMetrics() {
  try {
    // Record memory usage
    const memoryUsage = process.memoryUsage();
    const heapUsedPercent = (memoryUsage.heapUsed / memoryUsage.heapTotal) * 100;
    const memoryUsedPercent = (memoryUsage.rss / (os.totalmem ? os.totalmem() : 1)) * 100;
    
    recordMetric(METRIC_TYPES.SYSTEM.MEMORY_USAGE, memoryUsedPercent, {}, AGGREGATION_TYPES.GAUGE);
    recordMetric(METRIC_TYPES.SYSTEM.HEAP_USAGE, heapUsedPercent, {}, AGGREGATION_TYPES.GAUGE);
    
    // Record uptime
    recordMetric(METRIC_TYPES.SYSTEM.UPTIME, process.uptime(), {}, AGGREGATION_TYPES.GAUGE);
    
    // CPU usage would require an external module to measure accurately
    // This is a placeholder for actual implementation
    
  } catch (error) {
    logger.error('Failed to record system metrics', error);
  }
}

/**
 * Record client usage metrics
 * @param {Object} client - Client object with usage statistics
 */
export function recordClientUsageMetrics(client) {
  if (!client || !client.client_id) return;
  
  const clientId = client.client_id.toString();
  
  // Calculate usage percentages
  const emailUsage = client.daily_email_limit > 0 
    ? ((client.daily_email_limit - client.remaining_email) / client.daily_email_limit) * 100 
    : 0;
    
  const nameUsage = client.daily_name_limit > 0 
    ? ((client.daily_name_limit - client.remaining_name) / client.daily_name_limit) * 100 
    : 0;
    
  const phoneUsage = client.daily_phone_limit > 0 
    ? ((client.daily_phone_limit - client.remaining_phone) / client.daily_phone_limit) * 100 
    : 0;
    
  const addressUsage = client.daily_address_limit > 0 
    ? ((client.daily_address_limit - client.remaining_address) / client.daily_address_limit) * 100 
    : 0;
  
  // Record usage percentages
  recordMetric(METRIC_TYPES.CLIENT.USAGE_EMAIL, emailUsage, { clientId }, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.CLIENT.USAGE_NAME, nameUsage, { clientId }, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.CLIENT.USAGE_PHONE, phoneUsage, { clientId }, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.CLIENT.USAGE_ADDRESS, addressUsage, { clientId }, AGGREGATION_TYPES.GAUGE);
  
  // Record remaining limits
  recordMetric(METRIC_TYPES.CLIENT.LIMIT_REMAINING_EMAIL, client.remaining_email, { clientId }, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.CLIENT.LIMIT_REMAINING_NAME, client.remaining_name, { clientId }, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.CLIENT.LIMIT_REMAINING_PHONE, client.remaining_phone, { clientId }, AGGREGATION_TYPES.GAUGE);
  recordMetric(METRIC_TYPES.CLIENT.LIMIT_REMAINING_ADDRESS, client.remaining_address, { clientId }, AGGREGATION_TYPES.GAUGE);
}

/**
 * Initialize metrics collection and periodic tasks
 */
export function initializeMetrics() {
  logger.info('Initializing metrics collection...');
  
  // Only collect metrics in production if enabled
  if (!config.monitoring.enabled) {
    logger.info('Metrics collection is disabled');
    return;
  }
  
  // Set up periodic collection of system metrics
  setInterval(recordSystemMetrics, 60000); // Every minute
  
  // Set up periodic collection of queue metrics
  setInterval(async () => {
    try {
      const queueStats = await getQueueStats();
      recordQueueMetrics(queueStats);
    } catch (error) {
      logger.error('Failed to collect queue metrics', error);
    }
  }, 60000); // Every minute
  
  logger.info('Metrics collection initialized');
}

/**
 * Get queue statistics
 * @returns {Promise<Object>} Queue statistics
 */
async function getQueueStats() {
  try {
    // Get queue statistics from database
    const result = await ErrorRecovery.withRetry(async () => {
      return await db.query(`
        SELECT 
          status, 
          COUNT(*) as count,
          AVG(EXTRACT(EPOCH FROM (COALESCE(processing_completed_at, CURRENT_TIMESTAMP) - processing_started_at)) * 1000) as avg_time
        FROM 
          hubspot_webhook_queue 
        WHERE 
          created_at > NOW() - INTERVAL '24 hours'
        GROUP BY 
          status
      `);
    });
    
    // Parse queue statistics
    const queueStats = {
      pending: 0,
      processing: 0,
      completed: 0,
      failed: 0,
      total: 0,
      avgProcessingTime: 0,
      retryCount: 0
    };
    
    // Process each status
    result.rows.forEach(row => {
      const status = row.status.toLowerCase();
      const count = parseInt(row.count, 10);
      
      queueStats[status] = count;
      queueStats.total += count;
      
      if (status === 'completed' && row.avg_time) {
        queueStats.avgProcessingTime = parseFloat(row.avg_time);
      }
    });
    
    // Get retry count
    const retryResult = await db.query(`
      SELECT COUNT(*) as count
      FROM hubspot_webhook_queue
      WHERE attempts > 1
      AND created_at > NOW() - INTERVAL '24 hours'
    `);
    
    if (retryResult.rows.length > 0) {
      queueStats.retryCount = parseInt(retryResult.rows[0].count, 10);
    }
    
    return queueStats;
  } catch (error) {
    logger.error('Failed to get queue statistics', error);
    throw error;
  }
}

/**
 * Express middleware for recording API metrics
 */
export function metricsMiddleware() {
  return (req, res, next) => {
    // Skip metrics for health checks in production
    if (config.isProduction && req.path.includes('/health')) {
      return next();
    }
    
    // Record start time
    const startTime = Date.now();
    
    // Capture original end method
    const originalEnd = res.end;
    
    // Override end method to capture metrics
    res.end = function(...args) {
      // Calculate duration
      const duration = Date.now() - startTime;
      
      // Record metrics
      recordApiRequest(req, res, duration);
      
      // Call original end method
      return originalEnd.apply(this, args);
    };
    
    next();
  };
}

// Export metrics functions and types
export default {
  METRIC_TYPES,
  AGGREGATION_TYPES,
  TIME_WINDOWS,
  recordMetric,
  recordApiRequest,
  recordValidation,
  recordExternalRequest,
  recordDatabaseQuery,
  recordQueueMetrics,
  recordSystemMetrics,
  recordClientUsageMetrics,
  getAggregatedMetric,
  getAllAggregatedMetrics,
  getFilteredMetrics,
  initializeMetrics,
  getQueueStats,
  metricsMiddleware
};