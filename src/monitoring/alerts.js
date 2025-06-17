// src/monitoring/alerts.js
import { config } from '../core/config.js';
import { createServiceLogger } from '../core/logger.js';

// Create logger instance
const logger = createServiceLogger('alerts');

/**
 * Alert severity levels
 */
export const SEVERITY = {
  CRITICAL: 'critical',
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
  INFO: 'info'
};

/**
 * Alert channels - can be extended with other notification methods
 */
export const CHANNELS = {
  EMAIL: 'email',
  WEBHOOK: 'webhook',
  LOG: 'log' // Always available fallback
};

/**
 * Alert types grouped by category
 */
export const ALERT_TYPES = {
  // System health alerts
  SYSTEM: {
    SERVER_START: 'server_start',
    SERVER_STOP: 'server_stop',
    SERVER_ERROR: 'server_error',
    CONFIG_ERROR: 'config_error',
    MEMORY_HIGH: 'memory_high',
    CPU_HIGH: 'cpu_high'
  },
  
  // Database alerts
  DATABASE: {
    CONNECTION_FAILED: 'db_connection_failed',
    CONNECTION_SLOW: 'db_connection_slow',
    QUERY_TIMEOUT: 'db_query_timeout',
    POOL_EXHAUSTED: 'db_pool_exhausted'
  },
  
  // External service alerts
  EXTERNAL: {
    ZEROBOUNCE_ERROR: 'zerobounce_error',
    ZEROBOUNCE_RATE_LIMIT: 'zerobounce_rate_limit',
    ZEROBOUNCE_CREDITS_LOW: 'zerobounce_credits_low',
    OPENCAGE_ERROR: 'opencage_error',
    OPENCAGE_RATE_LIMIT: 'opencage_rate_limit',
    HUBSPOT_ERROR: 'hubspot_error'
  },
  
  // Application alerts
  APPLICATION: {
    API_ERROR_RATE_HIGH: 'api_error_rate_high',
    RATE_LIMIT_EXCEEDED: 'rate_limit_exceeded',
    QUEUE_BACKED_UP: 'queue_backed_up',
    QUEUE_PROCESSING_ERROR: 'queue_processing_error',
    WEBHOOK_ERROR: 'webhook_error',
    VALIDATION_ERROR_SPIKE: 'validation_error_spike'
  },
  
  // Security alerts
  SECURITY: {
    INVALID_API_KEY_ATTEMPTS: 'invalid_api_key_attempts',
    SUSPICIOUS_REQUEST_PATTERN: 'suspicious_request_pattern',
    RATE_LIMIT_ABUSE: 'rate_limit_abuse'
  },
  
  // Business alerts
  BUSINESS: {
    CLIENT_USAGE_HIGH: 'client_usage_high',
    CLIENT_APPROACHING_LIMIT: 'client_approaching_limit',
    CLIENT_REACHED_LIMIT: 'client_reached_limit'
  }
};

/**
 * Alert configuration settings
 * Each alert type has configuration for:
 * - severity: The alert's importance level
 * - channels: Where to send the alert
 * - threshold: Numerical threshold to trigger (if applicable)
 * - cooldown: Minimum time between alerts of this type (in ms)
 * - enabled: Whether the alert is active
 * - message: Template for alert message
 */
const alertConfig = {
  // System alerts
  [ALERT_TYPES.SYSTEM.SERVER_START]: {
    severity: SEVERITY.INFO,
    channels: [CHANNELS.LOG],
    cooldown: 0, // No cooldown for server start
    enabled: true,
    message: 'Server started in {environment} environment'
  },
  
  [ALERT_TYPES.SYSTEM.SERVER_STOP]: {
    severity: SEVERITY.INFO,
    channels: [CHANNELS.LOG],
    cooldown: 0, // No cooldown for server stop
    enabled: true,
    message: 'Server stopped in {environment} environment'
  },
  
  [ALERT_TYPES.SYSTEM.SERVER_ERROR]: {
    severity: SEVERITY.CRITICAL,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 300000, // 5 minutes
    enabled: true,
    message: 'Server error: {errorMessage}'
  },
  
  [ALERT_TYPES.SYSTEM.CONFIG_ERROR]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 3600000, // 1 hour
    enabled: true,
    message: 'Configuration error: {errorMessage}'
  },
  
  [ALERT_TYPES.SYSTEM.MEMORY_HIGH]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 80, // Percentage
    cooldown: 1800000, // 30 minutes
    enabled: true,
    message: 'High memory usage: {usagePercent}%'
  },
  
  [ALERT_TYPES.SYSTEM.CPU_HIGH]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 80, // Percentage
    cooldown: 1800000, // 30 minutes
    enabled: true,
    message: 'High CPU usage: {usagePercent}%'
  },
  
  // Database alerts
  [ALERT_TYPES.DATABASE.CONNECTION_FAILED]: {
    severity: SEVERITY.CRITICAL,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 300000, // 5 minutes
    enabled: true,
    message: 'Database connection failed: {errorMessage}'
  },
  
  [ALERT_TYPES.DATABASE.CONNECTION_SLOW]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 1000, // ms
    cooldown: 1800000, // 30 minutes
    enabled: true,
    message: 'Slow database connection: {connectionTime}ms'
  },
  
  [ALERT_TYPES.DATABASE.QUERY_TIMEOUT]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'Database query timeout: {query}'
  },
  
  [ALERT_TYPES.DATABASE.POOL_EXHAUSTED]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'Database connection pool exhausted'
  },
  
  // External service alerts
  [ALERT_TYPES.EXTERNAL.ZEROBOUNCE_ERROR]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'ZeroBounce API error: {errorMessage}'
  },
  
  [ALERT_TYPES.EXTERNAL.ZEROBOUNCE_RATE_LIMIT]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 3600000, // 1 hour
    enabled: true,
    message: 'ZeroBounce rate limit reached'
  },
  
  [ALERT_TYPES.EXTERNAL.ZEROBOUNCE_CREDITS_LOW]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 100, // Credits remaining
    cooldown: 86400000, // 24 hours
    enabled: true,
    message: 'ZeroBounce credits low: {creditsRemaining} remaining'
  },
  
  [ALERT_TYPES.EXTERNAL.OPENCAGE_ERROR]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'OpenCage API error: {errorMessage}'
  },
  
  [ALERT_TYPES.EXTERNAL.OPENCAGE_RATE_LIMIT]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 3600000, // 1 hour
    enabled: true,
    message: 'OpenCage rate limit reached'
  },
  
  [ALERT_TYPES.EXTERNAL.HUBSPOT_ERROR]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'HubSpot API error: {errorMessage}'
  },
  
  // Application alerts
  [ALERT_TYPES.APPLICATION.API_ERROR_RATE_HIGH]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 5, // Percentage
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'High API error rate: {errorRate}%'
  },
  
  [ALERT_TYPES.APPLICATION.RATE_LIMIT_EXCEEDED]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG],
    cooldown: 3600000, // 1 hour
    enabled: true,
    message: 'Rate limit exceeded for client {clientId}: {validationType}'
  },
  
  [ALERT_TYPES.APPLICATION.QUEUE_BACKED_UP]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 50, // Items
    cooldown: 1800000, // 30 minutes
    enabled: true,
    message: 'Queue backed up with {queueSize} pending items'
  },
  
  [ALERT_TYPES.APPLICATION.QUEUE_PROCESSING_ERROR]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'Queue processing error: {errorMessage}'
  },
  
  [ALERT_TYPES.APPLICATION.WEBHOOK_ERROR]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 900000, // 15 minutes
    enabled: true,
    message: 'Webhook processing error: {errorMessage}'
  },
  
  [ALERT_TYPES.APPLICATION.VALIDATION_ERROR_SPIKE]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 10, // Percentage increase
    cooldown: 3600000, // 1 hour
    enabled: true,
    message: 'Validation error spike: {errorRate}% (type: {validationType})'
  },
  
  // Security alerts
  [ALERT_TYPES.SECURITY.INVALID_API_KEY_ATTEMPTS]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 10, // Attempts
    cooldown: 1800000, // 30 minutes
    enabled: true,
    message: 'Multiple invalid API key attempts: {attempts} in the last hour'
  },
  
  [ALERT_TYPES.SECURITY.SUSPICIOUS_REQUEST_PATTERN]: {
    severity: SEVERITY.HIGH,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 3600000, // 1 hour
    enabled: true,
    message: 'Suspicious request pattern detected from IP {ipAddress}'
  },
  
  [ALERT_TYPES.SECURITY.RATE_LIMIT_ABUSE]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 5, // Consecutive violations
    cooldown: 3600000, // 1 hour
    enabled: true,
    message: 'Rate limit abuse detected for client {clientId}'
  },
  
  // Business alerts
  [ALERT_TYPES.BUSINESS.CLIENT_USAGE_HIGH]: {
    severity: SEVERITY.LOW,
    channels: [CHANNELS.LOG],
    threshold: 80, // Percentage
    cooldown: 86400000, // 24 hours
    enabled: true,
    message: 'High usage for client {clientId}: {usagePercent}% of {validationType} limit'
  },
  
  [ALERT_TYPES.BUSINESS.CLIENT_APPROACHING_LIMIT]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    threshold: 90, // Percentage
    cooldown: 86400000, // 24 hours
    enabled: true,
    message: 'Client {clientId} approaching {validationType} limit: {usagePercent}%'
  },
  
  [ALERT_TYPES.BUSINESS.CLIENT_REACHED_LIMIT]: {
    severity: SEVERITY.MEDIUM,
    channels: [CHANNELS.LOG, CHANNELS.EMAIL],
    cooldown: 86400000, // 24 hours
    enabled: true,
    message: 'Client {clientId} has reached their {validationType} limit'
  }
};

// Track when alerts were last triggered to respect cooldown periods
const lastAlertTimes = new Map();

/**
 * Format alert message with context data
 * @param {string} message - Message template with {placeholders}
 * @param {Object} context - Context data to insert into template
 * @returns {string} Formatted message
 */
function formatAlertMessage(message, context = {}) {
  return message.replace(/\{(\w+)\}/g, (match, key) => {
    return context[key] !== undefined ? context[key] : match;
  });
}

/**
 * Check if an alert can be triggered (respecting cooldown)
 * @param {string} alertType - Type of alert
 * @returns {boolean} Whether the alert can be triggered
 */
function canTriggerAlert(alertType) {
  const config = alertConfig[alertType];
  if (!config || !config.enabled) {
    return false;
  }
  
  const now = Date.now();
  const lastTriggered = lastAlertTimes.get(alertType) || 0;
  
  // Check cooldown period
  if (now - lastTriggered < config.cooldown) {
    return false;
  }
  
  return true;
}

/**
 * Check if a threshold-based alert should trigger
 * @param {string} alertType - Type of alert
 * @param {number} value - Current value to check against threshold
 * @returns {boolean} Whether the threshold is exceeded
 */
export function isThresholdExceeded(alertType, value) {
  const config = alertConfig[alertType];
  if (!config || !config.threshold) {
    return true; // No threshold defined, so always trigger
  }
  
  return value >= config.threshold;
}

/**
 * Trigger an alert
 * @param {string} alertType - Type of alert from ALERT_TYPES
 * @param {Object} context - Context data for alert message
 * @returns {boolean} Whether the alert was triggered
 */
export function triggerAlert(alertType, context = {}) {
  if (!canTriggerAlert(alertType)) {
    return false;
  }
  
  const config = alertConfig[alertType];
  
  // For threshold-based alerts, check the threshold
  if (config.threshold !== undefined) {
    const thresholdValue = context.value !== undefined ? context.value : 
      (context[Object.keys(context).find(k => !isNaN(context[k]))] || 0);
    
    if (!isThresholdExceeded(alertType, thresholdValue)) {
      return false;
    }
  }
  
  // Format message with context
  const message = formatAlertMessage(config.message, context);
  
  // Update last triggered time
  lastAlertTimes.set(alertType, Date.now());
  
  // Send alert to each configured channel
  config.channels.forEach(channel => {
    switch(channel) {
      case CHANNELS.LOG:
        logAlert(alertType, config.severity, message);
        break;
      case CHANNELS.EMAIL:
        sendEmailAlert(alertType, config.severity, message, context);
        break;
      case CHANNELS.WEBHOOK:
        sendWebhookAlert(alertType, config.severity, message, context);
        break;
    }
  });
  
  return true;
}

/**
 * Log an alert to the logger
 * @param {string} alertType - Type of alert
 * @param {string} severity - Alert severity
 * @param {string} message - Alert message
 */
function logAlert(alertType, severity, message) {
  const logLevel = {
    [SEVERITY.CRITICAL]: 'error',
    [SEVERITY.HIGH]: 'error',
    [SEVERITY.MEDIUM]: 'warn',
    [SEVERITY.LOW]: 'info',
    [SEVERITY.INFO]: 'info'
  }[severity] || 'info';
  
  logger[logLevel](`ALERT [${severity.toUpperCase()}]: ${message}`, { alertType });
}

/**
 * Send an email alert
 * @param {string} alertType - Type of alert
 * @param {string} severity - Alert severity
 * @param {string} message - Alert message
 * @param {Object} context - Alert context
 */
function sendEmailAlert(alertType, severity, message, context) {
  // Skip email alerts in development unless explicitly enabled
  if (config.isDevelopment && !process.env.ENABLE_DEV_EMAILS) {
    logger.debug('Email alert skipped in development', { alertType, message });
    return;
  }
  
  // In production, this would use an email service
  // For now, just log the email alert
  logger.info('Would send email alert', { 
    alertType,
    severity,
    message,
    to: process.env.ALERT_EMAIL || 'dev@unmessy.io',
    subject: `[${severity.toUpperCase()}] Unmessy Alert: ${alertType}`
  });
  
  // TODO: Implement actual email sending in production
}

/**
 * Send a webhook alert
 * @param {string} alertType - Type of alert
 * @param {string} severity - Alert severity
 * @param {string} message - Alert message
 * @param {Object} context - Alert context
 */
function sendWebhookAlert(alertType, severity, message, context) {
  // Skip webhook alerts in development unless explicitly enabled
  if (config.isDevelopment && !process.env.ENABLE_DEV_WEBHOOKS) {
    logger.debug('Webhook alert skipped in development', { alertType, message });
    return;
  }
  
  // In production, this would POST to a webhook URL
  // For now, just log the webhook alert
  logger.info('Would send webhook alert', { 
    alertType,
    severity,
    message,
    webhook: process.env.ALERT_WEBHOOK_URL || 'https://example.com/webhook'
  });
  
  // TODO: Implement actual webhook sending in production
}

/**
 * Check if system resource usage exceeds thresholds and trigger alerts if needed
 */
export function checkSystemResources() {
  // Check memory usage
  const memoryUsage = process.memoryUsage();
  const heapUsedPercent = (memoryUsage.heapUsed / memoryUsage.heapTotal) * 100;
  
  if (isThresholdExceeded(ALERT_TYPES.SYSTEM.MEMORY_HIGH, heapUsedPercent)) {
    triggerAlert(ALERT_TYPES.SYSTEM.MEMORY_HIGH, {
      usagePercent: heapUsedPercent.toFixed(2),
      heapUsed: formatBytes(memoryUsage.heapUsed),
      heapTotal: formatBytes(memoryUsage.heapTotal),
      rss: formatBytes(memoryUsage.rss)
    });
  }
  
  // CPU usage would require an external module to measure accurately
  // This is a placeholder for actual implementation
}

/**
 * Format bytes to human-readable format
 * @param {number} bytes - Bytes to format
 * @returns {string} Formatted string
 */
function formatBytes(bytes) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (bytes >= 1024 && i < units.length - 1) {
    bytes /= 1024;
    i++;
  }
  return `${bytes.toFixed(2)} ${units[i]}`;
}

/**
 * Initialize alert monitoring
 * Sets up periodic checks and event listeners
 */
export function initializeAlertMonitoring() {
  logger.info('Initializing alert monitoring...');
  
  // Set up periodic system resource checks
  if (config.isProduction) {
    setInterval(checkSystemResources, 60000); // Every minute
  }
  
  // Trigger server start alert
  triggerAlert(ALERT_TYPES.SYSTEM.SERVER_START, {
    environment: config.env,
    version: config.unmessy.version
  });
  
  // Set up process exit handler for server stop alert
  process.on('exit', () => {
    triggerAlert(ALERT_TYPES.SYSTEM.SERVER_STOP, {
      environment: config.env,
      version: config.unmessy.version
    });
  });
  
  logger.info('Alert monitoring initialized');
}

// Export alert system
export default {
  SEVERITY,
  CHANNELS,
  ALERT_TYPES,
  triggerAlert,
  initializeAlertMonitoring,
  formatAlertMessage,
  isThresholdExceeded,
  checkSystemResources
};