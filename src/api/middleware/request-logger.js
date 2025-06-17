// src/api/middleware/request-logger.js
const { v4: uuidv4 } = require('uuid');
const { Logger, createServiceLogger, sanitizeLogData } = require('../../core/logger');
const config = require('../../core/config');

// Create logger instance
const baseLogger = createServiceLogger('http');

/**
 * Generates a unique request ID
 * @returns {string} UUID v4
 */
function generateRequestId() {
  return uuidv4();
}

/**
 * Determines if a request should be logged
 * @param {Object} req - Express request object
 * @returns {boolean} Whether to log this request
 */
function shouldLogRequest(req) {
  // Skip health check endpoints in production to reduce noise
  if (config.isProduction && req.path.includes('/health')) {
    return false;
  }
  
  return true;
}

/**
 * Extracts client info for logging
 * @param {Object} req - Express request object
 * @returns {Object} Client info for logging
 */
function getClientInfo(req) {
  const clientInfo = {
    clientId: req.clientId || 'anonymous',
    ip: req.ip || req.connection.remoteAddress
  };
  
  if (req.client) {
    clientInfo.clientName = req.client.name;
  }
  
  return clientInfo;
}

/**
 * Safely stringifies an object for logging
 * @param {any} obj - Object to stringify
 * @param {number} maxLength - Maximum length before truncation
 * @returns {string} Stringified object
 */
function safeStringify(obj, maxLength = 1000) {
  if (!obj) return '';
  
  try {
    const str = JSON.stringify(sanitizeLogData(obj));
    if (str.length <= maxLength) return str;
    return str.substring(0, maxLength) + '... [truncated]';
  } catch (err) {
    return '[Could not stringify]';
  }
}

/**
 * Decides whether to log request body based on content type
 * @param {Object} req - Express request object
 * @returns {boolean} Whether to log body
 */
function shouldLogBody(req) {
  const contentType = req.get('content-type') || '';
  
  // Don't log file uploads
  if (contentType.includes('multipart/form-data')) {
    return false;
  }
  
  return true;
}

/**
 * Request logging middleware
 * 
 * @param {Object} options - Middleware options
 * @param {boolean} options.logBody - Whether to log request bodies
 * @param {boolean} options.logHeaders - Whether to log request headers
 * @param {number} options.bodyMaxLength - Maximum length for logged bodies
 * @returns {Function} Express middleware function
 */
function requestLogger(options = {}) {
  const {
    logBody = true,
    logHeaders = config.isDevelopment,
    bodyMaxLength = 1000
  } = options;
  
  return (req, res, next) => {
    // Skip logging for certain requests
    if (!shouldLogRequest(req)) {
      return next();
    }
    
    // Generate request ID and start time
    const requestId = req.headers['x-request-id'] || generateRequestId();
    const startTime = Date.now();
    
    // Add request ID to response headers
    res.set('X-Request-ID', requestId);
    
    // Create context-aware logger
    const clientInfo = getClientInfo(req);
    const requestLogger = new Logger({
      ...clientInfo,
      requestId,
      method: req.method,
      path: req.path
    });
    
    // Attach logger to request object for use in route handlers
    req.logger = requestLogger;
    req.requestId = requestId;
    
    // Log request
    const logData = {
      requestId,
      method: req.method,
      url: req.originalUrl,
      userAgent: req.get('user-agent'),
      ...clientInfo
    };
    
    // Add query parameters if present
    if (Object.keys(req.query).length > 0) {
      logData.query = sanitizeLogData(req.query);
    }
    
    // Add headers if enabled
    if (logHeaders) {
      logData.headers = sanitizeLogData(req.headers);
    }
    
    // Add body if enabled and appropriate
    if (logBody && shouldLogBody(req) && req.body) {
      logData.body = safeStringify(req.body, bodyMaxLength);
    }
    
    requestLogger.info('Request received', logData);
    
    // Intercept response to log when finished
    const originalEnd = res.end;
    res.end = function(...args) {
      // Calculate response time
      const responseTime = Date.now() - startTime;
      
      // Log response
      const responseLogData = {
        requestId,
        method: req.method,
        url: req.originalUrl,
        statusCode: res.statusCode,
        responseTime,
        responseSize: res.get('content-length'),
        ...clientInfo
      };
      
      // Choose log level based on status code
      const logLevel = res.statusCode >= 500 ? 'error' : 
                      res.statusCode >= 400 ? 'warn' : 
                      'info';
      
      requestLogger[logLevel]('Response sent', responseLogData);
      
      // Call original end method
      originalEnd.apply(res, args);
    };
    
    next();
  };
}

// Export middleware factory and utilities
module.exports = {
  requestLogger,
  generateRequestId
};