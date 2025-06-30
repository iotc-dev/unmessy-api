// src/api/middleware/request-logger.js
import { Logger, createServiceLogger, sanitizeLogData } from '../../core/logger.js';
import { config } from '../../core/config.js';
import db from '../../core/db.js';

// Create logger instance
const baseLogger = createServiceLogger('http');

/**
 * Generates a unique request ID
 * @returns {string} UUID v4
 */
function generateRequestId() {
  // Generate a UUID v4 without external dependency
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
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
 * Determines if a request should be logged to database
 * @param {Object} req - Express request object
 * @returns {boolean} Whether to log this request to database
 */
function shouldLogToDatabase(req) {
  // Skip static assets and health checks from database logging
  const skipPaths = ['/health', '/ready', '/live', '/favicon.ico', '/robots.txt'];
  
  return !skipPaths.some(path => req.path.includes(path));
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
 * Save request to database
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {number} responseTime - Response time in milliseconds
 */
async function saveRequestToDatabase(req, res, responseTime) {
  try {
    // Skip if we shouldn't log to database
    if (!shouldLogToDatabase(req)) {
      return;
    }
    
    const clientInfo = getClientInfo(req);
    
    // Prepare request log data
    const logData = {
      request_id: req.requestId,
      client_id: clientInfo.clientId,
      endpoint: req.path,
      method: req.method,
      ip_address: clientInfo.ip,
      user_agent: req.get('user-agent') || null,
      status_code: res.statusCode,
      response_time_ms: responseTime,
      rate_limit_remaining: res.getHeader('X-RateLimit-Remaining') || null,
      rate_limit_total: res.getHeader('X-RateLimit-Limit') || null,
      error_type: res.locals.errorType || null,
      error_message: res.locals.errorMessage || null,
      created_at: new Date().toISOString()
    };
    
    // Add request body if appropriate
    if (shouldLogBody(req) && req.body && Object.keys(req.body).length > 0) {
      logData.request_body = req.body;
    }
    
    // Add response body if stored (for debugging)
    if (res.locals.responseBody) {
      logData.response_body = res.locals.responseBody;
    }
    
    // Insert into database
    await db.insert('api_request_logs', logData, { returning: false });
    
    baseLogger.debug('Request logged to database', { 
      requestId: req.requestId,
      endpoint: req.path,
      statusCode: res.statusCode 
    });
    
  } catch (error) {
    // Don't let database errors break the request flow
    baseLogger.error('Failed to log request to database', error, {
      requestId: req.requestId,
      endpoint: req.path
    });
  }
}

/**
 * Request logging middleware
 * 
 * @param {Object} options - Middleware options
 * @param {boolean} options.logBody - Whether to log request bodies
 * @param {boolean} options.logHeaders - Whether to log request headers
 * @param {number} options.bodyMaxLength - Maximum length for logged bodies
 * @param {boolean} options.logToDatabase - Whether to log to database
 * @returns {Function} Express middleware function
 */
export function requestLogger(options = {}) {
  const {
    logBody = true,
    logHeaders = config.isDevelopment,
    bodyMaxLength = 1000,
    logToDatabase = true
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
    const requestLoggerInstance = new baseLogger.constructor({
      ...clientInfo,
      requestId,
      method: req.method,
      path: req.path,
      service: 'http'
    });
    
    // Attach logger to request object for use in route handlers
    req.logger = requestLoggerInstance;
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
    
    requestLoggerInstance.info('Request received', logData);
    
    // Intercept response to log when finished
    const originalEnd = res.end;
    let responseEnded = false;
    
    res.end = async function(...args) {
      // Prevent multiple calls
      if (responseEnded) {
        return originalEnd.apply(res, args);
      }
      responseEnded = true;
      
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
      
      requestLoggerInstance[logLevel]('Response sent', responseLogData);
      
      // Save to database if enabled
      if (logToDatabase) {
        // Don't await to avoid blocking response
        saveRequestToDatabase(req, res, responseTime).catch(err => {
          baseLogger.error('Database logging failed', err);
        });
      }
      
      // Call original end method
      originalEnd.apply(res, args);
    };
    
    // Also intercept errors that bypass normal response flow
    res.on('error', (error) => {
      const responseTime = Date.now() - startTime;
      
      requestLoggerInstance.error('Response error', {
        requestId,
        method: req.method,
        url: req.originalUrl,
        error: error.message,
        responseTime,
        ...clientInfo
      });
      
      // Save error to database
      if (logToDatabase && !responseEnded) {
        res.locals.errorType = 'response_error';
        res.locals.errorMessage = error.message;
        saveRequestToDatabase(req, res, responseTime).catch(err => {
          baseLogger.error('Database logging failed', err);
        });
      }
    });
    
    next();
  };
}

/**
 * Error logging middleware - captures error details for database logging
 * Should be placed before the error handler middleware
 */
export function errorLogger() {
  return (err, req, res, next) => {
    // Store error details for database logging
    res.locals.errorType = err.name || 'UnknownError';
    res.locals.errorMessage = err.message || 'An unknown error occurred';
    
    // Pass to next error handler
    next(err);
  };
}

// Export utilities
export { generateRequestId };

// Export default
export default {
  requestLogger,
  errorLogger,
  generateRequestId
};