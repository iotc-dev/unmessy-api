// src/core/logger.js
import winston from 'winston';
import { format } from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';

// Custom format for structured logging
const structuredFormat = format.printf(({ 
  timestamp, 
  level, 
  message, 
  service,
  requestId,
  clientId,
  duration,
  error,
  ...meta 
}) => {
  const log = {
    timestamp,
    level,
    service: service || 'unmessy-api',
    message,
    ...(requestId && { requestId }),
    ...(clientId && { clientId }),
    ...(duration && { duration }),
    ...(error && { 
      error: {
        message: error.message || error,
        stack: error.stack,
        code: error.code,
        type: error.type || error.name
      }
    }),
    ...meta
  };
  
  return JSON.stringify(log);
});

// Create the logger instance
const createLogger = () => {
  const transports = [];
  
  // Console transport (always enabled)
  transports.push(
    new winston.transports.Console({
      format: format.combine(
        format.timestamp(),
        format.errors({ stack: true }),
        format.colorize(),
        format.simple()
      )
    })
  );
  
  // File transports for production
  if (process.env.NODE_ENV === 'production') {
    // Error logs
    transports.push(
      new DailyRotateFile({
        filename: 'logs/error-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        level: 'error',
        maxSize: '20m',
        maxFiles: '14d',
        format: format.combine(
          format.timestamp(),
          format.errors({ stack: true }),
          structuredFormat
        )
      })
    );
    
    // Combined logs
    transports.push(
      new DailyRotateFile({
        filename: 'logs/combined-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        maxSize: '20m',
        maxFiles: '7d',
        format: format.combine(
          format.timestamp(),
          format.errors({ stack: true }),
          structuredFormat
        )
      })
    );
  }
  
  return winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: format.combine(
      format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss.SSS'
      }),
      format.errors({ stack: true }),
      structuredFormat
    ),
    transports,
    exitOnError: false
  });
};

// Create the main logger instance
const logger = createLogger();

// Specialized loggers for different contexts
class Logger {
  constructor(context = {}) {
    this.context = context;
    this.logger = logger;
  }
  
  // Add context to all logs
  child(additionalContext) {
    return new Logger({
      ...this.context,
      ...additionalContext
    });
  }
  
  // Log methods with context
  debug(message, meta = {}) {
    this.logger.debug(message, { ...this.context, ...meta });
  }
  
  info(message, meta = {}) {
    this.logger.info(message, { ...this.context, ...meta });
  }
  
  warn(message, meta = {}) {
    this.logger.warn(message, { ...this.context, ...meta });
  }
  
  error(message, error, meta = {}) {
    this.logger.error(message, {
      ...this.context,
      ...meta,
      error: error instanceof Error ? {
        message: error.message,
        stack: error.stack,
        code: error.code,
        type: error.constructor.name
      } : error
    });
  }
  
  // Performance logging
  startTimer() {
    return Date.now();
  }
  
  endTimer(startTime, message, meta = {}) {
    const duration = Date.now() - startTime;
    this.info(message, {
      ...meta,
      duration,
      durationMs: duration
    });
    return duration;
  }
  
  // API request logging
  logRequest(req, additionalMeta = {}) {
    this.info('API Request', {
      method: req.method,
      url: req.url,
      path: req.path,
      query: req.query,
      ip: req.ip || req.connection.remoteAddress,
      userAgent: req.get('user-agent'),
      ...additionalMeta
    });
  }
  
  // API response logging
  logResponse(req, res, startTime, additionalMeta = {}) {
    const duration = Date.now() - startTime;
    const level = res.statusCode >= 400 ? 'warn' : 'info';
    
    this[level]('API Response', {
      method: req.method,
      url: req.url,
      statusCode: res.statusCode,
      duration,
      durationMs: duration,
      ...additionalMeta
    });
  }
  
  // Validation logging
  logValidation(type, input, result, duration) {
    this.info(`${type} validation completed`, {
      validationType: type,
      input: type === 'email' ? input : undefined, // Only log email for privacy
      status: result.status,
      wasCorrected: result.wasCorrected,
      duration,
      durationMs: duration,
      recheckNeeded: result.recheckNeeded
    });
  }
  
  // External API logging
  logExternalAPI(service, operation, success, duration, error = null) {
    const level = success ? 'info' : 'warn';
    this[level](`External API ${operation}`, {
      service,
      operation,
      success,
      duration,
      durationMs: duration,
      ...(error && { error: error.message })
    });
  }
  
  // Database operation logging
  logDatabaseOperation(operation, table, success, duration, error = null) {
    const level = success ? 'debug' : 'error';
    this[level](`Database ${operation}`, {
      operation,
      table,
      success,
      duration,
      durationMs: duration,
      ...(error && { error: error.message })
    });
  }
  
  // Queue operation logging
  logQueueOperation(operation, data) {
    this.info(`Queue ${operation}`, {
      queueOperation: operation,
      eventId: data.eventId,
      eventType: data.eventType,
      attempts: data.attempts,
      status: data.status
    });
  }
  
  // Rate limit logging
  logRateLimit(clientId, validationType, remaining, limit) {
    const usage = ((limit - remaining) / limit * 100).toFixed(2);
    const level = usage > 90 ? 'warn' : 'info';
    
    this[level]('Rate limit check', {
      clientId,
      validationType,
      remaining,
      limit,
      usagePercent: parseFloat(usage)
    });
  }
  
  // Performance metrics logging
  logMetrics(metrics) {
    this.info('Performance metrics', {
      metrics,
      timestamp: new Date().toISOString()
    });
  }
}

// Middleware for request logging
export const requestLogger = (req, res, next) => {
  const startTime = Date.now();
  const requestId = req.id || generateRequestId();
  
  // Create logger with request context
  req.logger = new Logger({
    requestId,
    clientId: req.clientId,
    service: 'api'
  });
  
  // Log the request
  req.logger.logRequest(req);
  
  // Override res.end to log response
  const originalEnd = res.end;
  res.end = function(...args) {
    req.logger.logResponse(req, res, startTime, {
      responseSize: res.get('content-length')
    });
    originalEnd.apply(res, args);
  };
  
  next();
};

// Generate unique request ID
function generateRequestId() {
  return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

// Error serializer for better error logging
export const errorSerializer = (error) => {
  if (!error || !error.stack) {
    return error;
  }
  
  return {
    message: error.message,
    name: error.name,
    stack: error.stack,
    code: error.code,
    statusCode: error.statusCode,
    isOperational: error.isOperational,
    validationErrors: error.validationErrors,
    context: error.context
  };
};

// Create specialized loggers
export const createServiceLogger = (serviceName) => {
  return new Logger({ service: serviceName });
};

// Pre-configured loggers for different services
export const loggers = {
  api: createServiceLogger('api'),
  validation: createServiceLogger('validation'),
  queue: createServiceLogger('queue'),
  database: createServiceLogger('database'),
  external: createServiceLogger('external-api'),
  auth: createServiceLogger('auth'),
  metrics: createServiceLogger('metrics')
};

// Log uncaught exceptions and unhandled rejections
if (process.env.NODE_ENV === 'production') {
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception', {
      error: errorSerializer(error),
      fatal: true
    });
    // Give time for logs to be written
    setTimeout(() => process.exit(1), 1000);
  });
  
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection', {
      reason,
      promise,
      error: reason instanceof Error ? errorSerializer(reason) : reason
    });
  });
}

// Export the main logger and Logger class
export default logger;
export { Logger };

// Utility function to sanitize sensitive data before logging
export const sanitizeLogData = (data) => {
  const sensitiveFields = [
    'password', 'api_key', 'apiKey', 'token', 
    'authorization', 'credit_card', 'ssn', 'secret'
  ];
  
  const sanitized = { ...data };
  
  for (const field of sensitiveFields) {
    if (sanitized[field]) {
      sanitized[field] = '[REDACTED]';
    }
  }
  
  return sanitized;
};

// Performance monitoring helper
export class PerformanceMonitor {
  constructor(logger) {
    this.logger = logger;
    this.metrics = new Map();
  }
  
  startOperation(operationName) {
    const startTime = Date.now();
    this.metrics.set(operationName, { startTime });
    return startTime;
  }
  
  endOperation(operationName, metadata = {}) {
    const metric = this.metrics.get(operationName);
    if (!metric) return;
    
    const duration = Date.now() - metric.startTime;
    this.metrics.delete(operationName);
    
    this.logger.info(`Operation completed: ${operationName}`, {
      operation: operationName,
      duration,
      durationMs: duration,
      ...metadata
    });
    
    return duration;
  }
  
  recordMetric(name, value, metadata = {}) {
    this.logger.info('Metric recorded', {
      metric: name,
      value,
      ...metadata
    });
  }
}

// Correlation ID middleware
export const correlationIdMiddleware = (req, res, next) => {
  const correlationId = req.headers['x-correlation-id'] || generateRequestId();
  req.correlationId = correlationId;
  res.set('X-Correlation-ID', correlationId);
  
  // Add to logger context
  if (req.logger) {
    req.logger = req.logger.child({ correlationId });
  }
  
  next();
};

// Export convenience methods
export const log = {
  debug: (message, meta) => logger.debug(message, meta),
  info: (message, meta) => logger.info(message, meta),
  warn: (message, meta) => logger.warn(message, meta),
  error: (message, error, meta) => logger.error(message, { error: errorSerializer(error), ...meta })
};