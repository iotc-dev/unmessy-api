// src/core/errors.js

// Base custom error class
export class AppError extends Error {
  constructor(message, statusCode = 500, isOperational = true) {
    super(message);
    this.statusCode = statusCode;
    this.status = `${statusCode}`.startsWith('4') ? 'fail' : 'error';
    this.isOperational = isOperational;
    this.timestamp = new Date().toISOString();
    
    Error.captureStackTrace(this, this.constructor);
  }
  
  toJSON() {
    return {
      name: this.name,
      message: this.message,
      statusCode: this.statusCode,
      status: this.status,
      isOperational: this.isOperational,
      timestamp: this.timestamp,
      stack: this.stack
    };
  }
}

// Authentication errors
export class AuthenticationError extends AppError {
  constructor(message = 'Authentication failed') {
    super(message, 401, true);
    this.name = 'AuthenticationError';
  }
}

export class AuthorizationError extends AppError {
  constructor(message = 'Insufficient permissions') {
    super(message, 403, true);
    this.name = 'AuthorizationError';
  }
}

export class InvalidApiKeyError extends AuthenticationError {
  constructor(reason = 'Invalid API key') {
    super(reason);
    this.name = 'InvalidApiKeyError';
    this.reason = reason;
  }
}

export class InactiveClientError extends AuthorizationError {
  constructor(clientId) {
    super('Client account is inactive');
    this.name = 'InactiveClientError';
    this.clientId = clientId;
  }
}

// Validation errors
export class ValidationError extends AppError {
  constructor(message, validationErrors = []) {
    super(message, 400, true);
    this.name = 'ValidationError';
    this.validationErrors = validationErrors;
  }
}

export class InvalidInputError extends ValidationError {
  constructor(field, value, message) {
    super(message || `Invalid value for field: ${field}`);
    this.name = 'InvalidInputError';
    this.field = field;
    this.value = value;
  }
}

// Rate limiting errors
export class RateLimitError extends AppError {
  constructor(validationType, limit, used, remaining) {
    super('Rate limit exceeded', 429, true);
    this.name = 'RateLimitError';
    this.validationType = validationType;
    this.limit = limit;
    this.used = used;
    this.remaining = remaining;
    this.retryAfter = this.calculateRetryAfter();
  }
  
  calculateRetryAfter() {
    const now = new Date();
    const midnight = new Date(now);
    midnight.setHours(24, 0, 0, 0);
    return Math.ceil((midnight - now) / 1000); // seconds until midnight
  }
  
  toJSON() {
    return {
      ...super.toJSON(),
      validationType: this.validationType,
      limit: this.limit,
      used: this.used,
      remaining: this.remaining,
      retryAfter: this.retryAfter
    };
  }
}

// External service errors
export class ExternalServiceError extends AppError {
  constructor(service, message, originalError = null) {
    super(`External service error (${service}): ${message}`, 503, true);
    this.name = 'ExternalServiceError';
    this.service = service;
    this.originalError = originalError;
  }
}

export class ZeroBounceError extends ExternalServiceError {
  constructor(message, statusCode, originalError) {
    super('ZeroBounce', message, originalError);
    this.name = 'ZeroBounceError';
    this.apiStatusCode = statusCode;
  }
}

export class HubSpotError extends ExternalServiceError {
  constructor(message, statusCode, originalError) {
    super('HubSpot', message, originalError);
    this.name = 'HubSpotError';
    this.apiStatusCode = statusCode;
  }
}

export class OpenCageError extends ExternalServiceError {
  constructor(message, statusCode, originalError) {
    super('OpenCage', message, originalError);
    this.name = 'OpenCageError';
    this.apiStatusCode = statusCode;
  }
}

export class TwilioError extends ExternalServiceError {
  constructor(message, statusCode, originalError) {
    super('Twilio', message, originalError);
    this.name = 'TwilioError';
    this.apiStatusCode = statusCode;
  }
}

// Database errors
export class DatabaseError extends AppError {
  constructor(message, operation = null, originalError = null) {
    super(`Database error: ${message}`, 500, false);
    this.name = 'DatabaseError';
    this.operation = operation;
    this.originalError = originalError;
  }
}

export class DatabaseConnectionError extends DatabaseError {
  constructor(originalError) {
    super('Failed to connect to database', 'connection', originalError);
    this.name = 'DatabaseConnectionError';
  }
}

export class DatabaseTimeoutError extends DatabaseError {
  constructor(operation, timeout) {
    super(`Database operation timed out after ${timeout}ms`, operation);
    this.name = 'DatabaseTimeoutError';
    this.timeout = timeout;
  }
}

// Queue errors
export class QueueError extends AppError {
  constructor(message, operation = null) {
    super(`Queue error: ${message}`, 500, true);
    this.name = 'QueueError';
    this.operation = operation;
  }
}

export class QueueProcessingError extends QueueError {
  constructor(eventId, attempts, maxAttempts, originalError) {
    super(`Failed to process event ${eventId} after ${attempts} attempts`);
    this.name = 'QueueProcessingError';
    this.eventId = eventId;
    this.attempts = attempts;
    this.maxAttempts = maxAttempts;
    this.originalError = originalError;
  }
}

// Timeout errors
export class TimeoutError extends AppError {
  constructor(operation, timeoutMs) {
    super(`Operation '${operation}' timed out after ${timeoutMs}ms`, 504, true);
    this.name = 'TimeoutError';
    this.operation = operation;
    this.timeoutMs = timeoutMs;
  }
}

// Configuration errors
export class ConfigurationError extends AppError {
  constructor(message) {
    super(`Configuration error: ${message}`, 500, false);
    this.name = 'ConfigurationError';
  }
}

// Not found error
export class NotFoundError extends AppError {
  constructor(resource, identifier) {
    super(`${resource} not found${identifier ? `: ${identifier}` : ''}`, 404, true);
    this.name = 'NotFoundError';
    this.resource = resource;
    this.identifier = identifier;
  }
}

// Conflict error
export class ConflictError extends AppError {
  constructor(message, resource = null) {
    super(message, 409, true);
    this.name = 'ConflictError';
    this.resource = resource;
  }
}

// Error recovery utilities
export class ErrorRecovery {
  static async withRetry(
    operation, 
    options = {}
  ) {
    const {
      maxAttempts = 3,
      initialDelay = 1000,
      maxDelay = 10000,
      backoffMultiplier = 2,
      retryableErrors = [
        DatabaseConnectionError,
        DatabaseTimeoutError,
        ExternalServiceError,
        TimeoutError
      ],
      onRetry = null,
      context = {}
    } = options;
    
    let lastError;
    let delay = initialDelay;
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await operation(attempt);
      } catch (error) {
        lastError = error;
        
        // Check if error is retryable
        const isRetryable = retryableErrors.some(
          ErrorClass => error instanceof ErrorClass
        ) || (error.isOperational && error.statusCode >= 500);
        
        if (!isRetryable || attempt === maxAttempts) {
          throw error;
        }
        
        // Call retry callback if provided
        if (onRetry) {
          await onRetry(error, attempt, delay, context);
        }
        
        // Wait before retry
        await ErrorRecovery.sleep(delay);
        
        // Calculate next delay with exponential backoff
        delay = Math.min(delay * backoffMultiplier, maxDelay);
      }
    }
    
    throw lastError;
  }
  
  static async withTimeout(operation, timeoutMs, operationName = 'Operation') {
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => {
        reject(new TimeoutError(operationName, timeoutMs));
      }, timeoutMs);
    });
    
    return Promise.race([operation, timeoutPromise]);
  }
  
  static async withFallback(operation, fallbackOperation, shouldFallback = null) {
    try {
      return await operation();
    } catch (error) {
      if (shouldFallback && !shouldFallback(error)) {
        throw error;
      }
      return fallbackOperation(error);
    }
  }
  
  static sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Circuit breaker for external services
export class CircuitBreaker {
  constructor(options = {}) {
    this.name = options.name || 'CircuitBreaker';
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeout = options.resetTimeout || 60000; // 1 minute
    this.monitoringPeriod = options.monitoringPeriod || 10000; // 10 seconds
    
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
    this.failures = 0;
    this.lastFailureTime = null;
    this.nextAttempt = null;
    this.successCount = 0;
    this.failureCount = 0;
  }
  
  async execute(operation) {
    if (this.state === 'OPEN') {
      if (Date.now() < this.nextAttempt) {
        throw new ExternalServiceError(
          this.name,
          `Circuit breaker is OPEN. Service unavailable until ${new Date(this.nextAttempt).toISOString()}`
        );
      }
      this.state = 'HALF_OPEN';
    }
    
    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }
  
  onSuccess() {
    this.failures = 0;
    this.successCount++;
    
    if (this.state === 'HALF_OPEN') {
      this.state = 'CLOSED';
    }
  }
  
  onFailure() {
    this.failures++;
    this.failureCount++;
    this.lastFailureTime = Date.now();
    
    if (this.failures >= this.failureThreshold) {
      this.state = 'OPEN';
      this.nextAttempt = Date.now() + this.resetTimeout;
    }
  }
  
  getStatus() {
    return {
      name: this.name,
      state: this.state,
      failures: this.failures,
      successCount: this.successCount,
      failureCount: this.failureCount,
      lastFailureTime: this.lastFailureTime,
      nextAttempt: this.nextAttempt
    };
  }
}

// Error handler middleware
export const errorHandler = (logger) => (err, req, res, next) => {
  // Log error
  if (logger && req.logger) {
    req.logger.error('Request error', err, {
      url: req.url,
      method: req.method,
      ip: req.ip,
      userAgent: req.get('user-agent')
    });
  }
  
  // Set default error values
  let statusCode = err.statusCode || 500;
  let message = err.message || 'Internal server error';
  let status = err.status || 'error';
  
  // Handle specific error types
  if (err instanceof ValidationError) {
    return res.status(statusCode).json({
      status,
      message,
      errors: err.validationErrors
    });
  }
  
  if (err instanceof RateLimitError) {
    res.set('X-RateLimit-Limit', err.limit);
    res.set('X-RateLimit-Remaining', err.remaining);
    res.set('X-RateLimit-Reset', new Date(Date.now() + err.retryAfter * 1000).toISOString());
    res.set('Retry-After', err.retryAfter);
    
    return res.status(statusCode).json({
      status,
      message,
      limit: err.limit,
      used: err.used,
      remaining: err.remaining,
      retryAfter: err.retryAfter
    });
  }
  
  if (err instanceof AuthenticationError || err instanceof AuthorizationError) {
    return res.status(statusCode).json({
      status,
      message,
      reason: err.reason
    });
  }
  
  // Handle database errors
  if (err.code === 'ECONNREFUSED' || err.code === 'ETIMEDOUT') {
    statusCode = 503;
    message = 'Service temporarily unavailable';
  }
  
  // Handle Joi validation errors
  if (err.name === 'ValidationError' && err.details) {
    return res.status(400).json({
      status: 'fail',
      message: 'Validation error',
      errors: err.details.map(detail => ({
        field: detail.path.join('.'),
        message: detail.message
      }))
    });
  }
  
  // Don't leak error details in production
  if (process.env.NODE_ENV === 'production' && statusCode === 500) {
    message = 'Internal server error';
  }
  
  // Send error response
  res.status(statusCode).json({
    status,
    message,
    ...(process.env.NODE_ENV === 'development' && {
      error: err,
      stack: err.stack
    })
  });
};

// Async error wrapper
export const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// Export all error classes and utilities
export default {
  AppError,
  AuthenticationError,
  AuthorizationError,
  InvalidApiKeyError,
  InactiveClientError,
  ValidationError,
  InvalidInputError,
  RateLimitError,
  ExternalServiceError,
  ZeroBounceError,
  HubSpotError,
  OpenCageError,
  TwilioError,
  DatabaseError,
  DatabaseConnectionError,
  DatabaseTimeoutError,
  QueueError,
  QueueProcessingError,
  TimeoutError,
  ConfigurationError,
  NotFoundError,
  ConflictError,
  ErrorRecovery,
  CircuitBreaker,
  errorHandler,
  asyncHandler
};