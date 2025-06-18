// src/core/errors.js
import { createServiceLogger } from './logger.js';

const logger = createServiceLogger('errors');

/**
 * Base Error class for all custom errors
 * Named AppError to match existing imports throughout the project
 */
export class AppError extends Error {
  constructor(message, statusCode = 500, isOperational = true) {
    super(message);
    this.name = this.constructor.name;
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

// Alias for backward compatibility
export const BaseError = AppError;

/**
 * Authentication Error - for auth failures
 */
export class AuthenticationError extends AppError {
  constructor(message = 'Authentication failed') {
    super(message, 401, true);
    this.name = 'AuthenticationError';
  }
}

/**
 * Authorization Error - for permission failures
 */
export class AuthorizationError extends AppError {
  constructor(message = 'Insufficient permissions') {
    super(message, 403, true);
    this.name = 'AuthorizationError';
  }
}

/**
 * Invalid API Key Error - specific authentication error
 */
export class InvalidApiKeyError extends AuthenticationError {
  constructor(reason = 'Invalid API key') {
    super(reason);
    this.name = 'InvalidApiKeyError';
    this.reason = reason;
  }
}

/**
 * Inactive Client Error - specific authorization error
 */
export class InactiveClientError extends AuthorizationError {
  constructor(clientId) {
    super('Client account is inactive');
    this.name = 'InactiveClientError';
    this.clientId = clientId;
  }
}

/**
 * Validation Error - for input validation failures
 */
export class ValidationError extends AppError {
  constructor(message, validationErrors = []) {
    super(message, 400, true);
    this.name = 'ValidationError';
    this.validationErrors = validationErrors;
  }
}

/**
 * Invalid Input Error - specific validation error
 */
export class InvalidInputError extends ValidationError {
  constructor(field, value, message) {
    super(message || `Invalid value for field: ${field}`);
    this.name = 'InvalidInputError';
    this.field = field;
    this.value = value;
  }
}

/**
 * Rate Limit Error - for exceeding rate limits
 */
export class RateLimitError extends AppError {
  constructor(validationType, limit, used, remaining) {
    super('Rate limit exceeded', 429, true);
    this.name = 'RateLimitError';
    this.validationType = validationType;
    this.limit = limit;
    this.used = used;
    this.remaining = remaining;
    this.retryAfter = 3600; // Default to 1 hour
  }
}

/**
 * External Service Error - for third-party API failures
 */
export class ExternalServiceError extends AppError {
  constructor(service, message, statusCode = 502) {
    super(`${service} service error: ${message}`, statusCode, true);
    this.name = 'ExternalServiceError';
    this.service = service;
  }
}

/**
 * ZeroBounce specific error with error code support
 */
export class ZeroBounceError extends ExternalServiceError {
  constructor(message, statusCode = 502, code = null) {
    super('ZeroBounce', message, statusCode);
    this.name = 'ZeroBounceError';
    this.code = code;
  }
}

/**
 * HubSpot specific error
 */
export class HubSpotError extends ExternalServiceError {
  constructor(message, statusCode = 502) {
    super('HubSpot', message, statusCode);
    this.name = 'HubSpotError';
  }
}

/**
 * OpenCage specific error
 */
export class OpenCageError extends ExternalServiceError {
  constructor(message, statusCode = 502) {
    super('OpenCage', message, statusCode);
    this.name = 'OpenCageError';
  }
}

/**
 * Twilio specific error
 */
export class TwilioError extends ExternalServiceError {
  constructor(message, statusCode = 502) {
    super('Twilio', message, statusCode);
    this.name = 'TwilioError';
  }
}

/**
 * Database Error - for database operation failures
 */
export class DatabaseError extends AppError {
  constructor(message, operation = null, originalError = null) {
    super(`Database error: ${message}`, 500, false);
    this.name = 'DatabaseError';
    this.operation = operation;
    this.originalError = originalError;
  }
}

/**
 * Database Connection Error
 */
export class DatabaseConnectionError extends DatabaseError {
  constructor(originalError) {
    super('Failed to connect to database', 'connection', originalError);
    this.name = 'DatabaseConnectionError';
  }
}

/**
 * Database Timeout Error
 */
export class DatabaseTimeoutError extends DatabaseError {
  constructor(operation, timeout) {
    super(`Database operation timed out after ${timeout}ms`, operation);
    this.name = 'DatabaseTimeoutError';
    this.timeout = timeout;
  }
}

/**
 * Queue Error - for queue operation failures
 */
export class QueueError extends AppError {
  constructor(message, operation = null) {
    super(`Queue error: ${message}`, 500, true);
    this.name = 'QueueError';
    this.operation = operation;
  }
}

/**
 * Queue Processing Error
 */
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

/**
 * Timeout Error - for operation timeouts
 */
export class TimeoutError extends AppError {
  constructor(operation, timeoutMs) {
    super(`Operation '${operation}' timed out after ${timeoutMs}ms`, 504, true);
    this.name = 'TimeoutError';
    this.operation = operation;
    this.timeoutMs = timeoutMs;
  }
}

/**
 * Configuration Error - for missing or invalid configuration
 */
export class ConfigurationError extends AppError {
  constructor(message) {
    super(`Configuration error: ${message}`, 500, false);
    this.name = 'ConfigurationError';
  }
}

/**
 * Not Found Error - for missing resources
 */
export class NotFoundError extends AppError {
  constructor(resource, identifier) {
    super(`${resource} not found${identifier ? ` with ID: ${identifier}` : ''}`, 404, true);
    this.name = 'NotFoundError';
    this.resource = resource;
    this.identifier = identifier;
  }
}

/**
 * Conflict Error - for resource conflicts
 */
export class ConflictError extends AppError {
  constructor(resource, identifier) {
    super(`${resource} already exists${identifier ? ` with ID: ${identifier}` : ''}`, 409, true);
    this.name = 'ConflictError';
    this.resource = resource;
    this.identifier = identifier;
  }
}

/**
 * Error recovery utilities
 */
export class ErrorRecovery {
  /**
   * Retry an operation with exponential backoff
   * @param {Function} operation - The operation to retry
   * @param {number} maxRetries - Maximum number of retries
   * @param {number} initialDelayMs - Initial delay in milliseconds
   * @param {Function} shouldRetry - Function to determine if should retry based on error
   * @returns {Promise} The result of the operation
   */
  static async withRetry(operation, maxRetries = 3, initialDelayMs = 500, shouldRetry = null) {
    let lastError = null;
    let delay = initialDelayMs;
    
    for (let attempt = 1; attempt <= maxRetries + 1; attempt++) {
      try {
        return await operation(attempt);
      } catch (error) {
        lastError = error;
        
        // Don't retry if we've reached max attempts or if shouldRetry returns false
        if (attempt > maxRetries || (shouldRetry && !shouldRetry(error))) {
          throw error;
        }
        
        // Wait before retrying
        await ErrorRecovery.sleep(delay);
        
        // Exponential backoff
        delay *= 2;
      }
    }
    
    throw lastError;
  }
  
  /**
   * Execute operation with timeout
   * @param {Promise} promise - The promise to timeout
   * @param {number} timeoutMs - Timeout in milliseconds
   * @param {string} operation - Operation name for error message
   * @returns {Promise} The result or timeout error
   */
  static async withTimeout(promise, timeoutMs, operation = 'unknown') {
    let timeoutId;
    
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new TimeoutError(operation, timeoutMs));
      }, timeoutMs);
    });
    
    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      clearTimeout(timeoutId);
    }
  }
  
  /**
   * Execute with fallback
   * @param {Function} operation - Primary operation to try
   * @param {Function} fallbackOperation - Fallback operation if primary fails
   * @param {Function} shouldFallback - Function to determine if should use fallback
   * @returns {Promise} Result from either primary or fallback operation
   */
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
  
  /**
   * Sleep for specified milliseconds
   * @param {number} ms - Milliseconds to sleep
   * @returns {Promise} Promise that resolves after timeout
   */
  static sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

/**
 * Error handler middleware for Express
 */
export const errorHandler = (err, req, res, next) => {
  let error = err;
  let statusCode = error.statusCode || 500;
  let status = error.status || 'error';
  let message = error.message || 'Something went wrong';
  
  // Handle Joi validation errors
  if (error.name === 'ValidationError' && error.details) {
    statusCode = 400;
    status = 'fail';
    message = 'Invalid input data';
    
    error = {
      ...error,
      validationErrors: error.details.map(detail => ({
        field: detail.path.join('.'),
        type: detail.type,
        message: detail.message
      }))
    };
  }
  
  // Don't leak error details in production
  if (process.env.NODE_ENV === 'production' && statusCode === 500) {
    message = 'Internal server error';
  }
  
  // Log error
  if (statusCode >= 500) {
    logger.error('Server error occurred', error, {
      statusCode,
      path: req.path,
      method: req.method,
      ip: req.ip,
      userAgent: req.get('user-agent')
    });
  }
  
  // Send error response
  res.status(statusCode).json({
    status,
    message,
    ...(error.code && { code: error.code }),
    ...(error.validationErrors && { errors: error.validationErrors }),
    ...(process.env.NODE_ENV === 'development' && {
      error: err,
      stack: err.stack
    })
  });
};

/**
 * Async handler wrapper for Express routes
 */
export const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

/**
 * Global error handlers
 */
export function setupGlobalErrorHandlers() {
  // Handle unhandled promise rejections
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Promise Rejection', reason, {
      promise: promise.toString()
    });
    
    // In production, we might want to exit
    if (process.env.NODE_ENV === 'production') {
      // Give time to log before exiting
      setTimeout(() => {
        process.exit(1);
      }, 1000);
    }
  });
  
  // Handle uncaught exceptions
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception', error);
    
    // Always exit on uncaught exception
    setTimeout(() => {
      process.exit(1);
    }, 1000);
  });
  
  // Handle warnings
  process.on('warning', (warning) => {
    logger.warn('Process warning', {
      name: warning.name,
      message: warning.message,
      stack: warning.stack
    });
  });
}

// Export all error classes and utilities
export default {
  AppError,
  BaseError,
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
  errorHandler,
  asyncHandler,
  setupGlobalErrorHandlers
};