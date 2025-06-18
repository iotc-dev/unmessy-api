// src/core/errors.js
import { createServiceLogger } from './logger.js';

const logger = createServiceLogger('errors');

/**
 * Base Error class for all custom errors
 */
export class BaseError extends Error {
  constructor(message, statusCode = 500, isOperational = true) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    this.isOperational = isOperational;
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Validation Error - for input validation failures
 */
export class ValidationError extends BaseError {
  constructor(message, statusCode = 400) {
    super(message, statusCode);
    this.type = 'validation';
  }
}

/**
 * Authentication Error - for auth failures
 */
export class AuthenticationError extends BaseError {
  constructor(message = 'Authentication failed', statusCode = 401) {
    super(message, statusCode);
    this.type = 'authentication';
  }
}

/**
 * Authorization Error - for permission failures
 */
export class AuthorizationError extends BaseError {
  constructor(message = 'Access denied', statusCode = 403) {
    super(message, statusCode);
    this.type = 'authorization';
  }
}

/**
 * Not Found Error - for missing resources
 */
export class NotFoundError extends BaseError {
  constructor(resource, identifier) {
    super(`${resource} not found${identifier ? `: ${identifier}` : ''}`, 404);
    this.type = 'not_found';
    this.resource = resource;
    this.identifier = identifier;
  }
}

/**
 * Rate Limit Error - for exceeding rate limits
 */
export class RateLimitError extends BaseError {
  constructor(message = 'Rate limit exceeded', retryAfter = null) {
    super(message, 429);
    this.type = 'rate_limit';
    this.retryAfter = retryAfter;
  }
}

/**
 * Database Error - for database operation failures
 */
export class DatabaseError extends BaseError {
  constructor(message, originalError = null) {
    super(message, 500);
    this.type = 'database';
    this.originalError = originalError;
  }
}

/**
 * External Service Error - for third-party API failures
 */
export class ExternalServiceError extends BaseError {
  constructor(service, message, statusCode = 503) {
    super(`${service} service error: ${message}`, statusCode);
    this.type = 'external_service';
    this.service = service;
  }
}

/**
 * Configuration Error - for missing or invalid configuration
 */
export class ConfigurationError extends BaseError {
  constructor(message) {
    super(`Configuration error: ${message}`, 500);
    this.type = 'configuration';
    this.isOperational = false; // Config errors are not operational
  }
}

/**
 * Timeout Error - for operation timeouts
 */
export class TimeoutError extends BaseError {
  constructor(operation, timeout) {
    super(`Operation timed out: ${operation} (${timeout}ms)`, 504);
    this.type = 'timeout';
    this.operation = operation;
    this.timeout = timeout;
  }
}

/**
 * ZeroBounce specific error with error code support
 */
export class ZeroBounceError extends ExternalServiceError {
  constructor(message, statusCode = 500, code = null) {
    super('ZeroBounce', message, statusCode);
    this.code = code;
  }
}

/**
 * OpenCage specific error
 */
export class OpenCageError extends ExternalServiceError {
  constructor(message, statusCode = 500) {
    super('OpenCage', message, statusCode);
  }
}

/**
 * HubSpot specific error
 */
export class HubSpotError extends ExternalServiceError {
  constructor(message, statusCode = 500) {
    super('HubSpot', message, statusCode);
  }
}

/**
 * Queue Processing Error
 */
export class QueueError extends BaseError {
  constructor(message, statusCode = 500) {
    super(`Queue error: ${message}`, statusCode);
    this.type = 'queue';
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
   * @param {number} initialDelay - Initial delay in milliseconds
   * @param {Function} shouldRetry - Function to determine if should retry based on error
   * @returns {Promise} The result of the operation
   */
  static async withRetry(operation, maxRetries = 3, initialDelay = 1000, shouldRetry = () => true) {
    let lastError;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation(attempt);
      } catch (error) {
        lastError = error;
        
        // Check if we should retry
        if (attempt === maxRetries || !shouldRetry(error)) {
          throw error;
        }
        
        // Calculate delay with exponential backoff
        const delay = initialDelay * Math.pow(2, attempt - 1);
        
        logger.debug(`Retrying operation after ${delay}ms`, {
          attempt,
          maxRetries,
          error: error.message
        });
        
        // Wait before retrying
        await new Promise(resolve => setTimeout(resolve, delay));
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
  static async withTimeout(promise, timeoutMs, operation = 'Operation') {
    let timeoutId;
    
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new TimeoutError(operation, timeoutMs));
      }, timeoutMs);
    });
    
    try {
      const result = await Promise.race([promise, timeoutPromise]);
      clearTimeout(timeoutId);
      return result;
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  }
  
  /**
   * Circuit breaker pattern implementation
   * @param {Function} operation - The operation to protect
   * @param {Object} options - Circuit breaker options
   * @returns {Function} Protected operation
   */
  static createCircuitBreaker(operation, options = {}) {
    const {
      threshold = 5,
      timeout = 60000,
      resetTimeout = 30000
    } = options;
    
    let failures = 0;
    let lastFailureTime = null;
    let state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
    
    return async function(...args) {
      // Check if circuit should be reset
      if (state === 'OPEN' && Date.now() - lastFailureTime > resetTimeout) {
        state = 'HALF_OPEN';
        failures = 0;
      }
      
      // If circuit is open, fail fast
      if (state === 'OPEN') {
        throw new ExternalServiceError('Circuit breaker', 'Circuit is open', 503);
      }
      
      try {
        const result = await ErrorRecovery.withTimeout(
          operation(...args),
          timeout,
          'Circuit breaker operation'
        );
        
        // Reset on success
        if (state === 'HALF_OPEN') {
          state = 'CLOSED';
          failures = 0;
        }
        
        return result;
      } catch (error) {
        failures++;
        lastFailureTime = Date.now();
        
        // Open circuit if threshold reached
        if (failures >= threshold) {
          state = 'OPEN';
          logger.warn('Circuit breaker opened', {
            failures,
            threshold,
            error: error.message
          });
        }
        
        throw error;
      }
    };
  }
}

/**
 * Error handler middleware
 */
export function handleError(error, req = null, res = null) {
  // Log error
  logger.error('Error occurred', error, {
    isOperational: error.isOperational,
    statusCode: error.statusCode,
    type: error.type,
    path: req?.path,
    method: req?.method
  });
  
  // If not operational, we should probably exit
  if (!error.isOperational) {
    logger.error('Non-operational error detected, consider restarting', error);
  }
  
  // If we have a response object, send error response
  if (res && !res.headersSent) {
    const statusCode = error.statusCode || 500;
    const message = error.isOperational ? error.message : 'Internal server error';
    
    res.status(statusCode).json({
      success: false,
      error: {
        message,
        type: error.type || 'unknown',
        ...(error.retryAfter && { retryAfter: error.retryAfter }),
        ...(process.env.NODE_ENV === 'development' && { stack: error.stack })
      }
    });
  }
  
  return error;
}

/**
 * Async error wrapper for Express routes
 */
export function asyncHandler(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

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
  BaseError,
  ValidationError,
  AuthenticationError,
  AuthorizationError,
  NotFoundError,
  RateLimitError,
  DatabaseError,
  ExternalServiceError,
  ConfigurationError,
  TimeoutError,
  ZeroBounceError,
  OpenCageError,
  HubSpotError,
  QueueError,
  ErrorRecovery,
  handleError,
  asyncHandler,
  setupGlobalErrorHandlers
};