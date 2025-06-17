// src/api/middleware/error-handler.js
import { createServiceLogger } from '../../core/logger.js';
import {
  AppError,
  ValidationError,
  AuthenticationError,
  AuthorizationError,
  RateLimitError,
  NotFoundError,
  DatabaseError,
  ExternalServiceError
} from '../../core/errors.js';
import { config } from '../../core/config.js';

// Create logger instance
const logger = createServiceLogger('error-handler');

/**
 * Async handler wrapper to avoid try/catch blocks in route handlers
 * 
 * @param {Function} fn - The async route handler function
 * @returns {Function} Express middleware function
 */
export const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

/**
 * Format error response for client
 * 
 * @param {Error} err - The error object
 * @param {boolean} includeDetails - Whether to include error details
 * @returns {Object} Formatted error response
 */
const formatErrorResponse = (err, includeDetails = false) => {
  // Base error response
  const errorResponse = {
    status: err.status || 'error',
    message: err.message || 'Internal server error'
  };
  
  // Add error code if available
  if (err.code) {
    errorResponse.code = err.code;
  }
  
  // Add validation errors if available
  if (err instanceof ValidationError && err.validationErrors) {
    errorResponse.errors = err.validationErrors;
  }
  
  // Add rate limit info if available
  if (err instanceof RateLimitError) {
    errorResponse.limit = err.limit;
    errorResponse.remaining = err.remaining;
    errorResponse.retryAfter = err.retryAfter;
  }
  
  // Add details in development mode
  if (includeDetails && !config.isProduction) {
    errorResponse.stack = err.stack;
    errorResponse.type = err.constructor.name;
    
    if (err.originalError) {
      errorResponse.originalError = {
        message: err.originalError.message,
        ...(err.originalError.code && { code: err.originalError.code })
      };
    }
  }
  
  return errorResponse;
};

/**
 * Global error handler middleware
 */
export const errorHandler = (err, req, res, next) => {
  // Default status code
  let statusCode = 500;
  
  // Determine status code based on error type
  if (err instanceof ValidationError) {
    statusCode = 400;
  } else if (err instanceof AuthenticationError) {
    statusCode = 401;
  } else if (err instanceof AuthorizationError) {
    statusCode = 403;
  } else if (err instanceof NotFoundError) {
    statusCode = 404;
  } else if (err instanceof RateLimitError) {
    statusCode = 429;
  } else if (err instanceof ExternalServiceError) {
    statusCode = 503;
  }
  
  // Use error's status code if available
  if (err.statusCode) {
    statusCode = err.statusCode;
  }
  
  // Log the error
  const logMethod = statusCode >= 500 ? 'error' : 'warn';
  const logContext = {
    statusCode,
    path: req.path,
    method: req.method,
    clientId: req.clientId
  };
  
  if (err instanceof AppError) {
    // For our custom errors, log with structured details
    logger[logMethod](`${err.name}: ${err.message}`, err, logContext);
  } else {
    // For unexpected errors, log the full error
    logger.error('Unexpected error', err, logContext);
  }
  
  // Set appropriate headers for certain errors
  if (err instanceof RateLimitError) {
    res.set('X-RateLimit-Limit', err.limit);
    res.set('X-RateLimit-Remaining', err.remaining);
    res.set('X-RateLimit-Reset', new Date(Date.now() + err.retryAfter * 1000).toISOString());
    res.set('Retry-After', err.retryAfter);
  }
  
  // Send response
  const includeDetails = !config.isProduction;
  res.status(statusCode).json(formatErrorResponse(err, includeDetails));
};

/**
 * Not found handler middleware
 */
export const notFoundHandler = (req, res, next) => {
  const err = new NotFoundError('Resource', req.originalUrl);
  next(err);
};

/**
 * Handle uncaught errors
 */
export const setupUncaughtErrorHandlers = () => {
  process.on('uncaughtException', (err) => {
    logger.error('Uncaught exception', err);
    
    // Give time for logging before exiting
    setTimeout(() => {
      process.exit(1);
    }, 1000);
  });
  
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled rejection', {
      reason,
      promise
    });
  });
};

// Export all utilities
export default {
  asyncHandler,
  errorHandler,
  notFoundHandler,
  setupUncaughtErrorHandlers
};