// src/core/errors.js
import CircuitBreaker from 'opossum';

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
  }
}

// External service errors
export class ExternalServiceError extends AppError {
  constructor(service, message, statusCode = 502) {
    super(`${service} service error: ${message}`, statusCode, true);
    this.name = 'ExternalServiceError';
    this.service = service;
  }
}

export class ZeroBounceError extends ExternalServiceError {
  constructor(message, statusCode = 502) {
    super('ZeroBounce', message, statusCode);
    this.name = 'ZeroBounceError';
  }
}

export class HubSpotError extends ExternalServiceError {
  constructor(message, statusCode = 502) {
    super('HubSpot', message, statusCode);
    this.name = 'HubSpotError';
  }
}

export class OpenCageError extends ExternalServiceError {
  constructor(message, statusCode = 502) {
    super('OpenCage', message, statusCode);
    this.name = 'OpenCageError';
  }
}

export class TwilioError extends ExternalServiceError {
  constructor(message, statusCode = 502) {
    super('Twilio', message, statusCode);
    this.name = 'TwilioError';
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
    super(`${resource} not found${identifier ? ` with ID: ${identifier}` : ''}`, 404, true);
    this.name = 'NotFoundError';
    this.resource = resource;
    this.identifier = identifier;
  }
}

// Conflict error
export class ConflictError extends AppError {
  constructor(resource, identifier) {
    super(`${resource} already exists${identifier ? ` with ID: ${identifier}` : ''}`, 409, true);
    this.name = 'ConflictError';
    this.resource = resource;
    this.identifier = identifier;
  }
}

// Error recovery utilities
export class ErrorRecovery {
  // Retry an operation with exponential backoff
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
  
  // Operation with timeout
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
  
  // Execute with fallback
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

// Error handler middleware
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
        field: detail.context.key,
        message: detail.message
      }))
    };
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
  // CircuitBreaker is removed and replaced with Opossum
  errorHandler,
  asyncHandler
};