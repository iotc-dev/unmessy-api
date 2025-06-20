// src/api/middleware/auth.js
import { createServiceLogger } from '../../core/logger.js';
import { AuthenticationError, InvalidApiKeyError, InactiveClientError } from '../../core/errors.js';
import { config } from '../../core/config.js';
import clientService from '../../services/client-service.js';

// Create logger instance
const logger = createServiceLogger('auth-middleware');

/**
 * Extract API key from request
 * @param {Object} req - Express request object
 * @returns {string|null} The API key or null if not found
 */
function extractApiKey(req) {
  // Check for API key in header (preferred method)
  const headerKey = req.headers[config.clients.apiKeyHeader.toLowerCase()];
  if (headerKey) {
    return headerKey;
  }
  
  // Check for API key in query string (fallback)
  if (req.query && req.query.api_key) {
    return req.query.api_key;
  }
  
  // Check for API key in body (least preferred)
  if (req.body && req.body.api_key) {
    return req.body.api_key;
  }
  
  return null;
}

/**
 * Authentication middleware that validates API keys and attaches client info
 * to the request object
 * 
 * @param {Object} options - Middleware options
 * @param {boolean} options.required - Whether authentication is required (default: true)
 * @param {boolean} options.adminOnly - Whether only admin clients are allowed
 * @returns {Function} Express middleware function
 */
function authMiddleware(options = {}) {
  const { required = true, adminOnly = false } = options;
  
  return async (req, res, next) => {
    try {
      // Extract API key
      const apiKey = extractApiKey(req);
      
      // If auth is required and no API key is provided, reject
      if (required && !apiKey) {
        throw new AuthenticationError('API key is required');
      }
      
      // If no API key provided but not required, continue
      if (!apiKey && !required) {
        logger.debug('No API key provided, but auth not required');
        return next();
      }
      
      // Validate API key and get client info
      const { valid, clientId, error } = await clientService.validateApiKey(apiKey);
      
      if (!valid) {
        logger.warn('Invalid API key attempted', { 
          apiKey: apiKey.substring(0, 4) + '****', 
          error 
        });
        throw new InvalidApiKeyError(error || 'Invalid API key');
      }
      
      // Get client details
      const client = await clientService.getClient(clientId);
      
      // Check if client is active
      if (!client.active) {
        logger.warn('Inactive client attempted access', { clientId });
        throw new InactiveClientError(clientId);
      }
      
      // Check if admin access is required
      if (adminOnly && !client.is_admin) {
        logger.warn('Non-admin client attempted admin access', { clientId });
        throw new AuthenticationError('Admin access required');
      }
      
      // Attach client info to request
      req.clientId = clientId;
      req.client = client;
      
      // Add client info to logger context
      if (req.logger) {
        req.logger = req.logger.child({ clientId });
      }
      
      logger.debug('Client authenticated successfully', { clientId });
      next();
    } catch (error) {
      next(error);
    }
  };
}

// Export middleware factory and utilities
export { authMiddleware, extractApiKey };