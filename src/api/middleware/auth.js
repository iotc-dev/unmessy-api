// src/api/middleware/auth.js
import { createServiceLogger } from '../../core/logger.js';
import { AuthenticationError, InvalidApiKeyError, InactiveClientError } from '../../core/errors.js';
import { config } from '../../core/config.js';
import clientService from '../../services/client-service.js';

// Create logger instance
const logger = createServiceLogger('auth-middleware');

// Default API key header name
const API_KEY_HEADER = 'x-api-key';

/**
 * Extract API key from request
 * @param {Object} req - Express request object
 * @returns {string|null} The API key or null if not found
 */
function extractApiKey(req) {
  // Check for API key in header (preferred method)
  // Use the default header name or get from config if available
  const headerName = config.clients?.apiKeyHeader || API_KEY_HEADER;
  const headerKey = req.headers[headerName.toLowerCase()];
  if (headerKey) {
    return headerKey;
  }
  
  // Also check for common variations
  if (req.headers['api-key']) {
    return req.headers['api-key'];
  }
  
  if (req.headers['apikey']) {
    return req.headers['apikey'];
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
        logger.warn('No API key provided', {
          path: req.path,
          method: req.method,
          headers: Object.keys(req.headers).filter(h => h.toLowerCase().includes('api'))
        });
        throw new AuthenticationError('API key is required. Please provide it in the X-API-Key header, api_key query parameter, or api_key in the request body.');
      }
      
      // If no API key provided but not required, continue
      if (!apiKey && !required) {
        logger.debug('No API key provided, but auth not required');
        return next();
      }
      
      // Validate API key and get client info
      if (!clientService || typeof clientService.validateApiKey !== 'function') {
        logger.error('Client service not properly initialized', {
          hasClientService: !!clientService,
          type: typeof clientService,
          methods: clientService ? Object.getOwnPropertyNames(Object.getPrototypeOf(clientService)) : []
        });
        throw new AuthenticationError('Service initialization error');
      }
      
      const { valid, clientId, error } = await clientService.validateApiKey(apiKey);
      
      if (!valid) {
        logger.warn('Invalid API key attempted', { 
          apiKey: apiKey ? apiKey.substring(0, 4) + '****' : 'none',
          error 
        });
        throw new InvalidApiKeyError(error || 'Invalid API key');
      }
      
      // Get client details
      const client = await clientService.getClient(clientId);
      
      if (!client) {
        logger.error('Client not found after successful API key validation', { clientId });
        throw new AuthenticationError('Client configuration error');
      }
      
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
export { authMiddleware, extractApiKey, API_KEY_HEADER };