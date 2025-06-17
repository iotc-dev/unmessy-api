// src/api/middleware/rate-limit.js
import { createServiceLogger } from '../../core/logger.js';
import { RateLimitError } from '../../core/errors.js';
import { config } from '../../core/config.js';
import clientService from '../../services/client-service.js';

// Create logger instance
const logger = createServiceLogger('rate-limit');

// In-memory store for IP-based rate limiting
const ipStore = new Map();

/**
 * Rate limiting middleware factory
 * Supports both client-based and IP-based rate limiting
 * 
 * @param {Object} options - Rate limit options
 * @param {string} options.type - Type of validation (email, name, phone, address)
 * @param {number} options.cost - How many requests this operation costs (default: 1)
 * @param {boolean} options.skipAuth - Skip authentication check (default: false)
 * @param {number} options.ipLimit - IP-based rate limit (requests per window)
 * @param {number} options.ipWindowMs - IP rate limit window in milliseconds
 * @returns {Function} Express middleware function
 */
function createRateLimitMiddleware(options = {}) {
  const {
    type = null,
    cost = 1,
    skipAuth = false,
    ipLimit = null,
    ipWindowMs = 60 * 1000 // 1 minute default
  } = options;
  
  return async (req, res, next) => {
    try {
      // IP-based rate limiting (if configured)
      if (ipLimit) {
        const clientIp = req.ip || req.connection.remoteAddress;
        const now = Date.now();
        const windowStart = now - ipWindowMs;
        
        // Get or create IP record
        let ipRecord = ipStore.get(clientIp);
        if (!ipRecord) {
          ipRecord = { requests: [], blocked: false };
          ipStore.set(clientIp, ipRecord);
        }
        
        // Clean old requests outside the window
        ipRecord.requests = ipRecord.requests.filter(timestamp => timestamp > windowStart);
        
        // Check if IP is currently blocked
        if (ipRecord.blocked && ipRecord.blockedUntil > now) {
          const retryAfter = Math.ceil((ipRecord.blockedUntil - now) / 1000);
          
          logger.warn('IP rate limit exceeded', {
            ip: clientIp,
            requests: ipRecord.requests.length,
            limit: ipLimit,
            retryAfter
          });
          
          res.set('X-RateLimit-Limit', ipLimit);
          res.set('X-RateLimit-Remaining', 0);
          res.set('X-RateLimit-Reset', new Date(ipRecord.blockedUntil).toISOString());
          res.set('Retry-After', retryAfter);
          
          return res.status(429).json({
            status: 'error',
            message: 'Too many requests from this IP address',
            retryAfter
          });
        }
        
        // Check if limit would be exceeded
        if (ipRecord.requests.length >= ipLimit) {
          // Block the IP for the window duration
          ipRecord.blocked = true;
          ipRecord.blockedUntil = now + ipWindowMs;
          
          const retryAfter = Math.ceil(ipWindowMs / 1000);
          
          logger.warn('IP rate limit exceeded', {
            ip: clientIp,
            requests: ipRecord.requests.length,
            limit: ipLimit,
            retryAfter
          });
          
          res.set('X-RateLimit-Limit', ipLimit);
          res.set('X-RateLimit-Remaining', 0);
          res.set('X-RateLimit-Reset', new Date(ipRecord.blockedUntil).toISOString());
          res.set('Retry-After', retryAfter);
          
          return res.status(429).json({
            status: 'error',
            message: 'Too many requests from this IP address',
            retryAfter
          });
        }
        
        // Record this request
        ipRecord.requests.push(now);
        
        // Set rate limit headers
        res.set('X-RateLimit-Limit', ipLimit);
        res.set('X-RateLimit-Remaining', ipLimit - ipRecord.requests.length);
        res.set('X-RateLimit-Reset', new Date(now + ipWindowMs).toISOString());
      }
      
      // Client-based rate limiting (if type is specified and auth is available)
      if (type && !skipAuth && req.clientId) {
        // Check rate limit for the specific validation type
        const rateLimitCheck = await clientService.checkRateLimit(req.clientId, type, cost);
        
        if (!rateLimitCheck.allowed) {
          // Calculate retry after (assuming daily reset at midnight)
          const now = new Date();
          const tomorrow = new Date(now);
          tomorrow.setDate(tomorrow.getDate() + 1);
          tomorrow.setHours(0, 0, 0, 0);
          const retryAfter = Math.ceil((tomorrow - now) / 1000);
          
          logger.warn('Client rate limit exceeded', {
            clientId: req.clientId,
            type,
            limit: rateLimitCheck.limit,
            used: rateLimitCheck.used,
            cost
          });
          
          throw new RateLimitError(
            type,
            rateLimitCheck.limit,
            rateLimitCheck.used,
            retryAfter
          );
        }
        
        // Add rate limit info to response headers
        res.set(`X-RateLimit-${type}-Limit`, rateLimitCheck.limit);
        res.set(`X-RateLimit-${type}-Remaining`, rateLimitCheck.remaining);
        res.set(`X-RateLimit-${type}-Used`, rateLimitCheck.used);
      }
      
      next();
    } catch (error) {
      next(error);
    }
  };
}

/**
 * Cleanup function for IP store (prevents memory leaks)
 * Should be called periodically
 */
function cleanupIpStore() {
  const now = Date.now();
  let cleaned = 0;
  
  for (const [ip, record] of ipStore.entries()) {
    // Remove IPs that haven't made requests in the last hour
    const lastRequest = Math.max(...record.requests, 0);
    if (now - lastRequest > 3600000) { // 1 hour
      ipStore.delete(ip);
      cleaned++;
    }
  }
  
  if (cleaned > 0) {
    logger.debug('Cleaned up IP store', { entriesRemoved: cleaned });
  }
}

// Run cleanup every 10 minutes
setInterval(cleanupIpStore, 10 * 60 * 1000);

/**
 * Pre-configured rate limit middlewares
 */
export const rateLimit = {
  // Email validation rate limit
  email: (options = {}) => createRateLimitMiddleware({
    type: 'email',
    cost: options.cost || 1,
    ...options
  }),
  
  // Name validation rate limit
  name: (options = {}) => createRateLimitMiddleware({
    type: 'name',
    cost: options.cost || 1,
    ...options
  }),
  
  // Phone validation rate limit
  phone: (options = {}) => createRateLimitMiddleware({
    type: 'phone',
    cost: options.cost || 1,
    ...options
  }),
  
  // Address validation rate limit
  address: (options = {}) => createRateLimitMiddleware({
    type: 'address',
    cost: options.cost || 1,
    ...options
  }),
  
  // IP-only rate limit (for public endpoints)
  ip: (options = {}) => createRateLimitMiddleware({
    ipLimit: options.ipLimit || 100,
    ipWindowMs: options.ipWindowMs || 60 * 1000,
    ...options
  }),
  
  // Combined client + IP rate limit
  combined: (type, options = {}) => createRateLimitMiddleware({
    type,
    ipLimit: options.ipLimit || 1000,
    ipWindowMs: options.ipWindowMs || 60 * 1000,
    ...options
  })
};

// Export default
export default rateLimit;