// src/services/external/zerobounce.js
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ExternalServiceError,
  ZeroBounceError,
  CircuitBreaker,
  ErrorRecovery,
  TimeoutError
} from '../../core/errors.js';

const logger = createServiceLogger('zerobounce');

class ZeroBounceService {
  constructor() {
    this.logger = logger;
    this.baseUrl = config.services.zeroBounce.baseUrl;
    this.apiKey = config.services.zeroBounce.apiKey;
    this.timeout = config.services.zeroBounce.timeout;
    this.retryTimeout = config.services.zeroBounce.retryTimeout;
    this.maxRetries = config.services.zeroBounce.maxRetries;
    
    // Initialize circuit breaker
    this.circuitBreaker = new CircuitBreaker({
      name: 'ZeroBounce',
      failureThreshold: 5,
      resetTimeout: 60000, // 1 minute
      monitoringPeriod: 10000 // 10 seconds
    });
    
    // Cache for API credits (avoid repeated calls)
    this.creditsCache = {
      credits: null,
      timestamp: null,
      ttl: 5 * 60 * 1000 // 5 minutes
    };
    
    // Response cache for recent validations
    this.responseCache = new Map();
    this.responseCacheTTL = 60 * 1000; // 1 minute
    this.maxCacheSize = 1000;
  }
  
  // Validate email with ZeroBounce
  async validateEmail(email, options = {}) {
    const {
      ipAddress = '',
      timeout = this.timeout
    } = options;
    
    // Check if service is enabled
    if (!config.services.zeroBounce.enabled) {
      throw new ZeroBounceError('ZeroBounce service is not enabled', 503);
    }
    
    // Check if API key is configured
    if (!this.apiKey) {
      throw new ZeroBounceError('ZeroBounce API key not configured', 503);
    }
    
    // Check response cache first
    const cached = this.getFromResponseCache(email);
    if (cached) {
      this.logger.debug('Email found in response cache', { email });
      return cached;
    }
    
    // Execute with circuit breaker
    return this.circuitBreaker.execute(async () => {
      try {
        const result = await ErrorRecovery.withRetry(
          async (attempt) => {
            this.logger.debug('Calling ZeroBounce API', { 
              email, 
              attempt,
              timeout: attempt === 1 ? timeout : this.retryTimeout
            });
            
            // Use longer timeout for retries
            const attemptTimeout = attempt === 1 ? timeout : this.retryTimeout;
            
            const response = await this.makeApiCall('/validate', {
              api_key: this.apiKey,
              email: email,
              ip_address: ipAddress
            }, {
              timeout: attemptTimeout
            });
            
            return response;
          },
          {
            maxAttempts: this.maxRetries,
            delay: 1000,
            backoffMultiplier: 2,
            onRetry: (error, attempt) => {
              this.logger.warn('Retrying ZeroBounce validation', {
                email,
                attempt,
                error: error.message
              });
            }
          }
        );
        
        // Cache successful response
        this.addToResponseCache(email, result);
        
        return result;
      } catch (error) {
        this.logger.error('ZeroBounce validation failed', error, { email });
        
        // Transform to appropriate error type
        if (error instanceof TimeoutError) {
          throw new ZeroBounceError('Request timeout', 504, error);
        } else if (error.statusCode === 429) {
          throw new ZeroBounceError('Rate limit exceeded', 429, error);
        } else if (error.statusCode === 401) {
          throw new ZeroBounceError('Invalid API key', 401, error);
        } else {
          throw new ZeroBounceError(
            error.message || 'Validation failed',
            error.statusCode || 503,
            error
          );
        }
      }
    });
  }
  
  // Get email activity data
  async getEmailActivity(email) {
    if (!config.services.zeroBounce.enabled || !this.apiKey) {
      throw new ZeroBounceError('ZeroBounce service is not available', 503);
    }
    
    return this.circuitBreaker.execute(async () => {
      try {
        const response = await this.makeApiCall('/activity', {
          api_key: this.apiKey,
          email: email
        });
        
        return response;
      } catch (error) {
        this.logger.error('Failed to get email activity', error, { email });
        throw new ZeroBounceError(
          'Failed to get email activity',
          error.statusCode || 503,
          error
        );
      }
    });
  }
  
  // Get remaining API credits
  async getCredits() {
    // Check cache first
    if (this.creditsCache.credits !== null && 
        Date.now() - this.creditsCache.timestamp < this.creditsCache.ttl) {
      return this.creditsCache.credits;
    }
    
    if (!this.apiKey) {
      throw new ZeroBounceError('ZeroBounce API key not configured', 503);
    }
    
    try {
      const response = await this.makeApiCall('/getcredits', {
        api_key: this.apiKey
      }, {
        timeout: 5000 // Quick timeout for credit check
      });
      
      // Cache the result
      this.creditsCache = {
        credits: response.Credits || 0,
        timestamp: Date.now()
      };
      
      this.logger.info('ZeroBounce credits retrieved', { 
        credits: response.Credits 
      });
      
      return response.Credits;
    } catch (error) {
      this.logger.error('Failed to get ZeroBounce credits', error);
      // Don't throw - return cached value or null
      return this.creditsCache.credits;
    }
  }
  
  // Make API call with timeout and error handling
  async makeApiCall(endpoint, params, options = {}) {
    const {
      method = 'GET',
      timeout = this.timeout
    } = options;
    
    const url = new URL(`${this.baseUrl}${endpoint}`);
    
    // Add parameters to URL for GET requests
    if (method === 'GET' && params) {
      Object.keys(params).forEach(key => {
        if (params[key] !== undefined && params[key] !== null) {
          url.searchParams.append(key, params[key]);
        }
      });
    }
    
    const fetchOptions = {
      method,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Unmessy-API/1.0'
      }
    };
    
    // Add body for POST requests
    if (method === 'POST' && params) {
      fetchOptions.headers['Content-Type'] = 'application/json';
      fetchOptions.body = JSON.stringify(params);
    }
    
    try {
      const response = await ErrorRecovery.withTimeout(
        fetch(url.toString(), fetchOptions),
        timeout,
        `ZeroBounce ${endpoint}`
      );
      
      if (!response.ok) {
        const errorText = await response.text();
        throw {
          statusCode: response.status,
          message: errorText || response.statusText
        };
      }
      
      const data = await response.json();
      
      // Log successful API call
      this.logger.debug('ZeroBounce API call successful', {
        endpoint,
        status: response.status
      });
      
      return data;
    } catch (error) {
      if (error instanceof TimeoutError) {
        throw error;
      }
      
      this.logger.error('ZeroBounce API call failed', error, {
        endpoint,
        statusCode: error.statusCode
      });
      
      throw error;
    }
  }
  
  // Response cache management
  getFromResponseCache(email) {
    const cached = this.responseCache.get(email.toLowerCase());
    if (!cached) return null;
    
    // Check if expired
    if (Date.now() - cached.timestamp > this.responseCacheTTL) {
      this.responseCache.delete(email.toLowerCase());
      return null;
    }
    
    return cached.data;
  }
  
  addToResponseCache(email, data) {
    // Maintain cache size limit
    if (this.responseCache.size >= this.maxCacheSize) {
      // Remove oldest entry
      const firstKey = this.responseCache.keys().next().value;
      this.responseCache.delete(firstKey);
    }
    
    this.responseCache.set(email.toLowerCase(), {
      data,
      timestamp: Date.now()
    });
  }
  
  // Clear response cache
  clearResponseCache() {
    this.responseCache.clear();
    this.logger.info('ZeroBounce response cache cleared');
  }
  
  // Get circuit breaker state
  getCircuitBreakerState() {
    return this.circuitBreaker.state;
  }
  
  // Health check
  async healthCheck() {
    try {
      // Try to get credits as a health check
      const credits = await this.getCredits();
      
      return {
        status: 'healthy',
        circuitBreaker: this.circuitBreaker.state,
        credits: credits,
        cacheSize: this.responseCache.size,
        enabled: config.services.zeroBounce.enabled
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        circuitBreaker: this.circuitBreaker.state,
        error: error.message,
        enabled: config.services.zeroBounce.enabled
      };
    }
  }
  
  // Parse ZeroBounce response to standard format
  parseResponse(zbResponse) {
    // Map ZeroBounce status to our standard status
    const statusMap = {
      'valid': 'valid',
      'invalid': 'invalid',
      'catch-all': 'valid', // We consider catch-all as valid but flag it
      'unknown': 'unknown',
      'spamtrap': 'invalid',
      'abuse': 'invalid',
      'do_not_mail': 'invalid'
    };
    
    const status = statusMap[zbResponse.status] || 'unknown';
    
    return {
      email: zbResponse.address,
      status: status,
      sub_status: zbResponse.sub_status,
      free_email: zbResponse.free_email === true,
      role: zbResponse.role === true,
      catch_all: zbResponse.status === 'catch-all',
      disposable: zbResponse.disposable === true,
      toxic: zbResponse.toxic === true,
      do_not_mail: zbResponse.do_not_mail === true,
      score: zbResponse.smtp_score || null,
      did_you_mean: zbResponse.did_you_mean || null,
      
      // Additional metadata
      mx_found: zbResponse.mx_found === true,
      mx_record: zbResponse.mx_record,
      smtp_provider: zbResponse.smtp_provider,
      
      // Processing info
      processed_at: zbResponse.processed_at || new Date().toISOString(),
      
      // Raw response for debugging
      raw: zbResponse
    };
  }
}

// Create singleton instance
const zeroBounceService = new ZeroBounceService();

// Export both the instance and the class
export { zeroBounceService, ZeroBounceService };