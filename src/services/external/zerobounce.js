// src/services/external/zerobounce.js
import axios from 'axios';
import CircuitBreaker from 'opossum';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ExternalServiceError,
  ZeroBounceError,
  ErrorRecovery,
  TimeoutError
} from '../../core/errors.js';

const logger = createServiceLogger('zerobounce');

class ZeroBounceService {
  constructor() {
    this.logger = logger;
    // Use correct ZeroBounce API v2 base URL
    this.baseUrl = config.services.zeroBounce.baseUrl || 'https://api.zerobounce.net/v2';
    this.apiKey = config.services.zeroBounce.apiKey;
    this.timeout = config.services.zeroBounce.timeout || 10000;
    this.maxRetries = config.services.zeroBounce.retries || 2;
    
    // Log initialization (without exposing full API key)
    this.logger.info('ZeroBounce service initialized', {
      baseUrl: this.baseUrl,
      apiKeyConfigured: !!this.apiKey,
      apiKeyLength: this.apiKey ? this.apiKey.length : 0,
      apiKeyPrefix: this.apiKey ? this.apiKey.substring(0, 8) + '...' : 'not set',
      enabled: config.services.zeroBounce.enabled
    });
    
    // Initialize circuit breaker with Opossum
    this.circuitBreaker = new CircuitBreaker(this.executeRequest.bind(this), {
      name: 'ZeroBounce',
      timeout: 15000, // Time in ms before a request is considered failed
      errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
      resetTimeout: 60000, // Wait time before trying to close the circuit
      volumeThreshold: 5, // Minimum number of requests needed before tripping circuit
      rollingCountTimeout: 10000, // Time window for error rate calculation
      rollingCountBuckets: 10 // Number of buckets for stats tracking
    });
    
    // Add event listeners
    this.setupCircuitBreakerEvents();
    
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
  
  // Setup circuit breaker event handlers
  setupCircuitBreakerEvents() {
    this.circuitBreaker.on('open', () => {
      this.logger.warn('ZeroBounce circuit breaker opened');
    });
    
    this.circuitBreaker.on('halfOpen', () => {
      this.logger.info('ZeroBounce circuit breaker half-open, testing service');
    });
    
    this.circuitBreaker.on('close', () => {
      this.logger.info('ZeroBounce circuit breaker closed, service recovered');
    });
    
    this.circuitBreaker.on('fallback', (result) => {
      this.logger.warn('ZeroBounce circuit breaker fallback executed');
    });
    
    this.circuitBreaker.on('timeout', () => {
      this.logger.warn('ZeroBounce request timed out');
    });
    
    this.circuitBreaker.on('reject', () => {
      this.logger.warn('ZeroBounce request rejected (circuit open)');
    });
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
    if (!this.apiKey || this.apiKey.trim() === '') {
      this.logger.error('ZeroBounce API key is not configured or empty');
      throw new ZeroBounceError('ZeroBounce API key is not configured', 503);
    }
    
    // Check response cache first
    const cached = this.getFromResponseCache(email);
    if (cached) {
      this.logger.debug('ZeroBounce result from cache', { email });
      return cached;
    }
    
    try {
      // Execute through circuit breaker
      const result = await this.circuitBreaker.fire(email, ipAddress, timeout);
      
      // Cache the result
      this.addToResponseCache(email, result);
      
      return result;
    } catch (error) {
      // Check if error exists before accessing properties
      if (error && error.name === 'CircuitBreakerOpen') {
        throw new ZeroBounceError('ZeroBounce service is temporarily unavailable', 503);
      }
      
      // Handle null/undefined errors
      if (!error) {
        this.logger.error('Circuit breaker returned null error');
        throw new ZeroBounceError('ZeroBounce service error occurred', 500);
      }
      
      throw error;
    }
  }
  
  // Actual API request execution (wrapped by circuit breaker)
  async executeRequest(email, ipAddress, timeout) {
    try {
      // Call ZeroBounce API with retry logic
      return await ErrorRecovery.withRetry(async (attempt) => {
        this.logger.debug('Calling ZeroBounce API', {
          email,
          attempt,
          timeout: attempt === 1 ? timeout : timeout * 1.5
        });
        
        // Use longer timeout for retries
        const attemptTimeout = attempt === 1 ? timeout : timeout * 1.5;
        
        // Build the URL with query parameters
        const params = new URLSearchParams({
          api_key: this.apiKey,
          email: email
        });
        
        // Only add IP address if it's actually provided and not empty
        if (ipAddress && ipAddress.trim() !== '') {
          params.append('ip_address', ipAddress);
        }
        
        const url = `${this.baseUrl}/validate?${params.toString()}`;
        
        this.logger.debug('ZeroBounce validate URL constructed', {
          url: url.replace(this.apiKey, 'REDACTED'),
          email,
          hasIpAddress: !!(ipAddress && ipAddress.trim() !== '')
        });
        
        try {
          const response = await axios.get(url, {
            timeout: attemptTimeout,
            headers: {
              'Accept': 'application/json',
              'User-Agent': 'Unmessy-API/2.0'
            },
            validateStatus: (status) => status < 500 // Don't throw on 4xx errors
          });
          
          this.logger.debug('ZeroBounce API response received', {
            status: response.status,
            email,
            headers: response.headers
          });
          
          // Handle different status codes
          if (response.status === 400) {
            // Log the actual error message from ZeroBounce
            this.logger.error('ZeroBounce 400 error', {
              data: response.data,
              email
            });
            
            // Check for specific error messages
            if (response.data && response.data.error) {
              if (response.data.error.includes('Invalid email')) {
                throw new ZeroBounceError('Invalid email format', 400);
              } else if (response.data.error.includes('API key')) {
                throw new ZeroBounceError('Invalid API key', 401);
              } else {
                throw new ZeroBounceError(response.data.error, 400);
              }
            }
            
            throw new ZeroBounceError('Bad request to ZeroBounce API', 400);
          }
          
          if (response.status === 401) {
            throw new ZeroBounceError('Invalid API key', 401);
          }
          
          if (response.status === 429) {
            throw new ZeroBounceError('Rate limit exceeded', 429);
          }
          
          if (response.status >= 500) {
            throw new ZeroBounceError(`ZeroBounce service error: ${response.statusText}`, response.status);
          }
          
          // Check if we got a valid response
          if (!response.data || typeof response.data !== 'object') {
            this.logger.error('Invalid response structure', {
              data: response.data,
              type: typeof response.data
            });
            throw new ZeroBounceError('Invalid response from ZeroBounce API', 502);
          }
          
          // Return the formatted validation result
          return this.formatValidationResponse(response.data, email);
          
        } catch (error) {
          // Handle axios errors
          if (error.response) {
            // The request was made and the server responded with a status code
            const status = error.response.status;
            const data = error.response.data;
            
            this.logger.error('ZeroBounce API error response', {
              status,
              data,
              email,
              errorMessage: error.message
            });
            
            // Re-throw with proper error type
            throw error;
          } else if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT') {
            throw new TimeoutError('ZeroBounce request timed out', timeout);
          } else if (error.code === 'ENOTFOUND' || error.code === 'ECONNREFUSED') {
            throw new ExternalServiceError('Cannot connect to ZeroBounce API', 'zerobounce');
          } else if (error instanceof ZeroBounceError) {
            // Re-throw our custom errors
            throw error;
          } else {
            // Something else happened
            throw new ExternalServiceError(
              `ZeroBounce request failed: ${error.message}`,
              'zerobounce'
            );
          }
        }
      }, {
        maxAttempts: this.maxRetries,
        baseDelay: 1000,
        maxDelay: 5000,
        shouldRetry: (error) => {
          // Retry on timeout or 5xx errors
          return error instanceof TimeoutError ||
                 (error.statusCode && error.statusCode >= 500);
        }
      });
    } catch (error) {
      this.logger.error('ZeroBounce validation failed', error, { email });
      throw error;
    }
  }
  
  // Format the validation response from ZeroBounce
  formatValidationResponse(data, email) {
    // Handle case where data might be null or undefined
    if (!data) {
      return {
        email: email,
        status: 'unknown',
        sub_status: 'api_error',
        error: 'No response data from ZeroBounce'
      };
    }

    return {
      email: email,
      status: data.status || 'unknown',
      sub_status: data.sub_status || '',
      free_email: data.free_email || false,
      did_you_mean: data.did_you_mean || null,
      account: data.account || null,
      domain: data.domain || null,
      domain_age_days: data.domain_age_days || null,
      smtp_provider: data.smtp_provider || null,
      mx_record: data.mx_record || null,
      mx_found: data.mx_found || 'false',
      firstname: data.firstname || null,
      lastname: data.lastname || null,
      gender: data.gender || null,
      country: data.country || null,
      region: data.region || null,
      city: data.city || null,
      zipcode: data.zipcode || null,
      processed_at: data.processed_at || new Date().toISOString()
    };
  }
  
  // Check API credits
  async checkCredits() {
    // Check cache first
    if (this.creditsCache.credits !== null && 
        this.creditsCache.timestamp &&
        Date.now() - this.creditsCache.timestamp < this.creditsCache.ttl) {
      return { credits: this.creditsCache.credits };
    }
    
    try {
      const params = new URLSearchParams({
        api_key: this.apiKey
      });
      
      const url = `${this.baseUrl}/getcredits?${params.toString()}`;
      
      this.logger.debug('Checking ZeroBounce credits', {
        url: url.replace(this.apiKey, 'REDACTED')
      });
      
      const response = await axios.get(url, {
        timeout: 5000,
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'Unmessy-API/2.0'
        }
      });
      
      this.logger.debug('Credits response', {
        status: response.status,
        data: response.data
      });
      
      if (response.data && typeof response.data.Credits !== 'undefined') {
        // Update cache
        this.creditsCache.credits = response.data.Credits;
        this.creditsCache.timestamp = Date.now();
        
        this.logger.info('ZeroBounce credits checked', {
          credits: response.data.Credits
        });
        
        // Check for low credits
        if (response.data.Credits < 100) {
          this.logger.warn('ZeroBounce credits running low', {
            credits: response.data.Credits
          });
        }
        
        return { credits: response.data.Credits };
      }
      
      throw new ZeroBounceError('Invalid credits response', 502);
    } catch (error) {
      this.logger.error('Failed to check ZeroBounce credits', {
        error: error.message,
        response: error.response?.data
      });
      
      if (error.response?.status === 401) {
        throw new ZeroBounceError('Invalid API key', 401);
      }
      
      throw error;
    }
  }
  
  // Get from response cache
  getFromResponseCache(email) {
    const cached = this.responseCache.get(email.toLowerCase());
    
    if (!cached) return null;
    
    // Check if cache is still valid
    if (Date.now() - cached.timestamp > this.responseCacheTTL) {
      this.responseCache.delete(email.toLowerCase());
      return null;
    }
    
    return cached.data;
  }
  
  // Add to response cache
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
    return {
      state: this.circuitBreaker.status.name,
      stats: {
        successes: this.circuitBreaker.stats.successes,
        failures: this.circuitBreaker.stats.failures,
        rejects: this.circuitBreaker.stats.rejects,
        timeouts: this.circuitBreaker.stats.timeouts
      }
    };
  }
  
  // Health check
  async healthCheck() {
    try {
      const credits = await this.checkCredits();
      
      return {
        status: 'healthy',
        circuitBreaker: this.circuitBreaker.status.name,
        credits: credits.credits,
        cacheSize: this.responseCache.size,
        enabled: config.services.zeroBounce.enabled
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        circuitBreaker: this.circuitBreaker.status.name,
        error: error.message,
        errorCode: error.statusCode,
        enabled: config.services.zeroBounce.enabled
      };
    }
  }
}

// Create singleton instance
const zeroBounceService = new ZeroBounceService();

// Export both the instance and the class
export { zeroBounceService, ZeroBounceService };