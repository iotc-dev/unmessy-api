// src/services/external/zerobounce.js
import CircuitBreaker from 'opossum';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ZeroBounceError, 
  TimeoutError, 
  ValidationError, 
  ErrorRecovery 
} from '../../core/errors.js';
import { config } from '../../core/config.js';

// Create logger instance
const logger = createServiceLogger('zerobounce-service');

// Constants
const API_BASE_URL = 'https://api.zerobounce.net/v2';

/**
 * ZeroBounce email validation service
 * Provides email deliverability checking via ZeroBounce API
 */
class ZeroBounceService {
  constructor() {
    this.logger = logger;
    this.baseUrl = API_BASE_URL;
    this.apiKey = config.services.zeroBounce.apiKey;
    this.timeout = config.services.zeroBounce.timeout || 6000;
    this.retryTimeout = config.services.zeroBounce.retryTimeout || 8000;
    this.maxRetries = config.services.zeroBounce.maxRetries || 3;
    
    // Initialize circuit breaker with Opossum
    this.circuitBreaker = new CircuitBreaker(this.executeRequest.bind(this), {
      name: 'ZeroBounce',
      timeout: 10000, // Time in ms before a request is considered failed
      errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
      resetTimeout: 60000, // Wait time before trying to close the circuit
      volumeThreshold: 5, // Minimum number of requests needed before tripping circuit
      rollingCountTimeout: 10000, // Time window for error rate calculation
      rollingCountBuckets: 10 // Number of buckets for stats tracking
    });
    
    // Add event listeners
    this.setupCircuitBreakerEvents();
    
    // Credit balance cache
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
    if (!this.apiKey) {
      throw new ZeroBounceError('No API key configured', 401);
    }
    
    // Check response cache first
    const cached = this.getFromResponseCache(email);
    if (cached) {
      this.logger.debug('Returning cached ZeroBounce response', { email });
      return cached;
    }
    
    try {
      // Execute with circuit breaker
      return await this.circuitBreaker.fire(email, ipAddress, timeout);
    } catch (error) {
      // If circuit is open, return a default response
      if (this.circuitBreaker.opened) {
        throw new ZeroBounceError('Service temporarily unavailable', 503);
      }
      throw error;
    }
  }
  
  // Execute the actual API request
  async executeRequest(email, ipAddress, timeout) {
    try {
      return await ErrorRecovery.withRetry(
        async (attempt) => {
          this.logger.debug('Attempting ZeroBounce validation', {
            email,
            attempt,
            timeout: attempt === 1 ? timeout : this.retryTimeout
          });
          
          // Use longer timeout for retries
          const attemptTimeout = attempt === 1 ? timeout : this.retryTimeout;
          
          const params = {
            api_key: this.apiKey,
            email,
            ip_address: ipAddress || ''
          };
          
          // Fix: Use '/validate' instead of '/api/validate'
          const url = new URL('/validate', this.baseUrl);
          
          // Add params to URL
          Object.keys(params).forEach(key => {
            url.searchParams.append(key, params[key]);
          });
          
          // Execute API call with timeout
          const response = await ErrorRecovery.withTimeout(
            fetch(url.toString()),
            attemptTimeout,
            `ZeroBounce validate email: ${email}`
          );
          
          if (!response.ok) {
            const errorText = await response.text();
            throw new ZeroBounceError(
              `API error: ${response.status} ${errorText || response.statusText}`,
              response.status
            );
          }
          
          const data = await response.json();
          
          if (!data) {
            throw new ZeroBounceError('Invalid response from ZeroBounce API');
          }
          
          // Format response
          return this.formatValidationResponse(data, email);
        },
        this.maxRetries,
        500, // Initial delay in ms
        (error) => {
          // Only retry on network errors or 5xx errors
          return (
            error.message?.includes('network') ||
            error.statusCode >= 500 ||
            error instanceof TimeoutError
          );
        }
      );
    } catch (error) {
      this.logger.error('ZeroBounce validation failed', error, { email });
      throw error;
    }
  }
  
  // Format API response
  formatValidationResponse(data, email) {
    // Basic validation status
    const isValid = data.status === 'valid';
    
    return {
      email,
      valid: isValid,
      status: data.status,
      subStatus: data.sub_status,
      freeEmail: data.free_email,
      didYouMean: data.did_you_mean || null,
      account: data.account || null,
      domain: data.domain || null,
      domainAgeDays: data.domain_age_days,
      smtpProvider: data.smtp_provider || null,
      mxRecord: data.mx_record || null,
      mxFound: data.mx_found,
      firstname: data.firstname || null,
      lastname: data.lastname || null,
      gender: data.gender || null,
      country: data.country || null,
      region: data.region || null,
      city: data.city || null,
      zipcode: data.zipcode || null,
      processedAt: data.processed_at,
      source: 'zerobounce',
      rawResponse: data
    };
  }
  
  // Check if we have sufficient API credits
  async checkCredits() {
    // Check cache first
    if (
      this.creditsCache.credits !== null &&
      Date.now() - this.creditsCache.timestamp < this.creditsCache.ttl
    ) {
      return this.creditsCache.credits;
    }
    
    try {
      // Fix: Use '/getcredits' instead of '/api/getcredits'
      const url = new URL('/getcredits', this.baseUrl);
      url.searchParams.append('api_key', this.apiKey);
      
      const response = await fetch(url.toString());
      
      if (!response.ok) {
        throw new ZeroBounceError(
          `Failed to get credits: ${response.status} ${response.statusText}`,
          response.status
        );
      }
      
      const data = await response.json();
      
      if (data && typeof data.Credits === 'number') {
        // Update cache
        this.creditsCache.credits = data.Credits;
        this.creditsCache.timestamp = Date.now();
        
        return data.Credits;
      }
      
      throw new ZeroBounceError('Invalid response format for credits check');
    } catch (error) {
      this.logger.error('Failed to check ZeroBounce credits', error);
      
      // Return cached credits if available
      if (this.creditsCache.credits !== null) {
        return this.creditsCache.credits;
      }
      
      throw error;
    }
  }
  
  // API Call Helper
  async makeApiCall(endpoint, params = {}, method = 'GET', timeout = this.timeout) {
    const url = new URL(endpoint, this.baseUrl);
    
    // Add API key to all requests
    params.api_key = this.apiKey;
    
    // Configure fetch options
    const fetchOptions = {
      method,
      headers: {
        'Accept': 'application/json'
      }
    };
    
    // Handle different HTTP methods
    if (method === 'GET') {
      // Add params to URL for GET requests
      Object.keys(params).forEach(key => {
        url.searchParams.append(key, params[key]);
      });
    } else {
      // Add params to body for POST requests
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
    const cached = this.responseCache.get(email);
    if (cached && Date.now() - cached.timestamp < this.responseCacheTTL) {
      return cached.data;
    }
    
    // Remove expired entry
    if (cached) {
      this.responseCache.delete(email);
    }
    
    return null;
  }
  
  addToResponseCache(email, data) {
    // Limit cache size
    if (this.responseCache.size >= this.maxCacheSize) {
      // Remove oldest entry
      const firstKey = this.responseCache.keys().next().value;
      this.responseCache.delete(firstKey);
    }
    
    this.responseCache.set(email, {
      data,
      timestamp: Date.now()
    });
  }
  
  // Get circuit breaker state
  getCircuitBreakerState() {
    return {
      state: this.circuitBreaker.status,
      stats: {
        successes: this.circuitBreaker.stats.successes,
        failures: this.circuitBreaker.stats.failures,
        rejects: this.circuitBreaker.stats.rejects,
        timeouts: this.circuitBreaker.stats.timeouts
      }
    };
  }
  
  // Health check - simple version that doesn't make API calls
  async healthCheck() {
    try {
      // Just check if API key is configured and circuit is not open
      const hasApiKey = !!this.apiKey;
      const circuitClosed = !this.circuitBreaker.opened;
      
      return {
        status: hasApiKey && circuitClosed ? 'healthy' : 'unhealthy',
        hasApiKey,
        circuitBreaker: this.circuitBreaker.status,
        cachedCredits: this.creditsCache.credits
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        error: error.message
      };
    }
  }
}

// Create singleton instance
const zeroBounceService = new ZeroBounceService();

// Export both the instance and the class
export { zeroBounceService, ZeroBounceService };
export default zeroBounceService;