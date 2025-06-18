// src/services/external/zerobounce.js
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
    // Try USA endpoint first, then fall back to default
    this.baseUrl = config.services.zeroBounce.baseUrl || 'https://api.zerobounce.net/v2';
    this.baseUrlUS = 'https://api-us.zerobounce.net/v2';
    this.useUSEndpoint = config.services.zeroBounce.useUSEndpoint || false;
    this.apiKey = config.services.zeroBounce.apiKey;
    this.timeout = config.services.zeroBounce.timeout;
    this.retryTimeout = config.services.zeroBounce.retryTimeout;
    this.maxRetries = config.services.zeroBounce.maxRetries;
    
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
    if (!this.apiKey) {
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
      if (error.name === 'CircuitBreakerOpen') {
        throw new ZeroBounceError('ZeroBounce service is temporarily unavailable', 503);
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
          timeout: attempt === 1 ? timeout : this.retryTimeout
        });
        
        // Use longer timeout for retries
        const attemptTimeout = attempt === 1 ? timeout : this.retryTimeout;
        
        const params = {
          api_key: this.apiKey,
          email,
          ip_address: ipAddress || ''
        };
        
        // Use the endpoint that worked for credits, or try both
        const baseUrl = this.useUSEndpoint ? this.baseUrlUS : this.baseUrl;
        const url = new URL('/validate', baseUrl);
        
        // Add params to URL
        Object.keys(params).forEach(key => {
          url.searchParams.append(key, params[key]);
        });
        
        this.logger.debug('Calling ZeroBounce validate API', {
          endpoint: url.toString().replace(this.apiKey, 'REDACTED')
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
      });
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
    
    // Try both endpoints if needed
    const endpoints = this.useUSEndpoint ? 
      [this.baseUrlUS, this.baseUrl] : 
      [this.baseUrl, this.baseUrlUS];
    
    let lastError = null;
    
    for (const baseUrl of endpoints) {
      try {
        const url = new URL('/getcredits', baseUrl);
        url.searchParams.append('api_key', this.apiKey);
        
        this.logger.debug('Checking ZeroBounce credits', { 
          endpoint: url.toString().replace(this.apiKey, 'REDACTED') 
        });
        
        const response = await fetch(url.toString());
        
        if (!response.ok) {
          lastError = new ZeroBounceError(
            `Failed to get credits: ${response.status} ${response.statusText}`,
            response.status
          );
          continue; // Try next endpoint
        }
        
        const data = await response.json();
        
        if (data && typeof data.Credits === 'number') {
          // Update cache
          this.creditsCache.credits = data.Credits;
          this.creditsCache.timestamp = Date.now();
          
          // Remember which endpoint worked
          if (baseUrl === this.baseUrlUS) {
            this.useUSEndpoint = true;
          }
          
          this.logger.info('ZeroBounce credits retrieved', { 
            credits: data.Credits,
            endpoint: baseUrl 
          });
          
          return data.Credits;
        }
        
        if (data && data.Credits === -1) {
          throw new ZeroBounceError('Invalid API key', 401);
        }
        
        throw new ZeroBounceError('Invalid response format for credits check');
      } catch (error) {
        lastError = error;
        this.logger.debug('Failed to get credits from endpoint', { 
          endpoint: baseUrl,
          error: error.message 
        });
      }
    }
    
    this.logger.error('Failed to check ZeroBounce credits from all endpoints', lastError);
    
    // Return cached credits if available
    if (this.creditsCache.credits !== null) {
      return this.creditsCache.credits;
    }
    
    throw lastError || new ZeroBounceError('Failed to check credits from all endpoints');
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
  
  // Health check
  async healthCheck() {
    try {
      const credits = await this.checkCredits();
      
      return {
        status: 'healthy',
        circuitBreaker: this.circuitBreaker.status,
        credits,
        cacheSize: this.responseCache.size,
        enabled: config.services.zeroBounce.enabled
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        circuitBreaker: this.circuitBreaker.status,
        error: error.message,
        enabled: config.services.zeroBounce.enabled
      };
    }
  }
}

// Create singleton instance
const zeroBounceService = new ZeroBounceService();

// Export both the instance and the class
export { zeroBounceService, ZeroBounceService };