// src/services/external/opencage.js
import CircuitBreaker from 'opossum';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ExternalServiceError,
  OpenCageError,
  ErrorRecovery,
  TimeoutError
} from '../../core/errors.js';

const logger = createServiceLogger('opencage');

class OpenCageService {
  constructor() {
    this.logger = logger;
    this.baseUrl = config.services.openCage.baseUrl;
    this.apiKey = config.services.openCage.apiKey;
    this.timeout = config.services.openCage.timeout;
    this.maxRetries = config.services.openCage.maxRetries;
    
    // Initialize circuit breaker with Opossum
    this.circuitBreaker = new CircuitBreaker(this.executeRequest.bind(this), {
      name: 'OpenCage',
      timeout: 10000, // Time in ms before a request is considered failed
      errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
      resetTimeout: 60000, // Wait time before trying to close the circuit
      volumeThreshold: 5, // Minimum number of requests needed before tripping circuit
      rollingCountTimeout: 10000, // Time window for error rate calculation
      rollingCountBuckets: 10 // Number of buckets for stats tracking
    });
    
    // Add event listeners
    this.setupCircuitBreakerEvents();
    
    // Cache for geocoding results
    this.geocodeCache = new Map();
    this.geocodeCacheTTL = 60 * 60 * 1000; // 1 hour
    this.maxCacheSize = 1000;
    
    // Rate limiting info
    this.rateLimit = {
      remaining: null,
      reset: null,
      lastChecked: null
    };
  }
  
  // Setup circuit breaker event handlers
  setupCircuitBreakerEvents() {
    this.circuitBreaker.on('open', () => {
      this.logger.warn('OpenCage circuit breaker opened');
    });
    
    this.circuitBreaker.on('halfOpen', () => {
      this.logger.info('OpenCage circuit breaker half-open, testing service');
    });
    
    this.circuitBreaker.on('close', () => {
      this.logger.info('OpenCage circuit breaker closed, service recovered');
    });
    
    this.circuitBreaker.on('fallback', (result) => {
      this.logger.warn('OpenCage circuit breaker fallback executed');
    });
    
    this.circuitBreaker.on('timeout', () => {
      this.logger.warn('OpenCage request timed out');
    });
    
    this.circuitBreaker.on('reject', () => {
      this.logger.warn('OpenCage request rejected (circuit open)');
    });
  }
  
  // Geocode an address
  async geocode(address, options = {}) {
    const {
      countryCode = null,
      bounds = null,
      language = 'en',
      limit = 1,
      timeout = this.timeout
    } = options;
    
    // Check if service is enabled
    if (!config.services.openCage.enabled) {
      throw new OpenCageError('OpenCage service is not enabled', 503);
    }
    
    // Check if API key is configured
    if (!this.apiKey) {
      throw new OpenCageError('OpenCage API key not configured', 503);
    }
    
    // Check cache first
    const cacheKey = this.generateCacheKey(address, options);
    const cached = this.getFromCache(cacheKey);
    if (cached) {
      this.logger.debug('Address found in cache', { address });
      return cached;
    }
    
    // Execute with circuit breaker
    try {
      const requestData = {
        address,
        countryCode,
        bounds,
        language,
        limit,
        timeout
      };
      
      const result = await this.circuitBreaker.fire(requestData);
      
      // Cache successful response
      this.addToCache(cacheKey, result);
      
      return result;
    } catch (error) {
      if (error.name === 'CircuitBreaker:OpenError') {
        throw new OpenCageError('OpenCage service is currently unavailable (circuit open)', 503);
      }
      
      if (error.name === 'CircuitBreaker:TimeoutError') {
        throw new TimeoutError('OpenCage', timeout);
      }
      
      if (error instanceof OpenCageError || error instanceof TimeoutError) {
        throw error;
      }
      
      throw new OpenCageError(`Geocoding failed: ${error.message}`, 500);
    }
  }
  
  // This is the function that will be wrapped by the circuit breaker
  async executeRequest(requestData) {
    const {
      address,
      countryCode,
      bounds,
      language,
      limit,
      timeout
    } = requestData;
    
    try {
      return await ErrorRecovery.withRetry(
        async (attempt) => {
          this.logger.debug('Calling OpenCage API', { 
            address, 
            attempt,
            timeout
          });
          
          const url = new URL('/geocode/v1/json', this.baseUrl);
          
          // Build query parameters
          const params = {
            q: address,
            key: this.apiKey,
            language: language || 'en',
            limit: limit || 1,
            no_annotations: 0,
            abbrv: 1
          };
          
          // Add optional parameters
          if (countryCode) {
            params.countrycode = countryCode;
          }
          
          if (bounds) {
            params.bounds = bounds;
          }
          
          // Add params to URL
          Object.keys(params).forEach(key => {
            url.searchParams.append(key, params[key]);
          });
          
          // Execute API call with timeout
          const response = await ErrorRecovery.withTimeout(
            fetch(url.toString()),
            timeout,
            `OpenCage geocode: ${address}`
          );
          
          if (!response.ok) {
            const errorText = await response.text();
            throw new OpenCageError(
              `API error: ${response.status} ${errorText || response.statusText}`,
              response.status
            );
          }
          
          // Update rate limit info from headers
          this.updateRateLimitInfo(response.headers);
          
          const data = await response.json();
          
          if (!data || !data.results) {
            throw new OpenCageError('Invalid response from OpenCage API');
          }
          
          // Format response
          return this.formatGeocodingResponse(data, address);
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
      this.logger.error('OpenCage geocoding failed', error, { address });
      throw error;
    }
  }
  
  // Format geocoding response
  formatGeocodingResponse(data, query) {
    const results = data.results || [];
    const found = results.length > 0;
    
    // Extract components from first result
    const components = found ? results[0].components || {} : {};
    
    // Extract formatted address
    const formattedAddress = found ? results[0].formatted || '' : '';
    
    // Extract coordinates
    const coordinates = found ? {
      latitude: results[0].geometry.lat,
      longitude: results[0].geometry.lng
    } : {
      latitude: null,
      longitude: null
    };
    
    // Extract confidence
    const confidence = found ? results[0].confidence || 0 : 0;
    
    return {
      query,
      found,
      formattedAddress,
      coordinates,
      confidence,
      components: {
        houseNumber: components.house_number || '',
        road: components.road || components.street || '',
        suburb: components.suburb || components.neighborhood || '',
        city: components.city || components.town || components.village || '',
        county: components.county || components.state_district || '',
        state: components.state || components.province || '',
        country: components.country || '',
        countryCode: components.country_code || '',
        postcode: components.postcode || components.postal_code || ''
      },
      source: 'opencage',
      reference: components
    };
  }
  
  // Update rate limit info from headers
  updateRateLimitInfo(headers) {
    try {
      const remaining = headers.get('X-RateLimit-Remaining');
      const reset = headers.get('X-RateLimit-Reset');
      
      if (remaining !== null) {
        this.rateLimit.remaining = parseInt(remaining, 10);
      }
      if (reset !== null) {
        this.rateLimit.reset = parseInt(reset, 10) * 1000; // Convert to ms
      }
      this.rateLimit.lastChecked = Date.now();
      
      if (this.rateLimit.remaining !== null && this.rateLimit.remaining < 100) {
        this.logger.warn('OpenCage rate limit low', {
          remaining: this.rateLimit.remaining,
          reset: new Date(this.rateLimit.reset).toISOString()
        });
      }
    } catch (error) {
      // Ignore rate limit parsing errors
    }
  }
  
  // Cache management
  generateCacheKey(address, options) {
    const parts = [
      address.toLowerCase(),
      options.countryCode || '',
      options.language || 'en'
    ];
    return parts.join('|');
  }
  
  getFromCache(key) {
    const cached = this.geocodeCache.get(key);
    if (!cached) return null;
    
    // Check if expired
    if (Date.now() - cached.timestamp > this.geocodeCacheTTL) {
      this.geocodeCache.delete(key);
      return null;
    }
    
    return cached.data;
  }
  
  addToCache(key, data) {
    // Maintain cache size limit
    if (this.geocodeCache.size >= this.maxCacheSize) {
      // Remove oldest entry
      const firstKey = this.geocodeCache.keys().next().value;
      this.geocodeCache.delete(firstKey);
    }
    
    this.geocodeCache.set(key, {
      data,
      timestamp: Date.now()
    });
  }
  
  clearCache() {
    this.geocodeCache.clear();
    this.logger.info('OpenCage cache cleared');
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
  
  // Get rate limit info
  getRateLimitInfo() {
    return {
      ...this.rateLimit,
      apiKeyConfigured: !!this.apiKey,
      enabled: config.services.openCage.enabled
    };
  }
  
  // Health check
  async healthCheck() {
    try {
      // Try a simple geocode request
      const result = await this.geocode('London', { limit: 1 });
      
      return {
        status: 'healthy',
        circuitBreaker: this.circuitBreaker.status,
        rateLimit: this.getRateLimitInfo(),
        cacheSize: this.geocodeCache.size,
        enabled: config.services.openCage.enabled,
        hasResults: result.found
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        circuitBreaker: this.circuitBreaker.status,
        error: error.message,
        enabled: config.services.openCage.enabled
      };
    }
  }
}

// Create singleton instance
const openCageService = new OpenCageService();

// Export both the instance and the class
export { openCageService, OpenCageService };