// src/services/external/opencage.js
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ExternalServiceError,
  OpenCageError,
  CircuitBreaker,
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
    
    // Initialize circuit breaker
    this.circuitBreaker = new CircuitBreaker({
      name: 'OpenCage',
      failureThreshold: 5,
      resetTimeout: 60000, // 1 minute
      monitoringPeriod: 10000 // 10 seconds
    });
    
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
    return this.circuitBreaker.execute(async () => {
      try {
        const result = await ErrorRecovery.withRetry(
          async (attempt) => {
            this.logger.debug('Calling OpenCage API', { 
              address, 
              attempt,
              timeout
            });
            
            const response = await this.makeApiCall('/json', {
              key: this.apiKey,
              q: address,
              limit: limit,
              language: language,
              pretty: 0,
              no_annotations: 0,
              ...(countryCode && { countrycode: countryCode.toLowerCase() }),
              ...(bounds && { bounds: bounds })
            }, {
              timeout
            });
            
            return response;
          },
          {
            maxAttempts: this.maxRetries,
            delay: 1000,
            backoffMultiplier: 2,
            onRetry: (error, attempt) => {
              this.logger.warn('Retrying OpenCage geocoding', {
                address,
                attempt,
                error: error.message
              });
            }
          }
        );
        
        // Parse and cache result
        const parsedResult = this.parseGeocodeResponse(result, address);
        this.addToCache(cacheKey, parsedResult);
        
        return parsedResult;
      } catch (error) {
        this.logger.error('OpenCage geocoding failed', error, { address });
        
        // Transform to appropriate error type
        if (error instanceof TimeoutError) {
          throw new OpenCageError('Request timeout', 504, error);
        } else if (error.statusCode === 429) {
          throw new OpenCageError('Rate limit exceeded', 429, error);
        } else if (error.statusCode === 401 || error.statusCode === 403) {
          throw new OpenCageError('Invalid API key', 401, error);
        } else {
          throw new OpenCageError(
            error.message || 'Geocoding failed',
            error.statusCode || 503,
            error
          );
        }
      }
    });
  }
  
  // Reverse geocode coordinates
  async reverseGeocode(lat, lng, options = {}) {
    const {
      language = 'en',
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
    
    const query = `${lat},${lng}`;
    
    return this.circuitBreaker.execute(async () => {
      try {
        const response = await this.makeApiCall('/json', {
          key: this.apiKey,
          q: query,
          language: language,
          pretty: 0,
          no_annotations: 0
        }, {
          timeout
        });
        
        return this.parseGeocodeResponse(response, query);
      } catch (error) {
        this.logger.error('Reverse geocoding failed', error, { lat, lng });
        throw new OpenCageError(
          'Reverse geocoding failed',
          error.statusCode || 503,
          error
        );
      }
    });
  }
  
  // Make API call
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
    
    try {
      const response = await ErrorRecovery.withTimeout(
        fetch(url.toString(), fetchOptions),
        timeout,
        `OpenCage ${endpoint}`
      );
      
      // Extract rate limit info from headers
      this.updateRateLimitInfo(response.headers);
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw {
          statusCode: response.status,
          message: errorData.status?.message || response.statusText,
          code: errorData.status?.code
        };
      }
      
      const data = await response.json();
      
      // Check for API errors in response
      if (data.status && data.status.code !== 200) {
        throw {
          statusCode: data.status.code,
          message: data.status.message
        };
      }
      
      this.logger.debug('OpenCage API call successful', {
        endpoint,
        status: response.status,
        results: data.results?.length || 0
      });
      
      return data;
    } catch (error) {
      if (error instanceof TimeoutError) {
        throw error;
      }
      
      this.logger.error('OpenCage API call failed', error, {
        endpoint,
        statusCode: error.statusCode
      });
      
      throw error;
    }
  }
  
  // Parse geocoding response
  parseGeocodeResponse(response, query) {
    if (!response.results || response.results.length === 0) {
      return {
        query,
        found: false,
        results: [],
        totalResults: 0
      };
    }
    
    const results = response.results.map(result => ({
      // Basic info
      formatted: result.formatted,
      confidence: result.confidence || 0,
      
      // Geometry
      geometry: {
        lat: result.geometry.lat,
        lng: result.geometry.lng
      },
      
      // Bounds if available
      bounds: result.bounds || null,
      
      // Parsed components with all um_address fields
      components: this.parseAddressComponents(result.components),
      
      // Additional metadata
      annotations: {
        timezone: result.annotations?.timezone,
        what3words: result.annotations?.what3words,
        currency: result.annotations?.currency,
        dms: result.annotations?.DMS
      }
    }));
    
    return {
      query,
      found: true,
      results,
      totalResults: response.total_results || results.length,
      timestamp: new Date().toISOString()
    };
  }
  
  // Parse address components into um_address fields
  parseAddressComponents(components) {
    if (!components) return {};
    
    // Extract street number and name
    let houseNumber = components.house_number || '';
    let streetName = components.road || components.street || '';
    let streetType = '';
    let streetDirection = '';
    
    // Try to parse street type from road name
    if (streetName) {
      const streetParts = streetName.split(' ');
      const lastPart = streetParts[streetParts.length - 1];
      
      // Check if last part is a street type
      const commonTypes = ['Street', 'St', 'Avenue', 'Ave', 'Road', 'Rd', 'Drive', 'Dr', 
                          'Lane', 'Ln', 'Court', 'Ct', 'Boulevard', 'Blvd', 'Way'];
      if (commonTypes.some(type => lastPart.toLowerCase() === type.toLowerCase())) {
        streetType = lastPart;
        streetName = streetParts.slice(0, -1).join(' ');
      }
    }
    
    return {
      // House and street info
      um_house_number: houseNumber,
      um_street_name: streetName,
      um_street_type: streetType,
      um_street_direction: streetDirection,
      
      // Unit info (OpenCage doesn't usually provide this)
      um_unit_type: '',
      um_unit_number: '',
      
      // Constructed address lines
      um_address_line_1: [houseNumber, streetDirection, streetName, streetType]
        .filter(Boolean).join(' ').trim(),
      um_address_line_2: '',
      
      // City/State/Country
      um_city: components.city || components.town || components.village || 
               components.hamlet || components.suburb || '',
      um_state_province: components.state_code || components.state || 
                        components.province || components.region || '',
      um_country: components.country || '',
      um_country_code: (components.country_code || '').toUpperCase(),
      um_postal_code: components.postcode || '',
      
      // Raw components for reference
      raw: components
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
    return this.circuitBreaker.state;
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
        circuitBreaker: this.circuitBreaker.state,
        rateLimit: this.getRateLimitInfo(),
        cacheSize: this.geocodeCache.size,
        enabled: config.services.openCage.enabled,
        hasResults: result.found
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        circuitBreaker: this.circuitBreaker.state,
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