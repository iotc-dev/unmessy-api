// src/services/validation-service.js
import db from '../core/db.js';
import { config } from '../core/config.js';
import { createServiceLogger } from '../core/logger.js';
import { 
  ValidationError, 
  ExternalServiceError,
  TimeoutError,
  ErrorRecovery 
} from '../core/errors.js';

// Import validation services
import { emailValidationService } from './validation/email-validation-service.js';
import { NameValidationService } from './validation/name-validation-service.js';
import { PhoneValidationService } from './validation/phone-validation-service.js';
import { AddressValidationService } from './validation/address-validation-service.js';

// Import external services
import { openCageService } from './external/opencage.js';

const logger = createServiceLogger('validation-service');

class ValidationService {
  constructor() {
    this.logger = logger;
    
    // Use the singleton email validator instance
    this.emailValidator = emailValidationService;
    
    // Initialize other validation services
    this.nameValidator = new NameValidationService();
    this.phoneValidator = new PhoneValidationService();
    this.addressValidator = new AddressValidationService();
    
    // External services (only OpenCage now, ZeroBounce is integrated in email validator)
    this.openCage = openCageService;
    
    // Cache for batch operations
    this.batchCache = new Map();
    this.batchCacheTTL = 5 * 60 * 1000; // 5 minutes
    
    // Initialize on construction
    this.initialize().catch(error => {
      logger.error('Failed to initialize validation service', error);
    });
  }
  
  // Initialize all services
  async initialize() {
    try {
      this.logger.info('Initializing validation services');
      
      // Load normalization data for all validators
      await Promise.all([
        this.emailValidator.loadNormalizationData(),
        this.nameValidator.loadNormalizationData(),
        this.phoneValidator.loadNormalizationData(),
        this.addressValidator.loadNormalizationData()
      ]);
      
      this.logger.info('All validation services initialized');
    } catch (error) {
      this.logger.error('Failed to initialize validation services', error);
      // Don't throw - services can still work with default data
    }
  }
  
  // Email validation - now simplified to use the integrated email validator
  async validateEmail(email, options = {}) {
    const {
      clientId = null,
      skipZeroBounce = false,
      timeout = config.services.zeroBounce.timeout,
      useCache = true
    } = options;
    
    this.logger.debug('Starting email validation', {
      email,
      clientId,
      skipZeroBounce,
      useCache
    });
    
    try {
      // Use the integrated email validation service
      // It handles everything: cache check, format validation, typo correction, ZeroBounce
      const result = await this.emailValidator.validateEmail(email, {
        clientId,
        useCache,
        useZeroBounce: !skipZeroBounce
      });
      
      return result;
    } catch (error) {
      this.logger.error('Email validation failed', error, { email });
      throw new ValidationError(`Email validation failed: ${error.message}`);
    }
  }
  
  // Name validation (with support for both full name and separate names)
  async validateName(name, options = {}) {
    const {
      clientId = null,
      firstName = null,
      lastName = null,
      useCache = true
    } = options;
    
    try {
      let result;
      
      if (firstName !== null || lastName !== null) {
        // Validate separate names
        result = await this.nameValidator.validateSeparateNames(
          firstName || '',
          lastName || '',
          { useCache, clientId }
        );
      } else if (name) {
        // Validate full name
        result = await this.nameValidator.validateFullName(name, {
          useCache,
          clientId
        });
      } else {
        throw new ValidationError('Name or firstName/lastName required');
      }
      
      return result;
    } catch (error) {
      this.logger.error('Name validation failed', error, {
        name,
        firstName,
        lastName
      });
      throw new ValidationError(`Name validation failed: ${error.message}`);
    }
  }
  
  // Aliases for name validation methods
  async validateFullName(name, options = {}) {
    return this.validateName(name, options);
  }
  
  async validateSeparateNames(firstName, lastName, options = {}) {
    return this.validateName(null, { ...options, firstName, lastName });
  }
  
  // Phone validation
  async validatePhone(phone, options = {}) {
    const {
      clientId = null,
      country = config.validation.phone.defaultCountry,
      useCache = true
    } = options;
    
    this.logger.debug('Starting phone validation', {
      phone,
      country,
      clientId
    });
    
    try {
      // Use phone validation service
      const result = await this.phoneValidator.validatePhoneNumber(phone, {
        country,
        clientId,
        useCache
      });
      
      if (!result.valid) {
        return result;
      }
      
      // Check cache
      if (useCache && result.e164) {
        const cached = await this.phoneValidator.checkPhoneCache(result.e164);
        if (cached) {
          return { ...cached, isFromCache: true };
        }
      }
      
      // Save to cache if valid
      if (useCache && result.valid) {
        await this.phoneValidator.savePhoneCache(phone, result, clientId);
      }
      
      return result;
    } catch (error) {
      this.logger.error('Phone validation failed', error, { phone });
      throw new ValidationError(`Phone validation failed: ${error.message}`);
    }
  }
  
  // Address validation
  async validateAddress(address, options = {}) {
    const {
      clientId = null,
      country = config.validation.address.defaultCountry,
      skipExternal = false,
      timeout = config.services.openCage.timeout,
      useCache = true
    } = options;
    
    this.logger.debug('Starting address validation', {
      address: typeof address === 'string' ? address : 'Object',
      country,
      skipExternal,
      clientId
    });
    
    try {
      // Parse and validate address
      const parsedResult = await this.addressValidator.validateAddress(address, {
        country,
        clientId,
        useCache
      });
      
      if (!parsedResult.valid) {
        return parsedResult;
      }
      
      // Check cache
      if (useCache && parsedResult.formattedAddress) {
        const cached = await this.addressValidator.checkAddressCache(parsedResult.formattedAddress);
        if (cached) {
          return { ...cached, isFromCache: true };
        }
      }
      
      // Enhance with geocoding if needed
      let result = { ...parsedResult };
      
      if (!skipExternal && config.services.openCage.enabled && config.validation.address.geocode) {
        try {
          const geocodeResult = await ErrorRecovery.withTimeout(
            this.openCage.geocode(parsedResult.formattedAddress, { country }),
            timeout,
            'OpenCage geocoding'
          );
          
          // Merge results
          result = this.mergeAddressResults(result, geocodeResult);
        } catch (error) {
          this.logger.warn('External address validation failed', error, {
            address: parsedResult.formattedAddress,
            service: 'OpenCage'
          });
          
          // Continue with parsed result
          result.validationSteps.push({
            step: 'geocoding',
            error: error.message,
            skipped: true
          });
        }
      }
      
      // Save to cache if valid
      if (useCache && result.valid) {
        await this.addressValidator.saveAddressCache(address, result, clientId);
      }
      
      return result;
    } catch (error) {
      this.logger.error('Address validation failed', error, { address });
      throw new ValidationError(`Address validation failed: ${error.message}`);
    }
  }
  
  // Batch validation
  async validateBatch(items, validationType, options = {}) {
    const {
      chunkSize = 100,
      delayBetweenChunks = 1000,
      continueOnError = true,
      ...validationOptions
    } = options;
    
    // Check if batch is cached
    const cacheKey = `${validationType}-${JSON.stringify(items)}`;
    if (this.batchCache.has(cacheKey)) {
      const cached = this.batchCache.get(cacheKey);
      if (Date.now() - cached.timestamp < this.batchCacheTTL) {
        return cached.result;
      }
      this.batchCache.delete(cacheKey);
    }
    
    const results = [];
    const errors = [];
    
    // Process in chunks
    for (let i = 0; i < items.length; i += chunkSize) {
      const chunk = items.slice(i, Math.min(i + chunkSize, items.length));
      
      // Process chunk
      const chunkPromises = chunk.map(async (item, index) => {
        try {
          let result;
          
          switch (validationType) {
            case 'email':
              result = await this.validateEmail(item, validationOptions);
              break;
            case 'name':
              result = await this.validateName(item, validationOptions);
              break;
            case 'phone':
              result = await this.validatePhone(item, validationOptions);
              break;
            case 'address':
              result = await this.validateAddress(item, validationOptions);
              break;
            default:
              throw new ValidationError(`Unknown validation type: ${validationType}`);
          }
          
          results[i + index] = { success: true, data: result };
        } catch (error) {
          const itemIndex = i + index;
          errors.push({ index: itemIndex, item, error: error.message });
          
          if (continueOnError) {
            results[itemIndex] = { success: false, error: error.message };
          } else {
            throw error;
          }
        }
      });
      
      await Promise.all(chunkPromises);
      
      // Delay between chunks
      if (i + chunkSize < items.length && delayBetweenChunks > 0) {
        await new Promise(resolve => setTimeout(resolve, delayBetweenChunks));
      }
    }
    
    const batchResult = {
      results,
      errors,
      summary: {
        total: items.length,
        successful: results.filter(r => r?.success).length,
        failed: errors.length
      }
    };
    
    // Cache result
    this.batchCache.set(cacheKey, {
      result: batchResult,
      timestamp: Date.now()
    });
    
    // Clean up old cache entries
    if (this.batchCache.size > 1000) {
      const oldestKey = this.batchCache.keys().next().value;
      this.batchCache.delete(oldestKey);
    }
    
    this.logger.info('Batch validation completed', {
      type: validationType,
      ...batchResult.summary
    });
    
    return batchResult;
  }
  
  // Helper method for merging address results
  mergeAddressResults(parsedResult, geocodeResult) {
    return {
      ...parsedResult,
      geocoding: {
        latitude: geocodeResult.coordinates?.latitude,
        longitude: geocodeResult.coordinates?.longitude,
        confidence: geocodeResult.confidence,
        formatted: geocodeResult.formattedAddress,
        components: geocodeResult.components,
        found: geocodeResult.found
      },
      validationSteps: [
        ...parsedResult.validationSteps,
        {
          step: 'geocoding',
          provider: 'opencage',
          passed: geocodeResult.confidence >= 7
        }
      ]
    };
  }
  
  // Get validation service statistics
  async getStats() {
    const stats = {
      services: {
        email: this.emailValidator.constructor.name,
        name: this.nameValidator.constructor.name,
        phone: this.phoneValidator.constructor.name,
        address: this.addressValidator.constructor.name
      },
      external: {
        zeroBounce: {
          enabled: config.services.zeroBounce.enabled,
          integrated: 'In EmailValidationService'
        },
        openCage: {
          enabled: config.services.openCage.enabled,
          state: this.openCage.getCircuitBreakerState ? this.openCage.getCircuitBreakerState() : 'unknown'
        }
      },
      cache: {
        batchCacheSize: this.batchCache.size
      }
    };
    
    return stats;
  }
  
  // Health check
  async healthCheck() {
    const checks = {
      email: 'unknown',
      name: 'unknown',
      phone: 'unknown',
      address: 'unknown',
      external: {
        zeroBounce: 'integrated',
        openCage: 'unknown'
      }
    };
    
    try {
      // Test validators with simple test data
      const emailTest = await this.validateEmail('test@example.com', { skipZeroBounce: true });
      checks.email = emailTest ? 'healthy' : 'unhealthy';
      
      const nameTest = await this.validateName('John Doe');
      checks.name = nameTest ? 'healthy' : 'unhealthy';
      
      const phoneTest = await this.validatePhone('+14155552671');
      checks.phone = phoneTest ? 'healthy' : 'unhealthy';
      
      const addressTest = await this.validateAddress('123 Main St, San Francisco, CA 94105', { skipExternal: true });
      checks.address = addressTest ? 'healthy' : 'unhealthy';
      
      // Check external services
      if (this.openCage && this.openCage.healthCheck) {
        checks.external.openCage = await this.openCage.healthCheck() ? 'healthy' : 'unhealthy';
      }
    } catch (error) {
      this.logger.error('Health check failed', error);
    }
    
    return checks;
  }
}

// Create singleton instance
const validationService = new ValidationService();

// Export default
export default validationService;