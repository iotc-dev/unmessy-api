// src/services/validation-service.js
import { db } from '../core/db.js';
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
import { nameValidationService } from './validation/name-validation-service.js';
import { phoneValidationService } from './validation/phone-validation-service.js';
import { addressValidationService } from './validation/address-validation-service.js';

// Import external services
import { zeroBounceService } from './external/zerobounce.js';
import { openCageService } from './external/opencage.js';

const logger = createServiceLogger('validation-service');

class ValidationService {
  constructor() {
    this.logger = logger;
    
    // Initialize services
    this.emailValidator = emailValidationService;
    this.nameValidator = nameValidationService;
    this.phoneValidator = phoneValidationService;
    this.addressValidator = addressValidationService;
    
    // External services
    this.zeroBounce = zeroBounceService;
    this.openCage = openCageService;
    
    // Cache for batch operations
    this.batchCache = new Map();
    this.batchCacheTTL = 5 * 60 * 1000; // 5 minutes
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
  
  // Email validation
  async validateEmail(email, options = {}) {
    const {
      clientId = null,
      skipExternal = false,
      timeout = config.services.zeroBounce.timeout,
      useCache = true
    } = options;
    
    this.logger.debug('Starting email validation', {
      email,
      clientId,
      skipExternal,
      timeout
    });
    
    try {
      // Quick validation first
      const quickResult = await this.emailValidator.quickValidate(email, clientId);
      
      if (!quickResult.formatValid || quickResult.isInvalidDomain) {
        return quickResult;
      }
      
      // Check cache if enabled
      if (useCache) {
        const cached = await this.emailValidator.checkEmailCache(quickResult.currentEmail);
        if (cached) {
          return { ...cached, isFromCache: true };
        }
      }
      
      // Enhance with external validation if needed
      let result = { ...quickResult };
      
      if (!skipExternal && config.services.zeroBounce.enabled && quickResult.recheckNeeded) {
        try {
          const externalResult = await ErrorRecovery.withTimeout(
            this.zeroBounce.validateEmail(quickResult.currentEmail),
            timeout,
            'ZeroBounce validation'
          );
          
          // Merge results
          result = this.mergeEmailResults(result, externalResult);
        } catch (error) {
          this.logger.warn('External email validation failed', error, {
            email: quickResult.currentEmail,
            service: 'ZeroBounce'
          });
          
          // Continue with quick validation result
          result.validationSteps.push({
            step: 'external_validation',
            error: error.message,
            skipped: true
          });
        }
      }
      
      // Save to cache if valid
      if (useCache && result.status === 'valid') {
        await this.emailValidator.saveEmailCache(email, result, clientId);
      }
      
      return result;
    } catch (error) {
      this.logger.error('Email validation failed', error, { email });
      throw new ValidationError(`Email validation failed: ${error.message}`);
    }
  }
  
  // Name validation
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
          lastName || ''
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
      // Use only internal phone validation (no external carrier lookup)
      const result = await this.phoneValidator.validatePhoneNumber(phone, {
        country,
        clientId
      });
      
      if (!result.valid) {
        return result;
      }
      
      // Check cache
      if (useCache) {
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
      address: typeof address === 'string' ? address : 'complex object',
      country,
      clientId,
      skipExternal
    });
    
    try {
      // Parse and standardize address
      const parsedAddress = await this.addressValidator.parseAddress(address, {
        country,
        clientId
      });
      
      if (!parsedAddress.valid) {
        return parsedAddress;
      }
      
      // Check cache
      if (useCache) {
        const cacheKey = this.addressValidator.generateCacheKey(parsedAddress);
        const cached = await this.addressValidator.checkAddressCache(cacheKey);
        if (cached) {
          return { ...cached, isFromCache: true };
        }
      }
      
      // Enhance with geocoding if enabled
      let result = { ...parsedAddress };
      
      if (!skipExternal && config.services.openCage.enabled && config.validation.address.geocode) {
        try {
          const geocodeResult = await ErrorRecovery.withTimeout(
            this.openCage.geocode(parsedAddress.formatted),
            timeout,
            'OpenCage geocoding'
          );
          
          result = this.mergeAddressResults(result, geocodeResult);
        } catch (error) {
          this.logger.warn('External address validation failed', error, {
            address: parsedAddress.formatted,
            service: 'OpenCage'
          });
          
          // Continue with parsed result
          result.geocoding = {
            attempted: true,
            error: error.message
          };
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
      clientId = null,
      concurrency = 10,
      skipExternal = false,
      continueOnError = true
    } = options;
    
    this.logger.info('Starting batch validation', {
      type: validationType,
      count: items.length,
      clientId,
      concurrency
    });
    
    const results = [];
    const errors = [];
    
    // Process in chunks
    for (let i = 0; i < items.length; i += concurrency) {
      const chunk = items.slice(i, i + concurrency);
      
      const chunkPromises = chunk.map(async (item, index) => {
        try {
          let result;
          const itemIndex = i + index;
          
          switch (validationType) {
            case 'email':
              result = await this.validateEmail(item, { clientId, skipExternal });
              break;
            case 'name':
              result = await this.validateName(item, { clientId });
              break;
            case 'phone':
              result = await this.validatePhone(item, { clientId, skipExternal });
              break;
            case 'address':
              result = await this.validateAddress(item, { clientId, skipExternal });
              break;
            default:
              throw new ValidationError(`Unknown validation type: ${validationType}`);
          }
          
          results[itemIndex] = { success: true, result };
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
    }
    
    this.logger.info('Batch validation completed', {
      type: validationType,
      total: items.length,
      successful: results.filter(r => r.success).length,
      failed: errors.length
    });
    
    return {
      results,
      errors,
      summary: {
        total: items.length,
        successful: results.filter(r => r.success).length,
        failed: errors.length
      }
    };
  }
  
  // Helper methods for merging results
  mergeEmailResults(quickResult, externalResult) {
    return {
      ...quickResult,
      status: externalResult.status || quickResult.status,
      subStatus: externalResult.sub_status || quickResult.subStatus,
      freeEmail: externalResult.free_email,
      roleEmail: externalResult.role,
      catchAll: externalResult.catch_all,
      disposable: externalResult.disposable,
      toxic: externalResult.toxic,
      doNotMail: externalResult.do_not_mail,
      score: externalResult.score,
      externalValidation: {
        provider: 'zerobounce',
        timestamp: new Date().toISOString(),
        raw: externalResult
      },
      validationSteps: [
        ...quickResult.validationSteps,
        {
          step: 'external_validation',
          provider: 'zerobounce',
          passed: externalResult.status === 'valid'
        }
      ]
    };
  }

  
  mergeAddressResults(parsedResult, geocodeResult) {
    return {
      ...parsedResult,
      geocoding: {
        latitude: geocodeResult.geometry?.lat,
        longitude: geocodeResult.geometry?.lng,
        confidence: geocodeResult.confidence,
        formatted: geocodeResult.formatted,
        components: geocodeResult.components,
        timezone: geocodeResult.annotations?.timezone,
        what3words: geocodeResult.annotations?.what3words
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
  
  // Health check
  async healthCheck() {
    const checks = {
      email: 'unknown',
      name: 'unknown',
      phone: 'unknown',
      address: 'unknown',
      external: {
        zeroBounce: 'unknown',
        openCage: 'unknown'
      }
    };
    
    try {
      // Test validators
      const emailTest = await this.validateEmail('test@example.com', { skipExternal: true });
      checks.email = emailTest ? 'healthy' : 'unhealthy';
      
      const nameTest = await this.validateName('John Doe');
      checks.name = nameTest ? 'healthy' : 'unhealthy';
      
      // Check external service circuit breakers
      checks.external.zeroBounce = this.zeroBounce.getCircuitBreakerState();
      checks.external.openCage = this.openCage.getCircuitBreakerState();
      
    } catch (error) {
      this.logger.error('Health check failed', error);
    }
    
    return checks;
  }
}

// Create singleton instance
const validationService = new ValidationService();

// Initialize on first import
validationService.initialize().catch(error => {
  logger.error('Failed to initialize validation service', error);
});

// Export both the instance and the class
export { validationService, ValidationService };