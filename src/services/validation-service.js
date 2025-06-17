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
import { EmailValidationService } from './validation/email-validation-service.js';
import { NameValidationService } from './validation/name-validation-service.js';
import { PhoneValidationService } from './validation/phone-validation-service.js';
import { AddressValidationService } from './validation/address-validation-service.js';

// Import external services
import { zeroBounceService } from './external/zerobounce.js';
import { openCageService } from './external/opencage.js';

const logger = createServiceLogger('validation-service');

class ValidationService {
  constructor() {
    this.logger = logger;
    
    // Initialize validation services
    this.emailValidator = new EmailValidationService();
    this.nameValidator = new NameValidationService();
    this.phoneValidator = new PhoneValidationService();
    this.addressValidator = new AddressValidationService();
    
    // External services
    this.zeroBounce = zeroBounceService;
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
      address: typeof address === 'string' ? address : 'complex object',
      country,
      clientId,
      skipExternal
    });
    
    try {
      // Validate address using address service
      const validationResult = await this.addressValidator.validateAddress(address, {
        useOpenCage: !skipExternal && config.services.openCage.enabled && config.validation.address.geocode,
        clientId,
        country,
        timeout
      });
      
      // Check cache if enabled
      if (useCache && validationResult.valid) {
        const cacheKey = this.addressValidator.generateCacheKey(validationResult);
        const cached = await this.addressValidator.checkAddressCache(cacheKey);
        if (cached) {
          return { ...cached, isFromCache: true };
        }
      }
      
      // Save to cache if valid
      if (useCache && validationResult.valid) {
        await this.addressValidator.saveAddressCache(address, validationResult, clientId);
      }
      
      return validationResult;
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
    // Handle the email suggestion from ZeroBounce
    let finalEmail = quickResult.currentEmail;
    let wasCorrected = quickResult.wasCorrected;
    
    if (externalResult.didYouMean && externalResult.didYouMean !== quickResult.currentEmail) {
      finalEmail = externalResult.didYouMean;
      wasCorrected = true;
    }
    
    return {
      ...quickResult,
      currentEmail: finalEmail,
      um_email: finalEmail,
      wasCorrected,
      status: externalResult.valid ? 'valid' : 'invalid',
      subStatus: externalResult.subStatus || quickResult.subStatus,
      freeEmail: externalResult.freeEmail,
      roleEmail: externalResult.role,
      catchAll: externalResult.catchAll,
      disposable: externalResult.disposable,
      toxic: externalResult.toxic,
      doNotMail: externalResult.doNotMail,
      score: externalResult.score,
      mxFound: externalResult.mxFound,
      mxRecord: externalResult.mxRecord,
      smtpProvider: externalResult.smtpProvider,
      um_email_status: wasCorrected ? 'Changed' : 'Unchanged',
      um_bounce_status: externalResult.valid ? 'Unlikely to bounce' : 'Likely to bounce',
      externalValidation: {
        provider: 'zerobounce',
        timestamp: new Date().toISOString(),
        raw: externalResult.rawResponse
      },
      validationSteps: [
        ...quickResult.validationSteps,
        {
          step: 'external_validation',
          provider: 'zerobounce',
          passed: externalResult.valid
        }
      ]
    };
  }
  
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
          state: this.zeroBounce.getCircuitBreakerState()
        },
        openCage: {
          enabled: config.services.openCage.enabled,
          state: this.openCage.getCircuitBreakerState()
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
      
      const phoneTest = await this.validatePhone('+14155552671', { skipExternal: true });
      checks.phone = phoneTest ? 'healthy' : 'unhealthy';
      
      const addressTest = await this.validateAddress('123 Main St, San Francisco, CA 94105', { skipExternal: true });
      checks.address = addressTest ? 'healthy' : 'unhealthy';
      
      // Check external service circuit breakers
      checks.external.zeroBounce = this.zeroBounce.getCircuitBreakerState().state;
      checks.external.openCage = this.openCage.getCircuitBreakerState().state;
      
    } catch (error) {
      this.logger.error('Health check failed', error);
    }
    
    return checks;
  }
}

// Create singleton instance
const validationService = new ValidationService();

// Export both the instance and the class
export { validationService as default, ValidationService };