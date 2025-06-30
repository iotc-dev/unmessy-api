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
  
  // Phone validation - UPDATED to ensure proper return format
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
      
      // Ensure the result has all the required fields for queue-service
      const enhancedResult = {
        ...result,
        // Ensure these fields exist for queue-service compatibility
        formatted: result.formatted || result.e164 || phone,
        wasCorrected: result.wasCorrected || false,
        formatValid: result.valid || false,
        isValid: result.valid || false,
        isMobile: result.isMobile || false,
        countryCode: result.countryCode || '',
        country: result.country || country,
        type: result.type || 'unknown',
        carrier: result.carrier || null,
        // Additional fields that might be useful
        e164: result.e164 || null,
        national: result.national || null,
        international: result.international || null,
        lineType: result.lineType || result.type || 'unknown'
      };
      
      if (!result.valid) {
        return enhancedResult;
      }
      
      // Check cache
      if (useCache && result.e164) {
        const cached = await this.phoneValidator.checkPhoneCache(result.e164);
        if (cached) {
          return { ...enhancedResult, ...cached, isFromCache: true };
        }
      }
      
      // Save to cache if valid
      if (useCache && result.valid) {
        await this.phoneValidator.savePhoneCache(phone, enhancedResult, clientId);
      }
      
      return enhancedResult;
    } catch (error) {
      this.logger.error('Phone validation failed', error, { phone });
      throw new ValidationError(`Phone validation failed: ${error.message}`);
    }
  }
  
  // Address validation - UPDATED to ensure proper return format
  async validateAddress(address, options = {}) {
    const {
      clientId = null,
      country = config.validation.address.defaultCountry,
      skipExternal = false,
      timeout = config.services.openCage.timeout,
      useCache = true,
      useOpenCage = !skipExternal // Allow explicit control
    } = options;
    
    this.logger.debug('Starting address validation', {
      address: typeof address === 'string' ? address : 'Object',
      country,
      skipExternal,
      clientId
    });
    
    try {
      // Parse and validate address - handle multiple input formats
      const addressInput = this.normalizeAddressInput(address);
      
      const parsedResult = await this.addressValidator.validateAddress(addressInput, {
        country,
        clientId,
        useCache
      });
      
      // Ensure the result has all required fields for queue-service
      let result = {
        ...parsedResult,
        // Ensure these fields exist
        valid: parsedResult.valid || false,
        wasCorrected: parsedResult.wasCorrected || false,
        confidence: parsedResult.confidence || 0,
        // Address components
        houseNumber: parsedResult.houseNumber || parsedResult.um_house_number || '',
        streetName: parsedResult.streetName || parsedResult.um_street_name || '',
        streetType: parsedResult.streetType || parsedResult.um_street_type || '',
        streetDirection: parsedResult.streetDirection || parsedResult.um_street_direction || '',
        unitType: parsedResult.unitType || parsedResult.um_unit_type || '',
        unitNumber: parsedResult.unitNumber || parsedResult.um_unit_number || '',
        city: parsedResult.city || parsedResult.um_city || '',
        state: parsedResult.state || parsedResult.um_state_province || '',
        postalCode: parsedResult.postalCode || parsedResult.um_postal_code || '',
        country: parsedResult.country || parsedResult.um_country || country,
        countryCode: parsedResult.countryCode || parsedResult.um_country_code || '',
        // Formatted versions
        formattedAddress: parsedResult.formattedAddress || parsedResult.formatted || '',
        um_address_line_1: parsedResult.um_address_line_1 || parsedResult.addressLine1 || '',
        um_address_line_2: parsedResult.um_address_line_2 || parsedResult.addressLine2 || '',
        // Keep original parsed result fields
        um_house_number: parsedResult.houseNumber || parsedResult.um_house_number || '',
        um_street_name: parsedResult.streetName || parsedResult.um_street_name || '',
        um_street_type: parsedResult.streetType || parsedResult.um_street_type || '',
        um_street_direction: parsedResult.streetDirection || parsedResult.um_street_direction || '',
        um_unit_type: parsedResult.unitType || parsedResult.um_unit_type || '',
        um_unit_number: parsedResult.unitNumber || parsedResult.um_unit_number || '',
        um_city: parsedResult.city || parsedResult.um_city || '',
        um_state_province: parsedResult.state || parsedResult.um_state_province || '',
        um_postal_code: parsedResult.postalCode || parsedResult.um_postal_code || '',
        um_country: parsedResult.country || parsedResult.um_country || country,
        um_country_code: parsedResult.countryCode || parsedResult.um_country_code || ''
      };
      
      if (!result.valid && !useOpenCage) {
        return result;
      }
      
      // Check cache
      if (useCache && result.formattedAddress) {
        const cached = await this.addressValidator.checkAddressCache(result.formattedAddress);
        if (cached) {
          return { ...result, ...cached, isFromCache: true };
        }
      }
      
      // Enhance with geocoding if needed
      if (useOpenCage && config.services.openCage.enabled && config.validation.address.geocode) {
        try {
          const geocodeResult = await ErrorRecovery.withTimeout(
            this.openCage.geocode(result.formattedAddress || this.buildAddressString(result), { country }),
            timeout,
            'OpenCage geocoding'
          );
          
          // Merge results
          result = this.mergeAddressResults(result, geocodeResult);
        } catch (error) {
          this.logger.warn('External address validation failed', error, {
            address: result.formattedAddress,
            service: 'OpenCage'
          });
          
          // Continue with parsed result
          if (!result.validationSteps) {
            result.validationSteps = [];
          }
          result.validationSteps.push({
            step: 'geocoding',
            error: error.message,
            skipped: true
          });
        }
      }
      
      // Save to cache if valid
      if (useCache && result.valid) {
        await this.addressValidator.saveAddressCache(addressInput, result, clientId);
      }
      
      return result;
    } catch (error) {
      this.logger.error('Address validation failed', error, { address });
      throw new ValidationError(`Address validation failed: ${error.message}`);
    }
  }
  
  // Helper to normalize address input
  normalizeAddressInput(address) {
    if (typeof address === 'string') {
      return address;
    }
    
    if (typeof address === 'object' && address !== null) {
      // Handle various object formats
      if (address.line1 || address.address) {
        return {
          address: address.address || address.line1,
          address2: address.address2 || address.line2,
          city: address.city,
          state: address.state || address.stateProvince,
          postalCode: address.postalCode || address.zip || address.postal_code,
          country: address.country,
          countryCode: address.countryCode || address.country_code
        };
      }
      
      // Already normalized
      return address;
    }
    
    throw new ValidationError('Invalid address format');
  }
  
  // Helper to build address string from components
  buildAddressString(components) {
    const parts = [];
    
    if (components.houseNumber) parts.push(components.houseNumber);
    if (components.streetName) parts.push(components.streetName);
    if (components.streetType) parts.push(components.streetType);
    if (components.unitType && components.unitNumber) {
      parts.push(`${components.unitType} ${components.unitNumber}`);
    }
    
    const street = parts.join(' ');
    const cityStateZip = [
      components.city,
      components.state,
      components.postalCode
    ].filter(Boolean).join(', ');
    
    return [street, cityStateZip, components.country]
      .filter(Boolean)
      .join(', ');
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
              // Handle phone objects in batch
              if (typeof item === 'object' && item.phone) {
                result = await this.validatePhone(item.phone, {
                  ...validationOptions,
                  country: item.country || validationOptions.country
                });
              } else {
                result = await this.validatePhone(item, validationOptions);
              }
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
    const merged = {
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
        ...(parsedResult.validationSteps || []),
        {
          step: 'geocoding',
          provider: 'opencage',
          passed: geocodeResult.confidence >= 7
        }
      ]
    };
    
    // Update confidence based on geocoding
    if (geocodeResult.found && geocodeResult.confidence >= 7) {
      merged.confidence = Math.max(merged.confidence || 0, geocodeResult.confidence);
      merged.valid = true;
    }
    
    // Merge any enhanced components from geocoding
    if (geocodeResult.components) {
      merged.city = merged.city || geocodeResult.components.city || geocodeResult.components.town;
      merged.state = merged.state || geocodeResult.components.state || geocodeResult.components.state_code;
      merged.postalCode = merged.postalCode || geocodeResult.components.postcode;
      merged.country = merged.country || geocodeResult.components.country;
      merged.countryCode = merged.countryCode || geocodeResult.components.country_code;
      
      // Also update um_ fields
      merged.um_city = merged.city;
      merged.um_state_province = merged.state;
      merged.um_postal_code = merged.postalCode;
      merged.um_country = merged.country;
      merged.um_country_code = merged.countryCode;
    }
    
    return merged;
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