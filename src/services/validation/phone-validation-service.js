// src/services/validation/phone-validation-service.js
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { ValidationError } from '../../core/errors.js';
import { parsePhoneNumber, isValidPhoneNumber, getCountries } from 'libphonenumber-js';

const logger = createServiceLogger('phone-validation-service');

class PhoneValidationService {
  constructor() {
    this.logger = logger;
    
    // Default country for parsing
    this.defaultCountry = config.validation.phone.defaultCountry || 'US';
    
    // Track supported countries
    this.supportedCountries = new Set(getCountries());
  }
  
  // Generate um_check_id
  generateUmCheckId(clientId) {
    const epochTime = Date.now();
    const lastSixDigits = String(epochTime).slice(-6);
    const clientIdStr = clientId || config.clients.defaultClientId;
    const firstThreeDigits = String(epochTime).slice(0, 3);
    const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
    const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
    return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${config.unmessy.version}`);
  }
  
  // Main validation function
  async validatePhoneNumber(phoneNumber, options = {}) {
    const {
      country = this.defaultCountry,
      clientId = null
    } = options;
    
    this.logger.debug('Starting phone validation', { 
      phoneNumber,
      country,
      clientId
    });
    
    // Check for null/empty
    if (!phoneNumber) {
      return this.buildValidationResult(phoneNumber, {
        valid: false,
        formatValid: false,
        error: 'Phone number is required',
        wasCorrected: false
      }, clientId);
    }
    
    // Clean the phone number
    const cleanedNumber = this.cleanPhoneNumber(phoneNumber);
    
    try {
      // Try to parse with country hint first
      let parsed;
      let parseError = null;
      
      try {
        // First try with provided country
        parsed = parsePhoneNumber(cleanedNumber, country);
      } catch (error) {
        parseError = error;
        this.logger.debug('Failed to parse with country hint, trying without', {
          phoneNumber: cleanedNumber,
          country,
          error: error.message
        });
        
        // Try without country (for international format)
        try {
          parsed = parsePhoneNumber(cleanedNumber);
        } catch (secondError) {
          // If both fail, the number is invalid
          this.logger.debug('Failed to parse phone number', {
            phoneNumber: cleanedNumber,
            error: secondError.message
          });
          
          return this.buildValidationResult(phoneNumber, {
            valid: false,
            formatValid: false,
            error: 'Invalid phone number format',
            wasCorrected: false
          }, clientId);
        }
      }
      
      // Check if parsed number is valid
      if (!parsed || !parsed.isValid()) {
        return this.buildValidationResult(phoneNumber, {
          valid: false,
          formatValid: false,
          error: 'Invalid phone number',
          country: parsed?.country || 'unknown',
          wasCorrected: false
        }, clientId);
      }
      
      // Extract components
      const components = {
        countryCode: parsed.countryCallingCode,
        country: parsed.country || 'unknown',
        nationalNumber: parsed.nationalNumber,
        type: parsed.getType(),
        isMobile: this.isMobileType(parsed.getType())
      };
      
      // Format in different ways
      const formatted = {
        international: parsed.formatInternational(),
        national: parsed.formatNational(),
        e164: parsed.format('E.164'),
        rfc3966: parsed.format('RFC3966')
      };
      
      // Check if the number was modified
      const wasCorrected = this.wasPhoneNumberCorrected(phoneNumber, formatted);
      
      return this.buildValidationResult(phoneNumber, {
        valid: true,
        formatValid: true,
        wasCorrected,
        parsed,
        formatted,
        components
      }, clientId);
      
    } catch (error) {
      this.logger.error('Unexpected error in phone validation', error, {
        phoneNumber
      });
      
      return this.buildValidationResult(phoneNumber, {
        valid: false,
        formatValid: false,
        error: error.message,
        wasCorrected: false
      }, clientId);
    }
  }
  
  // Clean phone number input
  cleanPhoneNumber(phoneNumber) {
    if (!phoneNumber) return '';
    
    // Convert to string and trim
    let cleaned = String(phoneNumber).trim();
    
    // Remove common formatting characters but keep + for international
    cleaned = cleaned.replace(/[\s\-\(\)\.]/g, '');
    
    // Handle common prefixes
    if (cleaned.startsWith('00')) {
      // International format with 00 instead of +
      cleaned = '+' + cleaned.substring(2);
    }
    
    return cleaned;
  }
  
  // Check if phone type is mobile
  isMobileType(type) {
    return type === 'MOBILE' || 
           type === 'FIXED_LINE_OR_MOBILE' || 
           type === 'PERSONAL_NUMBER';
  }
  
  // Check if phone number was corrected
  wasPhoneNumberCorrected(original, formatted) {
    if (!original || !formatted) return false;
    
    // Clean original for comparison
    const cleanedOriginal = this.cleanPhoneNumber(original);
    
    // Compare with E.164 format (most canonical)
    const e164WithoutPlus = formatted.e164.replace('+', '');
    const cleanedWithoutPlus = cleanedOriginal.replace('+', '');
    
    // Also check if the format differs significantly
    const originalDigitsOnly = original.replace(/\D/g, '');
    const e164DigitsOnly = formatted.e164.replace(/\D/g, '');
    
    return cleanedWithoutPlus !== e164WithoutPlus || 
           originalDigitsOnly !== e164DigitsOnly;
  }
  
  // Build validation result
  buildValidationResult(originalPhone, validationData, clientId) {
    const now = new Date();
    const epochMs = now.getTime();
    const umCheckId = this.generateUmCheckId(clientId);
    
    // Determine um_phone (standardized international format)
    let umPhone = originalPhone;
    if (validationData.valid && validationData.formatted) {
      umPhone = validationData.formatted.international;
    }
    
    // Build base result
    const result = {
      // Original input
      originalPhone,
      
      // Validation status
      valid: validationData.valid || false,
      formatValid: validationData.formatValid || false,
      wasCorrected: validationData.wasCorrected || false,
      
      // Unmessy fields
      um_phone: umPhone,
      um_phone_status: validationData.wasCorrected ? 'Changed' : 'Unchanged',
      um_phone_format: validationData.formatValid ? 'Valid' : 'Invalid',
      
      // Timestamps
      date_last_um_check: now.toISOString(),
      date_last_um_check_epoch: epochMs,
      um_check_id: umCheckId
    };
    
    // Add error if present
    if (validationData.error) {
      result.error = validationData.error;
    }
    
    // Add components if valid
    if (validationData.valid && validationData.components) {
      result.um_phone_country_code = `+${validationData.components.countryCode}`;
      result.um_phone_country = validationData.components.country;
      result.um_phone_is_mobile = validationData.components.isMobile;
      result.type = validationData.components.type;
      
      // Add formatted versions
      result.formatted = validationData.formatted;
      
      // Add E.164 as the canonical format
      result.e164 = validationData.formatted.e164;
    }
    
    // Add validation steps for transparency
    result.validationSteps = [
      {
        step: 'format_check',
        passed: validationData.formatValid
      },
      {
        step: 'number_parsing',
        passed: validationData.valid
      },
      {
        step: 'standardization',
        applied: validationData.wasCorrected,
        result: umPhone
      }
    ];
    
    return result;
  }
  
  // Check phone cache
  async checkPhoneCache(phoneNumber) {
    try {
      const e164Phone = this.cleanPhoneNumber(phoneNumber);
      const { data, error } = await db.getPhoneValidation(e164Phone);
      
      if (data) {
        return {
          originalPhone: phoneNumber,
          valid: true,
          formatValid: true,
          wasCorrected: data.original_phone !== phoneNumber,
          um_phone: data.um_phone,
          um_phone_status: data.um_phone_status,
          um_phone_format: data.um_phone_format || 'Valid',
          um_phone_country_code: data.um_phone_country_code,
          um_phone_country: data.um_phone_country,
          um_phone_is_mobile: data.um_phone_is_mobile,
          e164: data.e164_format,
          type: data.phone_type,
          date_last_um_check: data.date_last_um_check,
          date_last_um_check_epoch: data.date_last_um_check_epoch,
          um_check_id: data.um_check_id,
          isFromCache: true
        };
      }
      
      return null;
    } catch (error) {
      this.logger.error('Failed to check phone cache', error, { phoneNumber });
      return null;
    }
  }
  
  // Save phone validation to cache
  async savePhoneCache(phoneNumber, validationResult, clientId) {
    // Only save valid phones
    if (!validationResult.valid) {
      return;
    }
    
    try {
      await db.savePhoneValidation({
        original_phone: phoneNumber,
        um_phone: validationResult.um_phone,
        um_phone_status: validationResult.um_phone_status,
        um_phone_format: validationResult.um_phone_format,
        um_phone_country_code: validationResult.um_phone_country_code,
        um_phone_country: validationResult.um_phone_country,
        um_phone_is_mobile: validationResult.um_phone_is_mobile,
        e164_format: validationResult.e164,
        phone_type: validationResult.type,
        date_last_um_check: validationResult.date_last_um_check,
        date_last_um_check_epoch: validationResult.date_last_um_check_epoch,
        um_check_id: validationResult.um_check_id,
        client_id: clientId
      });
      
      this.logger.debug('Phone validation saved to cache', { 
        phoneNumber, 
        e164: validationResult.e164,
        clientId 
      });
    } catch (error) {
      this.logger.error('Failed to save phone validation', error, { phoneNumber });
    }
  }
  
  // Validate multiple phone numbers
  async validatePhoneNumbers(phoneNumbers, options = {}) {
    const results = [];
    
    for (const phone of phoneNumbers) {
      const result = await this.validatePhoneNumber(phone, options);
      results.push(result);
    }
    
    return results;
  }
  
  // Get supported countries
  getSupportedCountries() {
    return Array.from(this.supportedCountries);
  }
  
  // Check if country is supported
  isCountrySupported(countryCode) {
    return this.supportedCountries.has(countryCode);
  }
}

// Export the class for use in validation-service.js
export { PhoneValidationService };