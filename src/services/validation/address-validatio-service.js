// src/services/validation/address-validation-service.js
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ValidationError, 
  ErrorRecovery 
} from '../../core/errors.js';
import { openCageService } from '../external/opencage.js';

const logger = createServiceLogger('address-validation-service');

class AddressValidationService {
  constructor() {
    this.logger = logger;
    
    // Use the external OpenCage service
    this.openCage = openCageService;
    
    // Common street types and their abbreviations
    this.streetTypes = new Map([
      ['street', 'St'], ['st', 'St'],
      ['avenue', 'Ave'], ['ave', 'Ave'],
      ['road', 'Rd'], ['rd', 'Rd'],
      ['boulevard', 'Blvd'], ['blvd', 'Blvd'],
      ['drive', 'Dr'], ['dr', 'Dr'],
      ['lane', 'Ln'], ['ln', 'Ln'],
      ['court', 'Ct'], ['ct', 'Ct'],
      ['place', 'Pl'], ['pl', 'Pl'],
      ['circle', 'Cir'], ['cir', 'Cir'],
      ['parkway', 'Pkwy'], ['pkwy', 'Pkwy'],
      ['highway', 'Hwy'], ['hwy', 'Hwy'],
      ['square', 'Sq'], ['sq', 'Sq'],
      ['terrace', 'Ter'], ['ter', 'Ter'],
      ['trail', 'Trl'], ['trl', 'Trl'],
      ['way', 'Way']
    ]);
    
    // Common unit types
    this.unitTypes = new Map([
      ['apartment', 'Apt'], ['apt', 'Apt'],
      ['suite', 'Ste'], ['ste', 'Ste'],
      ['unit', 'Unit'],
      ['building', 'Bldg'], ['bldg', 'Bldg'],
      ['floor', 'Fl'], ['fl', 'Fl'],
      ['room', 'Rm'], ['rm', 'Rm']
    ]);
    
    // Street directions
    this.streetDirections = new Map([
      ['north', 'N'], ['n', 'N'],
      ['south', 'S'], ['s', 'S'],
      ['east', 'E'], ['e', 'E'],
      ['west', 'W'], ['w', 'W'],
      ['northeast', 'NE'], ['ne', 'NE'],
      ['northwest', 'NW'], ['nw', 'NW'],
      ['southeast', 'SE'], ['se', 'SE'],
      ['southwest', 'SW'], ['sw', 'SW']
    ]);
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
  async validateAddress(input, options = {}) {
    const { 
      useOpenCage = config.validation.address.geocode,
      clientId = null,
      country = config.validation.address.defaultCountry,
      timeout = config.services.openCage.timeout
    } = options;
    
    this.logger.debug('Starting address validation', { 
      input, 
      useOpenCage, 
      clientId 
    });
    
    // Parse input into components
    const components = this.parseInput(input);
    
    // Check if we have minimum required data
    if (!this.hasMinimumData(components)) {
      return this.buildValidationResult(components, {
        valid: false,
        confidence: 0,
        error: 'Insufficient address data provided',
        wasCorrected: false
      }, clientId);
    }
    
    // Standardize components
    const standardized = this.standardizeComponents(components);
    
    // Build address string for geocoding
    const addressString = this.buildAddressString(standardized);
    
    // Basic validation
    const basicValidation = this.validateComponents(standardized);
    
    // If OpenCage is enabled, enhance with geocoding
    if (useOpenCage && config.services.openCage.enabled) {
      try {
        const geocodeResult = await ErrorRecovery.withTimeout(
          this.openCage.geocode(addressString, {
            countryCode: standardized.um_country_code || country,
            language: 'en'
          }),
          timeout,
          'OpenCage geocoding'
        );
        
        if (geocodeResult.found && geocodeResult.results.length > 0) {
          const topResult = geocodeResult.results[0];
          
          // Merge geocoded components with standardized
          const merged = this.mergeWithGeocodeResult(standardized, topResult);
          
          return this.buildValidationResult(merged, {
            valid: topResult.confidence >= 5,
            confidence: topResult.confidence,
            wasCorrected: this.wasAddressCorrected(components, merged),
            geocoded: true,
            geometry: topResult.geometry,
            formatted: topResult.formatted
          }, clientId);
        }
      } catch (error) {
        this.logger.warn('OpenCage geocoding failed, falling back to basic validation', error, {
          address: addressString
        });
        // Continue with basic validation
      }
    }
    
    // Return basic validation result
    return this.buildValidationResult(standardized, {
      valid: basicValidation.valid,
      confidence: basicValidation.confidence,
      wasCorrected: this.wasAddressCorrected(components, standardized),
      warnings: basicValidation.warnings
    }, clientId);
  }
  
  // Parse input into components
  parseInput(input) {
    const components = {
      um_house_number: '',
      um_street_name: '',
      um_street_type: '',
      um_street_direction: '',
      um_unit_type: '',
      um_unit_number: '',
      um_address_line_1: '',
      um_address_line_2: '',
      um_city: '',
      um_state_province: '',
      um_country: '',
      um_country_code: '',
      um_postal_code: ''
    };
    
    // Handle different input formats
    if (typeof input === 'string') {
      // Parse as single address string
      const parsed = this.parseAddressString(input);
      Object.assign(components, parsed);
    } else if (typeof input === 'object') {
      // Map input fields to um_ fields
      components.um_house_number = input.house_number || input.um_house_number || '';
      components.um_street_name = input.street_name || input.um_street_name || '';
      components.um_street_type = input.street_type || input.um_street_type || '';
      components.um_street_direction = input.street_direction || input.um_street_direction || '';
      components.um_unit_type = input.unit_type || input.um_unit_type || '';
      components.um_unit_number = input.unit_number || input.um_unit_number || '';
      components.um_address_line_1 = input.address_line_1 || input.um_address_line_1 || '';
      components.um_address_line_2 = input.address_line_2 || input.um_address_line_2 || '';
      components.um_city = input.city || input.um_city || '';
      components.um_state_province = input.state || input.state_province || input.um_state_province || '';
      components.um_country = input.country || input.um_country || '';
      components.um_country_code = input.country_code || input.um_country_code || '';
      components.um_postal_code = input.postal_code || input.zip || input.um_postal_code || '';
      
      // If address lines aren't provided, build them from components
      if (!components.um_address_line_1 && (components.um_house_number || components.um_street_name)) {
        components.um_address_line_1 = this.buildAddressLine1(components);
      }
      if (!components.um_address_line_2 && (components.um_unit_type || components.um_unit_number)) {
        components.um_address_line_2 = this.buildAddressLine2(components);
      }
    }
    
    return components;
  }
  
  // Parse address string (basic implementation)
  parseAddressString(address) {
    // This is a simplified parser - in production you might use a more sophisticated library
    const components = {
      um_address_line_1: '',
      um_city: '',
      um_state_province: '',
      um_postal_code: '',
      um_country: ''
    };
    
    // Split by commas
    const parts = address.split(',').map(p => p.trim());
    
    if (parts.length >= 1) {
      components.um_address_line_1 = parts[0];
    }
    if (parts.length >= 2) {
      components.um_city = parts[1];
    }
    if (parts.length >= 3) {
      // Try to parse state and zip from third part
      const stateZip = parts[2].trim();
      const zipMatch = stateZip.match(/\b(\d{5}(-\d{4})?)\b/);
      if (zipMatch) {
        components.um_postal_code = zipMatch[1];
        components.um_state_province = stateZip.replace(zipMatch[0], '').trim();
      } else {
        components.um_state_province = stateZip;
      }
    }
    if (parts.length >= 4) {
      components.um_country = parts[3];
    }
    
    return components;
  }
  
  // Check if we have minimum required data
  hasMinimumData(components) {
    return !!(
      (components.um_address_line_1 || components.um_street_name) &&
      components.um_city
    );
  }
  
  // Standardize components
  standardizeComponents(components) {
    const standardized = { ...components };
    
    // Standardize street type
    if (standardized.um_street_type) {
      const lower = standardized.um_street_type.toLowerCase();
      if (this.streetTypes.has(lower)) {
        standardized.um_street_type = this.streetTypes.get(lower);
      }
    }
    
    // Standardize street direction
    if (standardized.um_street_direction) {
      const lower = standardized.um_street_direction.toLowerCase();
      if (this.streetDirections.has(lower)) {
        standardized.um_street_direction = this.streetDirections.get(lower);
      }
    }
    
    // Standardize unit type
    if (standardized.um_unit_type) {
      const lower = standardized.um_unit_type.toLowerCase().replace(/[.#]/g, '');
      if (this.unitTypes.has(lower)) {
        standardized.um_unit_type = this.unitTypes.get(lower);
      }
    }
    
    // Title case city
    if (standardized.um_city) {
      standardized.um_city = this.titleCase(standardized.um_city);
    }
    
    // Uppercase state/country codes
    if (standardized.um_state_province && standardized.um_state_province.length <= 3) {
      standardized.um_state_province = standardized.um_state_province.toUpperCase();
    }
    if (standardized.um_country_code) {
      standardized.um_country_code = standardized.um_country_code.toUpperCase();
    }
    
    // Format postal code
    if (standardized.um_postal_code && standardized.um_country_code) {
      standardized.um_postal_code = this.formatPostalCode(
        standardized.um_postal_code, 
        standardized.um_country_code
      );
    }
    
    // Rebuild address lines
    if (!standardized.um_address_line_1 && standardized.um_street_name) {
      standardized.um_address_line_1 = this.buildAddressLine1(standardized);
    }
    if (!standardized.um_address_line_2 && (standardized.um_unit_type || standardized.um_unit_number)) {
      standardized.um_address_line_2 = this.buildAddressLine2(standardized);
    }
    
    return standardized;
  }
  
  // Build address line 1
  buildAddressLine1(components) {
    const parts = [];
    if (components.um_house_number) parts.push(components.um_house_number);
    if (components.um_street_direction) parts.push(components.um_street_direction);
    if (components.um_street_name) parts.push(components.um_street_name);
    if (components.um_street_type) parts.push(components.um_street_type);
    return parts.join(' ');
  }
  
  // Build address line 2
  buildAddressLine2(components) {
    const parts = [];
    if (components.um_unit_type) parts.push(components.um_unit_type);
    if (components.um_unit_number) parts.push(components.um_unit_number);
    return parts.join(' ');
  }
  
  // Build full address string
  buildAddressString(components) {
    const parts = [];
    if (components.um_address_line_1) parts.push(components.um_address_line_1);
    if (components.um_address_line_2) parts.push(components.um_address_line_2);
    if (components.um_city) parts.push(components.um_city);
    
    const stateZip = [];
    if (components.um_state_province) stateZip.push(components.um_state_province);
    if (components.um_postal_code) stateZip.push(components.um_postal_code);
    if (stateZip.length > 0) parts.push(stateZip.join(' '));
    
    if (components.um_country && components.um_country_code !== 'US') {
      parts.push(components.um_country);
    }
    
    return parts.join(', ');
  }
  
  // Validate components
  validateComponents(components) {
    const warnings = [];
    let confidence = 10;
    
    // Check required fields
    if (!components.um_street_name && !components.um_address_line_1) {
      warnings.push('Missing street address');
      confidence -= 3;
    }
    
    if (!components.um_city) {
      warnings.push('Missing city');
      confidence -= 2;
    }
    
    if (!components.um_state_province) {
      warnings.push('Missing state/province');
      confidence -= 2;
    }
    
    if (!components.um_postal_code) {
      warnings.push('Missing postal code');
      confidence -= 1;
    }
    
    // Validate postal code format if we have country
    if (components.um_postal_code && components.um_country_code) {
      if (!this.isValidPostalCode(components.um_postal_code, components.um_country_code)) {
        warnings.push('Invalid postal code format');
        confidence -= 2;
      }
    }
    
    return {
      valid: confidence >= 5,
      confidence: Math.max(0, confidence),
      warnings
    };
  }
  
  // Check if address was corrected
  wasAddressCorrected(original, standardized) {
    // Compare key fields
    const fields = [
      'um_house_number', 'um_street_name', 'um_street_type',
      'um_city', 'um_state_province', 'um_postal_code'
    ];
    
    for (const field of fields) {
      const origValue = (original[field] || '').toLowerCase().trim();
      const stdValue = (standardized[field] || '').toLowerCase().trim();
      if (origValue && stdValue && origValue !== stdValue) {
        return true;
      }
    }
    
    return false;
  }
  
  // Merge with geocode result
  mergeWithGeocodeResult(standardized, geocodeResult) {
    const merged = { ...standardized };
    const geocoded = geocodeResult.components;
    
    // Only update fields if geocoded has better data
    if (geocoded.um_house_number && !merged.um_house_number) {
      merged.um_house_number = geocoded.um_house_number;
    }
    if (geocoded.um_street_name) {
      merged.um_street_name = geocoded.um_street_name;
    }
    if (geocoded.um_street_type && !merged.um_street_type) {
      merged.um_street_type = geocoded.um_street_type;
    }
    if (geocoded.um_city) {
      merged.um_city = geocoded.um_city;
    }
    if (geocoded.um_state_province) {
      merged.um_state_province = geocoded.um_state_province;
    }
    if (geocoded.um_postal_code) {
      merged.um_postal_code = geocoded.um_postal_code;
    }
    if (geocoded.um_country) {
      merged.um_country = geocoded.um_country;
    }
    if (geocoded.um_country_code) {
      merged.um_country_code = geocoded.um_country_code;
    }
    
    // Rebuild address lines with geocoded data
    merged.um_address_line_1 = this.buildAddressLine1(merged);
    
    return merged;
  }
  
  // Build validation result
  buildValidationResult(components, validationData, clientId) {
    const now = new Date();
    const epochMs = now.getTime();
    const umCheckId = this.generateUmCheckId(clientId);
    
    const result = {
      // All um_address fields
      ...components,
      
      // Validation status
      valid: validationData.valid || false,
      confidence: validationData.confidence || 0,
      wasCorrected: validationData.wasCorrected || false,
      
      // Address status
      um_address_status: validationData.wasCorrected ? 'Changed' : 'Unchanged',
      
      // Formatted full address
      formatted_address: this.buildAddressString(components),
      
      // Timestamps
      date_last_um_check: now.toISOString(),
      date_last_um_check_epoch: epochMs,
      um_check_id: umCheckId
    };
    
    // Add error if present
    if (validationData.error) {
      result.error = validationData.error;
    }
    
    // Add warnings if present
    if (validationData.warnings && validationData.warnings.length > 0) {
      result.warnings = validationData.warnings;
    }
    
    // Add geocoding info if available
    if (validationData.geocoded) {
      result.geocoded = true;
      if (validationData.geometry) {
        result.geometry = validationData.geometry;
      }
      if (validationData.formatted) {
        result.geocoded_address = validationData.formatted;
      }
    }
    
    // Add validation steps
    result.validationSteps = [
      {
        step: 'parsing',
        passed: true
      },
      {
        step: 'standardization',
        applied: validationData.wasCorrected
      },
      {
        step: 'validation',
        passed: validationData.valid,
        confidence: validationData.confidence
      }
    ];
    
    if (validationData.geocoded !== undefined) {
      result.validationSteps.push({
        step: 'geocoding',
        attempted: true,
        success: validationData.geocoded
      });
    }
    
    return result;
  }
  
  // Validate postal code
  isValidPostalCode(postalCode, countryCode) {
    const patterns = {
      'US': /^\d{5}(-\d{4})?$/,
      'CA': /^[A-Z]\d[A-Z]\s?\d[A-Z]\d$/i,
      'GB': /^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$/i,
      'AU': /^\d{4}$/,
      'DE': /^\d{5}$/,
      'FR': /^\d{5}$/,
      'IT': /^\d{5}$/,
      'ES': /^\d{5}$/,
      'NL': /^\d{4}\s?[A-Z]{2}$/i,
      'JP': /^\d{3}-?\d{4}$/
    };
    
    const pattern = patterns[countryCode];
    if (!pattern) return true; // Don't validate unknown countries
    
    return pattern.test(postalCode);
  }
  
  // Format postal code
  formatPostalCode(postalCode, countryCode) {
    if (!postalCode) return '';
    
    const cleaned = postalCode.replace(/\s+/g, '').toUpperCase();
    
    switch (countryCode) {
      case 'US':
        if (cleaned.length === 9) {
          return `${cleaned.slice(0, 5)}-${cleaned.slice(5)}`;
        }
        return cleaned.slice(0, 5);
        
      case 'CA':
        if (cleaned.length === 6) {
          return `${cleaned.slice(0, 3)} ${cleaned.slice(3)}`;
        }
        return cleaned;
        
      case 'GB':
        if (cleaned.length >= 5) {
          return `${cleaned.slice(0, -3)} ${cleaned.slice(-3)}`;
        }
        return cleaned;
        
      default:
        return postalCode;
    }
  }
  
  // Title case helper
  titleCase(str) {
    return str.replace(/\w\S*/g, txt => 
      txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
    );
  }
  
  // Cache operations
  async checkAddressCache(addressKey) {
    try {
      const { data, error } = await db.getAddressValidation(addressKey);
      
      if (data) {
        return {
          ...data,
          isFromCache: true
        };
      }
      
      return null;
    } catch (error) {
      this.logger.error('Failed to check address cache', error, { addressKey });
      return null;
    }
  }
  
  async saveAddressCache(address, validationResult, clientId) {
    // Only save valid addresses
    if (!validationResult.valid) {
      return;
    }
    
    try {
      await db.saveAddressValidation({
        ...validationResult,
        client_id: clientId
      });
      
      this.logger.debug('Address validation saved to cache', { 
        address: validationResult.formatted_address,
        clientId 
      });
    } catch (error) {
      this.logger.error('Failed to save address validation', error);
    }
  }
  
  // Generate cache key for address
  generateCacheKey(components) {
    const parts = [
      components.um_address_line_1,
      components.um_address_line_2,
      components.um_city,
      components.um_state_province,
      components.um_postal_code,
      components.um_country_code
    ].filter(Boolean).map(p => p.toLowerCase().trim());
    
    return parts.join('|');
  }
}

// Export the class for use in validation-service.js
export { AddressValidationService };