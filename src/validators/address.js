// src/services/validation/address-validation-service.js
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ValidationError, 
  ExternalServiceError,
  CircuitBreaker,
  ErrorRecovery 
} from '../../core/errors.js';

const logger = createServiceLogger('address-validation-service');

class AddressValidationService {
  constructor() {
    this.logger = logger;
    
    // Initialize circuit breaker for OpenCage
    this.openCageCircuitBreaker = new CircuitBreaker({
      name: 'OpenCage',
      failureThreshold: 5,
      resetTimeout: 60000,
      monitoringPeriod: 10000
    });
    
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
  
  // Main validation function
  async validateAddress(input, options = {}) {
    const { useOpenCage = true, clientId = null } = options;
    
    this.logger.debug('Starting address validation', { 
      input, 
      useOpenCage, 
      clientId 
    });
    
    // Build address string from components
    const addressString = this.buildAddressString(input);
    
    if (!addressString) {
      return {
        valid: false,
        confidence: 0,
        error: 'No address data provided',
        components: this.createEmptyComponents()
      };
    }
    
    // Parse and standardize components
    let components = this.parseAddressComponents(input, addressString);
    
    // Standardize components
    components = this.standardizeComponents(components);
    
    // If OpenCage is enabled and available, enhance with geocoding
    if (useOpenCage && config.services.openCage.enabled && config.services.openCage.apiKey) {
      try {
        const openCageResult = await this.geocodeWithOpenCage(addressString, components);
        
        // Merge OpenCage results with parsed components
        components = this.mergeWithOpenCageResults(components, openCageResult);
        
        return {
          valid: openCageResult.confidence > 5,
          confidence: openCageResult.confidence,
          formatted_address: openCageResult.formatted,
          components: components,
          geocode: {
            lat: openCageResult.geometry.lat,
            lng: openCageResult.geometry.lng
          },
          source: 'opencage',
          standardized: true,
          corrected: this.wasAddressCorrected(input, components)
        };
        
      } catch (error) {
        this.logger.error('OpenCage geocoding failed', error, { addressString });
        // Fall back to internal validation
      }
    }
    
    // Internal validation only
    const validationResult = this.validateComponents(components);
    
    return {
      valid: validationResult.valid,
      confidence: validationResult.confidence,
      formatted_address: this.formatAddress(components),
      components: components,
      source: 'internal',
      standardized: true,
      corrected: this.wasAddressCorrected(input, components),
      warnings: validationResult.warnings
    };
  }
  
  // Build address string from various input formats
  buildAddressString(input) {
    // If full address provided, use it
    if (input.address) {
      return input.address.trim();
    }
    
    // Build from components or address lines
    const parts = [];
    
    // Address line 1 or components
    if (input.address_line_1) {
      parts.push(input.address_line_1);
    } else {
      const line1Parts = [];
      if (input.house_number) line1Parts.push(input.house_number);
      if (input.street_direction && !input.street_name?.includes(input.street_direction)) {
        line1Parts.push(input.street_direction);
      }
      if (input.street_name) line1Parts.push(input.street_name);
      if (input.street_type) line1Parts.push(input.street_type);
      if (line1Parts.length > 0) {
        parts.push(line1Parts.join(' '));
      }
    }
    
    // Address line 2 or unit components
    if (input.address_line_2) {
      parts.push(input.address_line_2);
    } else if (input.unit_type || input.unit_number) {
      const line2Parts = [];
      if (input.unit_type) line2Parts.push(input.unit_type);
      if (input.unit_number) line2Parts.push(input.unit_number);
      parts.push(line2Parts.join(' '));
    }
    
    // City, state, postal code
    if (input.city) parts.push(input.city);
    if (input.state || input.state_province) parts.push(input.state || input.state_province);
    if (input.postal_code) parts.push(input.postal_code);
    if (input.country) parts.push(input.country);
    
    return parts.filter(Boolean).join(', ');
  }
  
  // Parse address into components
  parseAddressComponents(input, addressString) {
    const components = {
      house_number: input.house_number || '',
      street_name: input.street_name || '',
      street_type: input.street_type || '',
      street_direction: input.street_direction || '',
      unit_type: input.unit_type || '',
      unit_number: input.unit_number || '',
      city: input.city || '',
      state_province: input.state || input.state_province || '',
      country: input.country || '',
      country_code: input.country_code || '',
      postal_code: input.postal_code || ''
    };
    
    // If we have address lines but not components, try to parse
    if (input.address_line_1 && !components.street_name) {
      const parsed = this.parseAddressLine1(input.address_line_1);
      Object.assign(components, parsed);
    }
    
    if (input.address_line_2 && !components.unit_number) {
      const parsed = this.parseAddressLine2(input.address_line_2);
      if (parsed.unit_type) components.unit_type = parsed.unit_type;
      if (parsed.unit_number) components.unit_number = parsed.unit_number;
    }
    
    // Generate address lines from components
    components.address_line_1 = this.buildAddressLine1(components);
    components.address_line_2 = this.buildAddressLine2(components);
    
    return components;
  }
  
  // Parse address line 1 (street address)
  parseAddressLine1(line1) {
    if (!line1) return {};
    
    const components = {};
    const parts = line1.trim().split(/\s+/);
    
    // Check if first part is a number (house number)
    if (parts.length > 0 && /^\d+/.test(parts[0])) {
      components.house_number = parts[0];
      parts.shift();
    }
    
    // Check for direction at beginning
    if (parts.length > 0) {
      const firstPart = parts[0].toLowerCase();
      if (this.streetDirections.has(firstPart)) {
        components.street_direction = this.streetDirections.get(firstPart);
        parts.shift();
      }
    }
    
    // Check for street type at end
    if (parts.length > 0) {
      const lastPart = parts[parts.length - 1].toLowerCase();
      if (this.streetTypes.has(lastPart)) {
        components.street_type = this.streetTypes.get(lastPart);
        parts.pop();
      }
    }
    
    // Remaining parts are street name
    if (parts.length > 0) {
      components.street_name = parts.join(' ');
    }
    
    return components;
  }
  
  // Parse address line 2 (unit information)
  parseAddressLine2(line2) {
    if (!line2) return {};
    
    const components = {};
    const parts = line2.trim().split(/\s+/);
    
    // Check for unit type
    if (parts.length > 0) {
      const firstPart = parts[0].toLowerCase();
      if (this.unitTypes.has(firstPart)) {
        components.unit_type = this.unitTypes.get(firstPart);
        parts.shift();
        
        // Remaining is unit number
        if (parts.length > 0) {
          components.unit_number = parts.join(' ');
        }
      } else if (/^(apt|suite|unit|ste|#)/i.test(firstPart)) {
        // Handle variations like "Apt.", "#", etc.
        components.unit_type = this.standardizeUnitType(firstPart);
        parts.shift();
        if (parts.length > 0) {
          components.unit_number = parts.join(' ');
        }
      } else {
        // Assume it's just a unit number
        components.unit_number = line2;
      }
    }
    
    return components;
  }
  
  // Standardize components
  standardizeComponents(components) {
    const standardized = { ...components };
    
    // Standardize street type
    if (standardized.street_type) {
      const lower = standardized.street_type.toLowerCase();
      if (this.streetTypes.has(lower)) {
        standardized.street_type = this.streetTypes.get(lower);
      }
    }
    
    // Standardize street direction
    if (standardized.street_direction) {
      const lower = standardized.street_direction.toLowerCase();
      if (this.streetDirections.has(lower)) {
        standardized.street_direction = this.streetDirections.get(lower);
      }
    }
    
    // Standardize unit type
    if (standardized.unit_type) {
      standardized.unit_type = this.standardizeUnitType(standardized.unit_type);
    }
    
    // Capitalize city
    if (standardized.city) {
      standardized.city = this.titleCase(standardized.city);
    }
    
    // Uppercase state/province if 2-3 letters
    if (standardized.state_province && standardized.state_province.length <= 3) {
      standardized.state_province = standardized.state_province.toUpperCase();
    }
    
    // Format postal code
    if (standardized.postal_code) {
      standardized.postal_code = this.formatPostalCode(standardized.postal_code, standardized.country_code);
    }
    
    // Update address lines
    standardized.address_line_1 = this.buildAddressLine1(standardized);
    standardized.address_line_2 = this.buildAddressLine2(standardized);
    
    return standardized;
  }
  
  // Standardize unit type
  standardizeUnitType(unitType) {
    const lower = unitType.toLowerCase().replace(/[.#]/g, '');
    if (this.unitTypes.has(lower)) {
      return this.unitTypes.get(lower);
    }
    // Default formatting
    return unitType.charAt(0).toUpperCase() + unitType.slice(1).toLowerCase();
  }
  
  // Build address line 1 from components
  buildAddressLine1(components) {
    const parts = [];
    
    if (components.house_number) parts.push(components.house_number);
    if (components.street_direction && !components.street_name?.includes(components.street_direction)) {
      parts.push(components.street_direction);
    }
    if (components.street_name) parts.push(components.street_name);
    if (components.street_type) parts.push(components.street_type);
    
    return parts.join(' ');
  }
  
  // Build address line 2 from components
  buildAddressLine2(components) {
    const parts = [];
    
    if (components.unit_type) parts.push(components.unit_type);
    if (components.unit_number) parts.push(components.unit_number);
    
    return parts.join(' ');
  }
  
  // Geocode with OpenCage
  async geocodeWithOpenCage(addressString, components) {
    const apiKey = config.services.openCage.apiKey;
    if (!apiKey) {
      throw new ValidationError('OpenCage API key not configured');
    }
    
    return this.openCageCircuitBreaker.execute(async () => {
      const params = new URLSearchParams({
        key: apiKey,
        q: addressString,
        limit: 1,
        no_annotations: 0,
        language: 'en'
      });
      
      // Add country hint if available
      if (components.country_code) {
        params.append('countrycode', components.country_code.toLowerCase());
      }
      
      const url = `${config.services.openCage.baseUrl}/json?${params}`;
      
      this.logger.debug('Calling OpenCage API', { addressString });
      
      const response = await ErrorRecovery.withTimeout(
        fetch(url),
        config.services.openCage.timeout,
        'OpenCage API'
      );
      
      if (!response.ok) {
        throw new ExternalServiceError(
          `OpenCage API error: ${response.statusText}`,
          'OpenCage',
          response.status
        );
      }
      
      const data = await response.json();
      
      if (!data.results || data.results.length === 0) {
        throw new ValidationError('No results found for address');
      }
      
      const result = data.results[0];
      
      this.logger.debug('OpenCage response received', {
        confidence: result.confidence,
        formatted: result.formatted
      });
      
      return result;
    });
  }
  
  // Merge OpenCage results with parsed components
  mergeWithOpenCageResults(components, openCageResult) {
    const merged = { ...components };
    const ocComponents = openCageResult.components || {};
    
    // Extract components from OpenCage
    if (ocComponents.house_number) {
      merged.house_number = ocComponents.house_number;
    }
    
    if (ocComponents.road) {
      // Parse the road to extract street name and type
      const roadParts = this.parseAddressLine1(ocComponents.road);
      if (roadParts.street_name) merged.street_name = roadParts.street_name;
      if (roadParts.street_type) merged.street_type = roadParts.street_type;
    }
    
    if (ocComponents.city || ocComponents.town || ocComponents.village) {
      merged.city = ocComponents.city || ocComponents.town || ocComponents.village;
    }
    
    if (ocComponents.state || ocComponents.province) {
      merged.state_province = ocComponents.state_code || ocComponents.state || ocComponents.province;
    }
    
    if (ocComponents.postcode) {
      merged.postal_code = ocComponents.postcode;
    }
    
    if (ocComponents.country) {
      merged.country = ocComponents.country;
    }
    
    if (ocComponents.country_code) {
      merged.country_code = ocComponents.country_code.toUpperCase();
    }
    
    // Rebuild address lines
    merged.address_line_1 = this.buildAddressLine1(merged);
    merged.address_line_2 = this.buildAddressLine2(merged);
    
    return merged;
  }
  
  // Validate components
  validateComponents(components) {
    const warnings = [];
    let confidence = 10;
    
    // Check required fields
    if (!components.street_name) {
      warnings.push('Missing street name');
      confidence -= 3;
    }
    
    if (!components.city) {
      warnings.push('Missing city');
      confidence -= 2;
    }
    
    if (!components.state_province) {
      warnings.push('Missing state/province');
      confidence -= 2;
    }
    
    if (!components.postal_code) {
      warnings.push('Missing postal code');
      confidence -= 1;
    }
    
    // Validate postal code format
    if (components.postal_code && components.country_code) {
      if (!this.isValidPostalCode(components.postal_code, components.country_code)) {
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
  wasAddressCorrected(input, components) {
    // Check if any component differs from input
    if (input.house_number && input.house_number !== components.house_number) return true;
    if (input.street_name && input.street_name.toLowerCase() !== components.street_name.toLowerCase()) return true;
    if (input.street_type && input.street_type.toLowerCase() !== components.street_type.toLowerCase()) return true;
    if (input.city && input.city.toLowerCase() !== components.city.toLowerCase()) return true;
    if (input.postal_code && input.postal_code.replace(/\s+/g, '') !== components.postal_code.replace(/\s+/g, '')) return true;
    
    return false;
  }
  
  // Format address for display
  formatAddress(components) {
    const parts = [];
    
    if (components.address_line_1) parts.push(components.address_line_1);
    if (components.address_line_2) parts.push(components.address_line_2);
    if (components.city) parts.push(components.city);
    
    const stateZip = [];
    if (components.state_province) stateZip.push(components.state_province);
    if (components.postal_code) stateZip.push(components.postal_code);
    if (stateZip.length > 0) parts.push(stateZip.join(' '));
    
    if (components.country && components.country_code !== 'US') {
      parts.push(components.country);
    }
    
    return parts.join(', ');
  }
  
  // Validate postal code format
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
        // Format as 12345 or 12345-6789
        if (cleaned.length === 9) {
          return `${cleaned.slice(0, 5)}-${cleaned.slice(5)}`;
        }
        return cleaned.slice(0, 5);
        
      case 'CA':
        // Format as A1A 1A1
        if (cleaned.length === 6) {
          return `${cleaned.slice(0, 3)} ${cleaned.slice(3)}`;
        }
        return cleaned;
        
      case 'GB':
        // Format with space before last 3 characters
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
  
  // Create empty components
  createEmptyComponents() {
    return {
      house_number: '',
      street_name: '',
      street_type: '',
      street_direction: '',
      unit_type: '',
      unit_number: '',
      address_line_1: '',
      address_line_2: '',
      city: '',
      state_province: '',
      country: '',
      country_code: '',
      postal_code: ''
    };
  }
}

// Create singleton instance
const addressValidationService = new AddressValidationService();

export { addressValidationService, AddressValidationService };