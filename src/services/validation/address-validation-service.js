// src/services/validation/address-validation-service.js
import db from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ValidationError, 
  ErrorRecovery 
} from '../../core/errors.js';
import { openCageService } from '../external/opencage.js';

const logger = createServiceLogger('address-validation-service');

export class AddressValidationService {
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
    
    // State abbreviations map
    this.stateAbbreviations = new Map([
      ['alabama', 'AL'], ['alaska', 'AK'], ['arizona', 'AZ'], ['arkansas', 'AR'],
      ['california', 'CA'], ['colorado', 'CO'], ['connecticut', 'CT'], ['delaware', 'DE'],
      ['florida', 'FL'], ['georgia', 'GA'], ['hawaii', 'HI'], ['idaho', 'ID'],
      ['illinois', 'IL'], ['indiana', 'IN'], ['iowa', 'IA'], ['kansas', 'KS'],
      ['kentucky', 'KY'], ['louisiana', 'LA'], ['maine', 'ME'], ['maryland', 'MD'],
      ['massachusetts', 'MA'], ['michigan', 'MI'], ['minnesota', 'MN'], ['mississippi', 'MS'],
      ['missouri', 'MO'], ['montana', 'MT'], ['nebraska', 'NE'], ['nevada', 'NV'],
      ['new hampshire', 'NH'], ['new jersey', 'NJ'], ['new mexico', 'NM'], ['new york', 'NY'],
      ['north carolina', 'NC'], ['north dakota', 'ND'], ['ohio', 'OH'], ['oklahoma', 'OK'],
      ['oregon', 'OR'], ['pennsylvania', 'PA'], ['rhode island', 'RI'], ['south carolina', 'SC'],
      ['south dakota', 'SD'], ['tennessee', 'TN'], ['texas', 'TX'], ['utah', 'UT'],
      ['vermont', 'VT'], ['virginia', 'VA'], ['washington', 'WA'], ['west virginia', 'WV'],
      ['wisconsin', 'WI'], ['wyoming', 'WY']
    ]);
  }
  
  // Load normalization data (called by validation-service.js)
  async loadNormalizationData() {
    try {
      this.logger.info('Address validation service ready');
      // Additional normalization data could be loaded from database here
      return true;
    } catch (error) {
      this.logger.error('Failed to load address normalization data', error);
      throw error;
    }
  }
  
  // Generate um_check_id
  generateUmCheckId(clientId) {
    const epochTime = Date.now();
    const lastSixDigits = String(epochTime).slice(-6);
    const clientIdStr = clientId || config.clients.defaultClientId || '0001';
    const firstThreeDigits = String(epochTime).slice(0, 3);
    const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
    const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
    return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${config.unmessy.version.replace(/\./g, '')}`);
  }
  
  // Main validation function
  async validateAddress(input, options = {}) {
    const { 
      useOpenCage = config.validation.address.geocode && config.services.openCage.enabled,
      clientId = null,
      country = config.validation.address.defaultCountry,
      timeout = config.services.openCage.timeout
    } = options;
    
    this.logger.debug('Starting address validation', { 
      input: typeof input === 'string' ? input : 'object', 
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
    if (useOpenCage && this.openCage) {
      try {
        const geocodeResult = await ErrorRecovery.withTimeout(
          this.openCage.geocode(addressString, {
            countryCode: standardized.um_country_code || country,
            language: 'en'
          }),
          timeout,
          'OpenCage geocoding'
        );
        
        if (geocodeResult && geocodeResult.found) {
          // Merge geocoded components with standardized
          const merged = this.mergeWithGeocodeResult(standardized, geocodeResult);
          
          return this.buildValidationResult(merged, {
            valid: geocodeResult.confidence >= 5,
            confidence: geocodeResult.confidence,
            wasCorrected: this.wasAddressCorrected(components, merged),
            geocoded: true,
            geometry: geocodeResult.coordinates,
            formatted: geocodeResult.formattedAddress
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
    } else if (typeof input === 'object' && input !== null) {
      // Map input fields to um_ fields
      components.um_house_number = input.house_number || input.um_house_number || '';
      components.um_street_name = input.street_name || input.um_street_name || '';
      components.um_street_type = input.street_type || input.um_street_type || '';
      components.um_street_direction = input.street_direction || input.um_street_direction || '';
      components.um_unit_type = input.unit_type || input.um_unit_type || '';
      components.um_unit_number = input.unit_number || input.um_unit_number || '';
      components.um_address_line_1 = input.address_line_1 || input.address || input.um_address_line_1 || '';
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
      
      // Parse address line 1 if we have it but not the components
      if (components.um_address_line_1 && !components.um_street_name) {
        const parsedLine1 = this.parseAddressLine1(components.um_address_line_1);
        if (!components.um_house_number) components.um_house_number = parsedLine1.houseNumber;
        if (!components.um_street_direction) components.um_street_direction = parsedLine1.streetDirection;
        if (!components.um_street_name) components.um_street_name = parsedLine1.streetName;
        if (!components.um_street_type) components.um_street_type = parsedLine1.streetType;
      }
    }
    
    return components;
  }
  
  // Parse address string (improved implementation)
  parseAddressString(address) {
    const components = {
      um_address_line_1: '',
      um_address_line_2: '',
      um_city: '',
      um_state_province: '',
      um_postal_code: '',
      um_country: ''
    };
    
    if (!address) return components;
    
    // Split by commas
    const parts = address.split(',').map(p => p.trim()).filter(Boolean);
    
    if (parts.length >= 1) {
      // Check if first part might contain unit info
      const unitMatch = parts[0].match(/(.+?)\s+(apt|suite|ste|unit|#)\s*(.+)/i);
      if (unitMatch) {
        components.um_address_line_1 = unitMatch[1].trim();
        components.um_address_line_2 = `${unitMatch[2]} ${unitMatch[3]}`.trim();
      } else {
        components.um_address_line_1 = parts[0];
      }
    }
    
    if (parts.length >= 2) {
      components.um_city = parts[1];
    }
    
    if (parts.length >= 3) {
      // Try to parse state and zip from third part
      const stateZip = parts[2].trim();
      
      // US ZIP code pattern
      const usZipMatch = stateZip.match(/\b(\d{5}(-\d{4})?)\b/);
      // Canadian postal code pattern
      const caPostalMatch = stateZip.match(/\b([A-Z]\d[A-Z]\s?\d[A-Z]\d)\b/i);
      
      if (usZipMatch) {
        components.um_postal_code = usZipMatch[1];
        components.um_state_province = stateZip.replace(usZipMatch[0], '').trim();
      } else if (caPostalMatch) {
        components.um_postal_code = caPostalMatch[1].toUpperCase();
        components.um_state_province = stateZip.replace(caPostalMatch[0], '').trim();
      } else {
        // Try to identify state by checking last word
        const words = stateZip.split(/\s+/);
        if (words.length > 1) {
          const lastWord = words[words.length - 1];
          // Check if last word might be a postal code
          if (/^\d{4,}/.test(lastWord) || /^[A-Z]\d[A-Z]/i.test(lastWord)) {
            components.um_postal_code = lastWord;
            components.um_state_province = words.slice(0, -1).join(' ');
          } else {
            components.um_state_province = stateZip;
          }
        } else {
          components.um_state_province = stateZip;
        }
      }
    }
    
    if (parts.length >= 4) {
      components.um_country = parts[3];
    }
    
    return components;
  }
  
  // Parse address line 1 into components
  parseAddressLine1(line1) {
    const result = {
      houseNumber: '',
      streetDirection: '',
      streetName: '',
      streetType: ''
    };
    
    if (!line1) return result;
    
    const tokens = line1.trim().split(/\s+/);
    if (tokens.length === 0) return result;
    
    let currentIndex = 0;
    
    // Check for house number (could be at start)
    if (/^\d+[A-Za-z]?$/.test(tokens[0])) {
      result.houseNumber = tokens[0];
      currentIndex = 1;
    }
    
    // Check for pre-directional
    if (currentIndex < tokens.length) {
      const token = tokens[currentIndex].toLowerCase();
      if (this.streetDirections.has(token)) {
        result.streetDirection = this.streetDirections.get(token);
        currentIndex++;
      }
    }
    
    // Find street type from the end
    let streetTypeIndex = -1;
    for (let i = tokens.length - 1; i >= currentIndex; i--) {
      const token = tokens[i].toLowerCase().replace(/[.,]/, '');
      if (this.streetTypes.has(token)) {
        streetTypeIndex = i;
        result.streetType = this.streetTypes.get(token);
        break;
      }
    }
    
    // Everything between current index and street type is street name
    if (streetTypeIndex > currentIndex) {
      result.streetName = tokens.slice(currentIndex, streetTypeIndex).join(' ');
    } else if (streetTypeIndex === -1) {
      // No street type found, rest is street name
      result.streetName = tokens.slice(currentIndex).join(' ');
    }
    
    return result;
  }
  
  // Check if we have minimum required data
  hasMinimumData(components) {
    // Need at least a street/address line and city
    return !!(
      (components.um_address_line_1 || components.um_street_name) &&
      (components.um_city || components.um_state_province || components.um_postal_code)
    );
  }
  
  // Standardize components
  standardizeComponents(components) {
    const standardized = { ...components };
    
    // Standardize street type
    if (standardized.um_street_type) {
      const lower = standardized.um_street_type.toLowerCase().replace(/[.,]/, '');
      if (this.streetTypes.has(lower)) {
        standardized.um_street_type = this.streetTypes.get(lower);
      }
    }
    
    // Standardize street direction
    if (standardized.um_street_direction) {
      const lower = standardized.um_street_direction.toLowerCase().replace(/[.,]/, '');
      if (this.streetDirections.has(lower)) {
        standardized.um_street_direction = this.streetDirections.get(lower);
      }
    }
    
    // Standardize unit type
    if (standardized.um_unit_type) {
      const lower = standardized.um_unit_type.toLowerCase().replace(/[.#]/, '');
      if (this.unitTypes.has(lower)) {
        standardized.um_unit_type = this.unitTypes.get(lower);
      }
    }
    
    // Standardize state/province
    if (standardized.um_state_province) {
      const lower = standardized.um_state_province.toLowerCase();
      // Check if it's a full state name that should be abbreviated
      if (this.stateAbbreviations.has(lower)) {
        standardized.um_state_province = this.stateAbbreviations.get(lower);
      } else if (standardized.um_state_province.length === 2) {
        // Already abbreviated, just uppercase
        standardized.um_state_province = standardized.um_state_province.toUpperCase();
      }
    }
    
    // Title case city
    if (standardized.um_city) {
      standardized.um_city = this.titleCase(standardized.um_city);
    }
    
    // Uppercase country code
    if (standardized.um_country_code) {
      standardized.um_country_code = standardized.um_country_code.toUpperCase();
    }
    
    // Format postal code
    if (standardized.um_postal_code) {
      standardized.um_postal_code = this.formatPostalCode(
        standardized.um_postal_code, 
        standardized.um_country_code || 'US'
      );
    }
    
    // Rebuild address lines if needed
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
    
    if (components.um_address_line_1) {
      parts.push(components.um_address_line_1);
    }
    if (components.um_address_line_2) {
      parts.push(components.um_address_line_2);
    }
    if (components.um_city) {
      parts.push(components.um_city);
    }
    
    const stateZip = [];
    if (components.um_state_province) {
      stateZip.push(components.um_state_province);
    }
    if (components.um_postal_code) {
      stateZip.push(components.um_postal_code);
    }
    if (stateZip.length > 0) {
      parts.push(stateZip.join(' '));
    }
    
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
    
    if (!geocodeResult || !geocodeResult.components) {
      return merged;
    }
    
    const geocoded = geocodeResult.components;
    
    // Update with geocoded data if available
    if (geocoded.houseNumber && !merged.um_house_number) {
      merged.um_house_number = geocoded.houseNumber;
    }
    if (geocoded.road) {
      // Parse the road to extract street name and type
      const roadParts = geocoded.road.split(' ');
      if (roadParts.length > 1) {
        const lastPart = roadParts[roadParts.length - 1].toLowerCase();
        if (this.streetTypes.has(lastPart)) {
          merged.um_street_type = this.streetTypes.get(lastPart);
          merged.um_street_name = roadParts.slice(0, -1).join(' ');
        } else {
          merged.um_street_name = geocoded.road;
        }
      } else {
        merged.um_street_name = geocoded.road;
      }
    }
    if (geocoded.city) {
      merged.um_city = this.titleCase(geocoded.city);
    }
    if (geocoded.state) {
      // Check if state needs to be abbreviated
      const stateLower = geocoded.state.toLowerCase();
      if (this.stateAbbreviations.has(stateLower)) {
        merged.um_state_province = this.stateAbbreviations.get(stateLower);
      } else {
        merged.um_state_province = geocoded.state;
      }
    }
    if (geocoded.postcode) {
      merged.um_postal_code = this.formatPostalCode(geocoded.postcode, geocoded.countryCode || 'US');
    }
    if (geocoded.country) {
      merged.um_country = geocoded.country;
    }
    if (geocoded.countryCode) {
      merged.um_country_code = geocoded.countryCode.toUpperCase();
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
        success: validationData.geocoded === true
      });
    }
    
    return result;
  }
  
  // Validate postal code
  isValidPostalCode(postalCode, countryCode) {
    if (!postalCode) return false;
    
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
      'JP': /^\d{3}-?\d{4}$/,
      'MX': /^\d{5}$/,
      'BR': /^\d{5}-?\d{3}$/,
      'IN': /^\d{6}$/,
      'CN': /^\d{6}$/,
      'RU': /^\d{6}$/,
      'PH': /^\d{4}$/
    };
    
    const pattern = patterns[countryCode];
    if (!pattern) return true; // Don't validate unknown countries
    
    return pattern.test(postalCode);
  }
  
  // Format postal code
  formatPostalCode(postalCode, countryCode) {
    if (!postalCode) return '';
    
    const cleaned = postalCode.replace(/[\s-]+/g, '').toUpperCase();
    
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
          const inwardCode = cleaned.slice(-3);
          const outwardCode = cleaned.slice(0, -3);
          return `${outwardCode} ${inwardCode}`;
        }
        return cleaned;
        
      case 'BR':
        if (cleaned.length === 8) {
          return `${cleaned.slice(0, 5)}-${cleaned.slice(5)}`;
        }
        return cleaned;
        
      case 'JP':
        if (cleaned.length === 7) {
          return `${cleaned.slice(0, 3)}-${cleaned.slice(3)}`;
        }
        return cleaned;
        
      default:
        return postalCode.trim();
    }
  }
  
  // Title case helper
  titleCase(str) {
    if (!str) return '';
    
    // Handle special cases
    const specialCases = {
      'nyc': 'NYC',
      'la': 'LA',
      'dc': 'DC',
      'uk': 'UK',
      'usa': 'USA'
    };
    
    const lower = str.toLowerCase();
    if (specialCases[lower]) {
      return specialCases[lower];
    }
    
    // Title case each word
    return str.replace(/\w\S*/g, txt => {
      // Keep certain words lowercase
      const lowercaseWords = ['of', 'the', 'and', 'or', 'in', 'at', 'by', 'for'];
      if (lowercaseWords.includes(txt.toLowerCase()) && txt !== str.substring(0, txt.length)) {
        return txt.toLowerCase();
      }
      return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
    });
  }
  
  // Cache operations
  async checkAddressCache(addressKey) {
    try {
      const { rows } = await db.select(
        'address_validations',
        { cache_key: addressKey },
        { limit: 1 }
      );
      
      const data = rows[0];
      
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
      const cacheKey = this.generateCacheKey(validationResult);
      
      await db.insert('address_validations', {
        cache_key: cacheKey,
        formatted_address: validationResult.formatted_address,
        um_house_number: validationResult.um_house_number,
        um_street_name: validationResult.um_street_name,
        um_street_type: validationResult.um_street_type,
        um_street_direction: validationResult.um_street_direction,
        um_unit_type: validationResult.um_unit_type,
        um_unit_number: validationResult.um_unit_number,
        um_address_line_1: validationResult.um_address_line_1,
        um_address_line_2: validationResult.um_address_line_2,
        um_city: validationResult.um_city,
        um_state_province: validationResult.um_state_province,
        um_country: validationResult.um_country,
        um_country_code: validationResult.um_country_code,
        um_postal_code: validationResult.um_postal_code,
        valid: validationResult.valid,
        confidence: validationResult.confidence,
        geometry: validationResult.geometry ? JSON.stringify(validationResult.geometry) : null,
        client_id: clientId,
        created_at: new Date()
      });
      
      this.logger.debug('Address validation saved to cache', { 
        address: validationResult.formatted_address,
        clientId 
      });
    } catch (error) {
      // Handle duplicate key errors gracefully
      if (error.code !== '23505') { // PostgreSQL unique violation
        this.logger.error('Failed to save address validation', error);
      }
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

// Create singleton instance
const addressValidationService = new AddressValidationService();

// Export both the class and instance as default
export default addressValidationService;
export { addressValidationService };