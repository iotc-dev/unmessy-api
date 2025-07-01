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
      ['wisconsin', 'WI'], ['wyoming', 'WY'],
      // DC
      ['district of columbia', 'DC'], ['washington dc', 'DC'], ['washington d.c.', 'DC']
    ]);

    // Initialize city corrections for common misspellings
    this.cityCorrections = new Map([
      ['newyork', 'New York'],
      ['new york city', 'New York'],
      ['nyc', 'New York'],
      ['la', 'Los Angeles'],
      ['san fran', 'San Francisco'],
      ['sf', 'San Francisco'],
      ['philly', 'Philadelphia'],
      ['vegas', 'Las Vegas'],
      ['nola', 'New Orleans'],
      ['chi-town', 'Chicago'],
      ['chitown', 'Chicago']
    ]);

    // Known city to state mappings for inference
    this.cityToState = new Map([
      ['new york', 'NY'],
      ['los angeles', 'CA'],
      ['chicago', 'IL'],
      ['houston', 'TX'],
      ['phoenix', 'AZ'],
      ['philadelphia', 'PA'],
      ['san antonio', 'TX'],
      ['san diego', 'CA'],
      ['dallas', 'TX'],
      ['san jose', 'CA'],
      ['austin', 'TX'],
      ['jacksonville', 'FL'],
      ['san francisco', 'CA'],
      ['columbus', 'OH'],
      ['charlotte', 'NC'],
      ['indianapolis', 'IN'],
      ['seattle', 'WA'],
      ['denver', 'CO'],
      ['washington', 'DC'],
      ['boston', 'MA'],
      ['nashville', 'TN'],
      ['detroit', 'MI'],
      ['portland', 'OR'],
      ['las vegas', 'NV'],
      ['louisville', 'KY'],
      ['baltimore', 'MD'],
      ['milwaukee', 'WI'],
      ['albuquerque', 'NM'],
      ['tucson', 'AZ'],
      ['fresno', 'CA'],
      ['sacramento', 'CA'],
      ['kansas city', 'MO'],
      ['mesa', 'AZ'],
      ['richmond', 'VA'],
      ['omaha', 'NE'],
      ['miami', 'FL'],
      ['cleveland', 'OH'],
      ['tulsa', 'OK'],
      ['raleigh', 'NC'],
      ['long beach', 'CA'],
      ['virginia beach', 'VA'],
      ['minneapolis', 'MN'],
      ['tampa', 'FL'],
      ['santa ana', 'CA'],
      ['arvada', 'CO'],
      // International
      ['toronto', 'ON'],
      ['vancouver', 'BC'],
      ['montreal', 'QC'],
      ['calgary', 'AB'],
      ['ottawa', 'ON'],
      ['london', 'England'],
      ['manchester', 'England'],
      ['birmingham', 'England'],
      ['cebu city', 'Cebu']
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
  
  // Main validation function with hierarchical approach
  async validateAddress(input, options = {}) {
    const { 
      useOpenCage = config.validation.address.geocode && config.services.openCage.enabled,
      clientId = null,
      country = config.validation.address.defaultCountry,
      timeout = config.services.openCage.timeout,
      useCache = true
    } = options;
    
    this.logger.debug('Starting address validation', { 
      input: typeof input === 'string' ? input : 'object', 
      useOpenCage, 
      clientId 
    });
    
    // Parse input into components
    const components = this.parseInput(input);
    
    // Check if address is empty
    if (this.isAddressEmpty(components)) {
      return this.buildValidationResult(components, {
        valid: false,
        error: 'Address is required',
        confidence: 0
      }, clientId);
    }
    
    // Generate cache key before any modifications
    const cacheKey = this.generateCacheKey(components);
    
    // Check cache first if enabled
    if (useCache) {
      const cached = await this.checkAddressCache(cacheKey);
      if (cached) {
        this.logger.debug('Address found in cache', { cacheKey });
        return cached;
      }
    }
    
    // Standardize components first
    const standardized = this.standardizeComponents(components);
    
    // Perform hierarchical validation
    const validationResult = await this.performHierarchicalValidation(
      standardized, 
      { useOpenCage, timeout, country }
    );
    
    // Build final result
    const result = this.buildValidationResult(standardized, {
      valid: validationResult.valid,
      confidence: validationResult.confidence,
      wasCorrected: validationResult.wasCorrected || Object.keys(validationResult.corrections).length > 0,
      warnings: validationResult.warnings,
      method: validationResult.method,
      geocoded: validationResult.geocoded,
      geometry: validationResult.geometry,
      formatted: validationResult.formatted,
      completedFields: validationResult.completedFields
    }, clientId);
    
    // Save to cache if valid
    if (useCache && result.valid) {
      await this.saveAddressCache(cacheKey, result, clientId);
    }
    
    return result;
  }

  // Check if address is empty
  isAddressEmpty(components) {
    return !(
      components.um_address_line_1 ||
      components.um_street_name ||
      components.um_city ||
      components.um_state_province ||
      components.um_postal_code ||
      components.um_country
    );
  }

  // Enhanced hierarchical validation with OpenCage integration
  async performHierarchicalValidation(components, options = {}) {
    const validationResult = {
      valid: false,
      confidence: 0,
      corrections: {},
      warnings: [],
      method: 'none',
      completedFields: [],
      wasCorrected: false
    };

    // Level 1: Try OpenCage first if we have partial address
    if (options.useOpenCage && this.openCage && this.hasMinimalAddressInfo(components)) {
      try {
        const openCageResult = await this.completeAddressWithOpenCage(components, options);
        
        if (openCageResult.success) {
          validationResult.valid = true;
          validationResult.confidence = openCageResult.confidence;
          validationResult.corrections = openCageResult.corrections;
          validationResult.completedFields = openCageResult.completedFields;
          validationResult.method = 'opencage_completion';
          validationResult.geocoded = true;
          validationResult.geometry = openCageResult.geometry;
          validationResult.formatted = openCageResult.formatted;
          validationResult.wasCorrected = openCageResult.wasCorrected;
          
          // Apply corrections
          Object.assign(components, openCageResult.corrections);
          
          // If OpenCage gave us high confidence, we're done
          if (openCageResult.confidence >= 85) {
            return validationResult;
          }
        }
      } catch (error) {
        this.logger.warn('OpenCage completion failed, falling back to other methods', error);
        validationResult.warnings.push('External geocoding service temporarily unavailable');
      }
    }

    // Level 2: Postal Code Validation (Most Specific)
    if (components.um_postal_code && components.um_country_code) {
      const postalValidation = await this.validateByPostalCode(
        components.um_postal_code,
        components.um_country_code,
        components,
        options
      );
      
      if (postalValidation.valid && postalValidation.confidence > validationResult.confidence) {
        validationResult.valid = true;
        validationResult.confidence = postalValidation.confidence;
        validationResult.corrections = { ...validationResult.corrections, ...postalValidation.corrections };
        validationResult.method = 'postal_code';
        validationResult.completedFields = [...validationResult.completedFields, ...postalValidation.completedFields];
        validationResult.wasCorrected = true;
        
        // Apply corrections
        Object.assign(components, postalValidation.corrections);
      }
    }

    // Level 3: City + State Validation
    if (components.um_city && components.um_state_province) {
      const cityStateValidation = await this.validateCityState(
        components.um_city,
        components.um_state_province,
        components.um_country_code || 'US',
        options
      );
      
      if (cityStateValidation.valid && cityStateValidation.confidence > validationResult.confidence) {
        validationResult.valid = true;
        validationResult.confidence = cityStateValidation.confidence;
        validationResult.corrections = { ...validationResult.corrections, ...cityStateValidation.corrections };
        validationResult.method = validationResult.method === 'opencage_completion' ? 'opencage_and_city_state' : 'city_state';
        validationResult.completedFields = [...validationResult.completedFields, ...cityStateValidation.completedFields];
        validationResult.wasCorrected = true;
        
        // Apply corrections
        Object.assign(components, cityStateValidation.corrections);
      }
    }

    // Level 4: Fuzzy Matching for Partial Info
    if (!validationResult.valid || validationResult.confidence < 70) {
      const fuzzyValidation = this.performFuzzyMatching(components);
      
      if (fuzzyValidation.confidence > 50) {
        validationResult.valid = true;
        validationResult.confidence = Math.max(validationResult.confidence, fuzzyValidation.confidence);
        validationResult.corrections = { ...validationResult.corrections, ...fuzzyValidation.corrections };
        validationResult.warnings = [...validationResult.warnings, ...fuzzyValidation.warnings];
        validationResult.method = validationResult.method === 'none' ? 'fuzzy_match' : validationResult.method + '_and_fuzzy';
        validationResult.wasCorrected = true;
        
        // Apply corrections
        Object.assign(components, fuzzyValidation.corrections);
      }
    }

    // If still not valid, perform basic validation
    if (!validationResult.valid) {
      const basicValidation = this.validateComponents(components);
      validationResult.valid = basicValidation.valid;
      validationResult.confidence = basicValidation.confidence;
      validationResult.warnings = [...validationResult.warnings, ...basicValidation.warnings];
      validationResult.method = 'basic';
    }

    return validationResult;
  }

  // Check if we have minimal address info for OpenCage
  hasMinimalAddressInfo(components) {
    return !!(
      components.um_postal_code ||
      (components.um_city && components.um_state_province) ||
      components.um_address_line_1 ||
      (components.um_street_name && components.um_city) ||
      components.um_city ||
      components.um_state_province
    );
  }

  // Complete address using OpenCage API
  async completeAddressWithOpenCage(components, options) {
    const result = {
      success: false,
      confidence: 0,
      corrections: {},
      completedFields: [],
      wasCorrected: false
    };

    // Build query from available components (most specific to least specific)
    const queryParts = [];
    
    // Street address
    if (components.um_address_line_1) {
      queryParts.push(components.um_address_line_1);
    } else if (components.um_house_number || components.um_street_name) {
      const streetParts = [];
      if (components.um_house_number) streetParts.push(components.um_house_number);
      if (components.um_street_direction) streetParts.push(components.um_street_direction);
      if (components.um_street_name) streetParts.push(components.um_street_name);
      if (components.um_street_type) streetParts.push(components.um_street_type);
      if (streetParts.length > 0) {
        queryParts.push(streetParts.join(' '));
      }
    }
    
    // City, State, Postal Code
    if (components.um_city) queryParts.push(components.um_city);
    if (components.um_state_province) queryParts.push(components.um_state_province);
    if (components.um_postal_code) queryParts.push(components.um_postal_code);
    if (components.um_country) queryParts.push(components.um_country);
    else if (components.um_country_code) queryParts.push(components.um_country_code);
    
    const query = queryParts.filter(Boolean).join(', ');
    
    if (!query || query.length < 3) {
      return result;
    }

    try {
      // Call OpenCage with the partial address
      const geocodeOptions = {
        countryCode: components.um_country_code || options.country,
        language: 'en',
        limit: 1,
        no_annotations: 0 // We want full annotations for better data
      };

      // Add bounds if we have country code
      const bounds = this.getBoundsForCountry(components.um_country_code);
      if (bounds) {
        geocodeOptions.bounds = bounds;
      }

      const geocodeResult = await ErrorRecovery.withTimeout(
        this.openCage.geocode(query, geocodeOptions),
        options.timeout || 10000,
        'OpenCage geocoding'
      );

      if (geocodeResult && geocodeResult.found && geocodeResult.components) {
        const gc = geocodeResult.components;
        
        // Track what fields we're completing
        const completedFields = [];
        
        // Complete missing house number
        if (!components.um_house_number && gc.house_number) {
          result.corrections.um_house_number = gc.house_number;
          completedFields.push('house_number');
          result.wasCorrected = true;
        }
        
        // Complete missing street name and type
        if (gc.road) {
          const roadParts = this.parseRoadName(gc.road);
          
          if (!components.um_street_name && roadParts.name) {
            result.corrections.um_street_name = roadParts.name;
            completedFields.push('street_name');
            result.wasCorrected = true;
          }
          
          if (!components.um_street_type && roadParts.type) {
            result.corrections.um_street_type = roadParts.type;
            completedFields.push('street_type');
            result.wasCorrected = true;
          }
        }
        
        // Complete missing city
        if (!components.um_city && (gc.city || gc.town || gc.village || gc.municipality)) {
          const city = gc.city || gc.town || gc.village || gc.municipality;
          result.corrections.um_city = this.titleCase(city);
          completedFields.push('city');
          result.wasCorrected = true;
        }
        
        // Complete missing state/province
        if (!components.um_state_province && (gc.state || gc.province)) {
          const state = gc.state || gc.province;
          // For US, abbreviate state names
          if ((components.um_country_code === 'US' || gc.country_code === 'us') && state) {
            const stateAbbrev = this.getStateAbbreviation(state);
            result.corrections.um_state_province = stateAbbrev || state;
          } else {
            result.corrections.um_state_province = state;
          }
          completedFields.push('state_province');
          result.wasCorrected = true;
        }
        
        // Complete missing postal code
        if (!components.um_postal_code && gc.postcode) {
          result.corrections.um_postal_code = this.formatPostalCode(
            gc.postcode, 
            gc.country_code?.toUpperCase() || components.um_country_code || 'US'
          );
          completedFields.push('postal_code');
          result.wasCorrected = true;
        }
        
        // Complete missing country
        if (!components.um_country && gc.country) {
          result.corrections.um_country = gc.country;
          completedFields.push('country');
          result.wasCorrected = true;
        }
        
        if (!components.um_country_code && gc.country_code) {
          result.corrections.um_country_code = gc.country_code.toUpperCase();
          completedFields.push('country_code');
          result.wasCorrected = true;
        }
        
        // Build complete address lines if missing
        if (!components.um_address_line_1 && (result.corrections.um_street_name || components.um_street_name)) {
          result.corrections.um_address_line_1 = this.buildAddressLine1({
            ...components,
            ...result.corrections
          });
          completedFields.push('address_line_1');
          result.wasCorrected = true;
        }
        
        // Calculate confidence based on what we found
        if (completedFields.length > 0 || geocodeResult.confidence >= 7) {
          result.success = true;
          result.completedFields = completedFields;
          
          // Base confidence on geocoding confidence and number of fields completed
          const geocodeConfidence = (geocodeResult.confidence || 7) * 10; // Convert 1-10 to percentage
          const completionBonus = Math.min(completedFields.length * 5, 20);
          result.confidence = Math.min(geocodeConfidence + completionBonus, 95);
          
          // Add geocoding metadata
          if (geocodeResult.coordinates) {
            result.geometry = {
              lat: geocodeResult.coordinates.lat,
              lng: geocodeResult.coordinates.lng
            };
          }
          
          if (geocodeResult.formattedAddress) {
            result.formatted = geocodeResult.formattedAddress;
          }
        }
      }
    } catch (error) {
      this.logger.error('OpenCage address completion failed', error);
      throw error;
    }

    return result;
  }

  // Parse road name to extract street name and type
  parseRoadName(road) {
    const result = {
      name: road,
      type: null
    };
    
    if (!road) return result;
    
    const words = road.split(' ');
    const lastWord = words[words.length - 1].toLowerCase();
    
    // Check if last word is a street type
    if (this.streetTypes.has(lastWord)) {
      result.type = this.streetTypes.get(lastWord);
      result.name = words.slice(0, -1).join(' ');
    }
    
    return result;
  }

  // Get state abbreviation
  getStateAbbreviation(stateName) {
    if (!stateName) return null;
    
    const normalized = stateName.toLowerCase().trim();
    return this.stateAbbreviations.get(normalized) || null;
  }

  // Get bounding box for country to improve geocoding accuracy
  getBoundsForCountry(countryCode) {
    const bounds = {
      'US': '-125,24,-66,49', // Continental US
      'CA': '-141,41.7,-52.6,83.1', // Canada
      'GB': '-8.2,49.9,1.8,60.8', // Great Britain
      'AU': '112.9,-43.6,153.6,-10.7', // Australia
      'PH': '116.9,4.6,126.6,21.1', // Philippines
      'DE': '5.9,47.3,15.0,55.1', // Germany
      'FR': '-5.2,41.3,9.6,51.1', // France
      'IT': '6.7,36.6,18.5,47.1', // Italy
      'ES': '-9.3,36.0,3.3,43.8', // Spain
      'NL': '3.4,50.8,7.2,53.5', // Netherlands
      'BE': '2.5,49.5,6.4,51.5', // Belgium
      'CH': '5.9,45.8,10.5,47.8', // Switzerland
      'AT': '9.5,46.4,17.2,49.0', // Austria
      'PL': '14.1,49.0,24.2,54.8', // Poland
      'CZ': '12.1,48.6,18.9,51.1', // Czech Republic
      'IN': '68.2,8.1,97.4,37.0', // India
      'JP': '123.0,24.0,146.0,46.0', // Japan
      'CN': '73.5,18.2,134.8,53.6', // China
      'BR': '-73.9,-33.7,-28.8,5.3', // Brazil
      'MX': '-118.4,14.5,-86.7,32.7', // Mexico
      'ZA': '16.5,-34.8,32.9,-22.1', // South Africa
      'NZ': '166.5,-47.3,178.5,-34.4', // New Zealand
      'SG': '103.6,1.2,104.0,1.5', // Singapore
      'HK': '113.8,22.2,114.4,22.6', // Hong Kong
      'KR': '124.6,33.1,131.9,38.6', // South Korea
      'TH': '97.3,5.6,105.6,20.5', // Thailand
      'MY': '100.1,0.9,119.3,7.4', // Malaysia
      'ID': '95.0,-11.0,141.0,6.0', // Indonesia
      'VN': '102.1,8.4,109.5,23.4', // Vietnam
      // Add more countries as needed
    };
    
    return bounds[countryCode] || null;
  }

  // Enhanced postal code validation with OpenCage fallback
  async validateByPostalCode(postalCode, countryCode, components, options = {}) {
    const result = {
      valid: false,
      confidence: 0,
      corrections: {},
      completedFields: []
    };

    // Check postal code format
    if (!this.isValidPostalCode(postalCode, countryCode)) {
      return result;
    }

    // Try OpenCage reverse geocoding with postal code
    if (options.useOpenCage && this.openCage) {
      try {
        const query = `${postalCode}, ${countryCode}`;
        const geocodeResult = await ErrorRecovery.withTimeout(
          this.openCage.geocode(query, {
            countryCode: countryCode,
            limit: 1
          }),
          options.timeout || 5000,
          'OpenCage postal code lookup'
        );
        
        if (geocodeResult && geocodeResult.found && geocodeResult.components) {
          const gc = geocodeResult.components;
          
          // Validate and complete city
          if (gc.city || gc.town || gc.municipality) {
            const geocodedCity = gc.city || gc.town || gc.municipality;
            if (!components.um_city || 
                components.um_city.toLowerCase() !== geocodedCity.toLowerCase()) {
              result.corrections.um_city = this.titleCase(geocodedCity);
              result.completedFields.push('city');
            }
            result.valid = true;
            result.confidence = 90;
          }
          
          // Validate and complete state
          if (gc.state || gc.province) {
            const geocodedState = countryCode === 'US' ? 
              this.getStateAbbreviation(gc.state) || gc.state : 
              gc.state || gc.province;
              
            if (!components.um_state_province || 
                components.um_state_province.toUpperCase() !== geocodedState.toUpperCase()) {
              result.corrections.um_state_province = geocodedState;
              result.completedFields.push('state_province');
            }
          }
          
          return result;
        }
      } catch (error) {
        this.logger.warn('OpenCage postal code lookup failed', { postalCode, error: error.message });
      }
    }

    // Fallback to basic format validation
    result.valid = true;
    result.confidence = 60;
    return result;
  }

  // Enhanced city/state validation with OpenCage
  async validateCityState(city, state, countryCode, options = {}) {
    const result = {
      valid: false,
      confidence: 0,
      corrections: {},
      completedFields: []
    };

    if (!city || !state) {
      return result;
    }

    // Try OpenCage validation
    if (options.useOpenCage && this.openCage) {
      try {
        const query = `${city}, ${state}, ${countryCode}`;
        const geocodeResult = await ErrorRecovery.withTimeout(
          this.openCage.geocode(query, {
            countryCode: countryCode,
            limit: 1
          }),
          options.timeout || 5000,
          'OpenCage city/state lookup'
        );
        
        if (geocodeResult && geocodeResult.found && geocodeResult.components) {
          const gc = geocodeResult.components;
          
          // Validate city/state combination
          if ((gc.city || gc.town || gc.municipality) && (gc.state || gc.province)) {
            result.valid = true;
            result.confidence = 85;
            
            // Apply standard formatting
            const geocodedCity = gc.city || gc.town || gc.municipality;
            if (geocodedCity) {
              result.corrections.um_city = this.titleCase(geocodedCity);
            }
            
            // Complete postal code if missing
            if (!components.um_postal_code && gc.postcode) {
              result.corrections.um_postal_code = this.formatPostalCode(gc.postcode, countryCode);
              result.completedFields.push('postal_code');
              result.confidence = 90;
            }
            
            // Ensure state is properly formatted
            if (countryCode === 'US' && gc.state) {
              const abbrev = this.getStateAbbreviation(gc.state) || state.toUpperCase();
              if (abbrev !== state) {
                result.corrections.um_state_province = abbrev;
              }
            }
          }
        }
      } catch (error) {
        this.logger.warn('OpenCage city/state validation failed', { city, state, error: error.message });
      }
    }

    // Fallback to basic validation
    if (!result.valid) {
      // Check our known city/state pairs
      const normalizedCity = city.toLowerCase().trim();
      const expectedState = this.cityToState.get(normalizedCity);
      
      if (expectedState) {
        if (state.toUpperCase() === expectedState || 
            this.getStateAbbreviation(state) === expectedState) {
          result.valid = true;
          result.confidence = 70;
          
          // Apply standard formatting
          result.corrections.um_city = this.titleCase(city);
          
          if (countryCode === 'US' && state.length > 2) {
            const abbrev = this.stateAbbreviations.get(state.toLowerCase());
            if (abbrev) {
              result.corrections.um_state_province = abbrev;
            }
          }
        }
      }
    }

    return result;
  }

  // Fuzzy matching for partial information
  performFuzzyMatching(components) {
    const result = {
      confidence: 0,
      corrections: {},
      warnings: []
    };

    // Try to correct city names with common misspellings
    if (components.um_city) {
      const cityLower = components.um_city.toLowerCase();
      const correctedCity = this.cityCorrections.get(cityLower);
      
      if (correctedCity) {
        result.corrections.um_city = correctedCity;
        result.confidence += 30;
      }
    }

    // Try to infer state from well-known cities
    if (components.um_city && !components.um_state_province) {
      const cityLower = components.um_city.toLowerCase();
      const inferredState = this.cityToState.get(cityLower);
      
      if (inferredState) {
        result.corrections.um_state_province = inferredState;
        result.confidence += 25;
        result.warnings.push('State inferred from city name');
      }
    }

    // Validate/correct state abbreviations
    if (components.um_state_province) {
      const stateLower = components.um_state_province.toLowerCase();
      if (this.stateAbbreviations.has(stateLower)) {
        result.corrections.um_state_province = this.stateAbbreviations.get(stateLower);
        result.confidence += 20;
      } else if (components.um_state_province.length === 2) {
        result.corrections.um_state_province = components.um_state_province.toUpperCase();
        result.confidence += 15;
      }
    }

    // Check for obvious country indicators
    if (!components.um_country_code && components.um_postal_code) {
      // Canadian postal code pattern
      if (/^[A-Z]\d[A-Z]\s?\d[A-Z]\d$/i.test(components.um_postal_code)) {
        result.corrections.um_country_code = 'CA';
        result.corrections.um_country = 'Canada';
        result.confidence += 20;
        result.warnings.push('Country inferred from postal code format');
      }
      // UK postal code pattern
      else if (/^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$/i.test(components.um_postal_code)) {
        result.corrections.um_country_code = 'GB';
        result.corrections.um_country = 'United Kingdom';
        result.confidence += 20;
        result.warnings.push('Country inferred from postal code format');
      }
      // Australian postal code (4 digits)
      else if (/^\d{4}$/.test(components.um_postal_code)) {
        const code = parseInt(components.um_postal_code);
        if (code >= 1000 && code <= 9999) {
          result.corrections.um_country_code = 'AU';
          result.corrections.um_country = 'Australia';
          result.confidence += 15;
          result.warnings.push('Country might be Australia based on postal code format');
        }
      }
    }

    return result;
  }

  // [Rest of the methods remain the same as in the original file...]
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
    // Need at least a street/address line and city OR just postal code with country
    return !!(
      ((components.um_address_line_1 || components.um_street_name) &&
       (components.um_city || components.um_state_province || components.um_postal_code)) ||
      (components.um_postal_code && (components.um_country || components.um_country_code))
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
    if (geocoded.house_number && !merged.um_house_number) {
      merged.um_house_number = geocoded.house_number;
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
      merged.um_postal_code = this.formatPostalCode(geocoded.postcode, geocoded.country_code || 'US');
    }
    if (geocoded.country) {
      merged.um_country = geocoded.country;
    }
    if (geocoded.country_code) {
      merged.um_country_code = geocoded.country_code.toUpperCase();
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
    
    // Add completed fields if any
    if (validationData.completedFields && validationData.completedFields.length > 0) {
      result.completedFields = validationData.completedFields;
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
        confidence: validationData.confidence,
        method: validationData.method
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
  
  async saveAddressCache(cacheKey, validationResult, clientId) {
    // Only save valid addresses
    if (!validationResult.valid) {
      return;
    }
    
    try {
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