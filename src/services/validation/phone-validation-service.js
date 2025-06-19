// src/services/validation/phone-validation-service.js
import db from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { ValidationError } from '../../core/errors.js';
import { 
  parsePhoneNumber, 
  parsePhoneNumberFromString,
  getCountryCallingCode, 
  getCountries, 
  ParseError 
} from 'libphonenumber-js';

const logger = createServiceLogger('phone-validation-service');

class PhoneValidationService {
  constructor() {
    this.logger = logger;
    this.logger.info('Phone validation service initialized using libphonenumber-js');
  }
  
  // Calculate confidence level based on validation context
  calculateConfidence(phoneNumber, parseAttempts, options = {}) {
    const {
      providedCountry,
      detectedMethod,
      originalHasPlus,
      requiredPrefixAdd,
      multipleValidCountries
    } = options;
    
    // Start with base confidence
    let confidence = 0;
    let factors = [];
    
    // If phoneNumber is null (validation failed), return very low confidence
    if (!phoneNumber) {
      return {
        score: 0,
        level: 'none',
        factors: ['validation_failed']
      };
    }
    
    // Factor 1: How the number was parsed (40 points max)
    if (detectedMethod === 'auto-detect-international' && originalHasPlus) {
      confidence += 40;
      factors.push('international_format');
    } else if (detectedMethod === 'provided-country' && providedCountry) {
      confidence += 35;
      factors.push('explicit_country');
    } else if (detectedMethod === 'fallback-country' && parseAttempts.length === 1) {
      confidence += 30;
      factors.push('unambiguous_local');
    } else if (detectedMethod === 'fallback-country') {
      confidence += 20;
      factors.push('ambiguous_local');
    } else if (detectedMethod === 'auto-detect-with-prefix') {
      confidence += 15;
      factors.push('prefix_required');
    }
    
    // Factor 2: Number validity and type (30 points max)
    // Check if phoneNumber exists before calling methods
    try {
      if (phoneNumber.isValid() && phoneNumber.isPossible()) {
        confidence += 30;
        factors.push('valid_possible');
      } else if (phoneNumber.isValid()) {
        confidence += 20;
        factors.push('valid_only');
      } else if (phoneNumber.isPossible()) {
        confidence += 10;
        factors.push('possible_only');
      }
    } catch (e) {
      // If methods fail, add minimal points
      confidence += 5;
      factors.push('validation_error');
    }
    
    // Factor 3: Phone type certainty (20 points max)
    try {
      const phoneType = phoneNumber.getType();
      if (phoneType === 'MOBILE' || phoneType === 'FIXED_LINE') {
        confidence += 20;
        factors.push('definite_type');
      } else if (phoneType === 'FIXED_LINE_OR_MOBILE') {
        confidence += 10;
        factors.push('ambiguous_type');
      } else {
        confidence += 5;
        factors.push('unknown_type');
      }
    } catch (e) {
      confidence += 0;
      factors.push('type_detection_failed');
    }
    
    // Factor 4: Parse attempt clarity (10 points max)
    const successfulAttempts = parseAttempts.filter(a => a.success).length;
    const failedAttempts = parseAttempts.filter(a => !a.success).length;
    
    if (successfulAttempts === 1 && failedAttempts === 0) {
      confidence += 10;
      factors.push('first_try_success');
    } else if (successfulAttempts === 1 && failedAttempts <= 2) {
      confidence += 7;
      factors.push('quick_success');
    } else if (successfulAttempts === 1) {
      confidence += 5;
      factors.push('eventual_success');
    }
    
    // Penalty factors
    if (multipleValidCountries) {
      confidence -= 15;
      factors.push('multiple_valid_countries');
    }
    
    if (requiredPrefixAdd) {
      confidence -= 10;
      factors.push('prefix_guessing_required');
    }
    
    // Ensure confidence is between 0 and 100
    confidence = Math.max(0, Math.min(100, confidence));
    
    // Convert to descriptive level
    let level;
    if (confidence >= 85) {
      level = 'very_high';
    } else if (confidence >= 70) {
      level = 'high';
    } else if (confidence >= 50) {
      level = 'medium';
    } else if (confidence >= 30) {
      level = 'low';
    } else if (confidence > 0) {
      level = 'very_low';
    } else {
      level = 'none';
    }
    
    return {
      score: confidence,
      level,
      factors
    };
  }
  
  // Validate phone number using libphonenumber-js
  async validatePhoneNumber(phone, options = {}) {
    const {
      country = null,
      countryHint = null,
      strictCountry = false,
      fallbackCountries = ['AU', 'GB', 'NZ', 'US', 'CA', 'PH', 'IN'],
      clientId = null,
      useCache = true
    } = options;
    
    const providedCountry = country || countryHint;
    
    this.logger.debug('Starting phone validation', {
      phone,
      providedCountry,
      strictCountry,
      clientId
    });
    
    // Handle null/empty
    if (!phone || phone === '') {
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Phone number is required',
        formatValid: false,
        confidence: { score: 0, level: 'none', factors: ['no_input'] }
      }, clientId);
    }
    
    // Clean phone number
    const cleanedPhone = this.cleanPhoneNumber(phone);
    const originalHasPlus = cleanedPhone.startsWith('+');
    
    // Check cache first if enabled
    if (useCache && cleanedPhone.startsWith('+')) {
      const cached = await this.checkPhoneCache(cleanedPhone);
      if (cached) {
        this.logger.debug('Phone found in cache', { phone: cleanedPhone });
        return cached;
      }
    }
    
    let phoneNumber = null;
    let successfulCountry = null;
    let parseAttempts = [];
    let detectedMethod = null;
    let requiredPrefixAdd = false;
    let validCountries = [];
    
    try {
      // Strategy 1: If number has international format (+XX...), let library auto-detect
      if (cleanedPhone.startsWith('+')) {
        try {
          phoneNumber = parsePhoneNumberFromString(cleanedPhone);
          if (phoneNumber && phoneNumber.isValid()) {
            successfulCountry = phoneNumber.country;
            detectedMethod = 'auto-detect-international';
            parseAttempts.push({ method: 'auto-detect-international', success: true, country: successfulCountry });
          }
        } catch (e) {
          parseAttempts.push({ method: 'auto-detect-international', success: false, reason: e.message });
        }
      }
      
      // Strategy 2: Try with provided country if specified
      if (!phoneNumber && providedCountry) {
        try {
          phoneNumber = parsePhoneNumber(cleanedPhone, providedCountry);
          if (phoneNumber && phoneNumber.isValid()) {
            successfulCountry = providedCountry;
            detectedMethod = 'provided-country';
            parseAttempts.push({ method: 'provided-country', country: providedCountry, success: true });
          } else {
            parseAttempts.push({ method: 'provided-country', country: providedCountry, success: false, reason: 'Invalid' });
            
            if (strictCountry) {
              const confidence = this.calculateConfidence(null, parseAttempts, {
                providedCountry,
                detectedMethod: 'failed',
                originalHasPlus
              });
              
              return this.buildValidationResult(phone, {
                valid: false,
                error: `Invalid ${this.getCountryName(providedCountry)} phone number`,
                formatValid: false,
                country: providedCountry,
                parseAttempts,
                confidence
              }, clientId);
            }
          }
        } catch (e) {
          parseAttempts.push({ method: 'provided-country', country: providedCountry, success: false, reason: e.message });
          
          if (strictCountry) {
            const confidence = this.calculateConfidence(null, parseAttempts, {
              providedCountry,
              detectedMethod: 'failed',
              originalHasPlus
            });
            
            return this.buildValidationResult(phone, {
              valid: false,
              error: `Not a valid ${this.getCountryName(providedCountry)} phone number format`,
              formatValid: false,
              country: providedCountry,
              parseAttempts,
              confidence
            }, clientId);
          }
        }
      }
      
      // Strategy 3: For local numbers (no country code), try fallback countries
      if (!phoneNumber && !cleanedPhone.startsWith('+')) {
        // First, check which countries this could be valid in
        for (const testCountry of fallbackCountries) {
          try {
            const testPhone = parsePhoneNumber(cleanedPhone, testCountry);
            if (testPhone && testPhone.isValid()) {
              validCountries.push(testCountry);
            }
          } catch (e) {
            // Not valid for this country
          }
        }
        
        // Now actually parse with fallback countries
        for (const testCountry of fallbackCountries) {
          try {
            // Skip if already tried
            if (parseAttempts.some(a => a.country === testCountry && a.method !== 'validation-check')) continue;
            
            phoneNumber = parsePhoneNumber(cleanedPhone, testCountry);
            if (phoneNumber && phoneNumber.isValid()) {
              successfulCountry = testCountry;
              detectedMethod = 'fallback-country';
              parseAttempts.push({ method: 'fallback-country', country: testCountry, success: true });
              
              this.logger.debug('Successfully parsed with fallback country', { 
                country: testCountry,
                phone: cleanedPhone,
                validInCountries: validCountries
              });
              break;
            } else {
              parseAttempts.push({ method: 'fallback-country', country: testCountry, success: false, reason: 'Invalid' });
            }
          } catch (e) {
            parseAttempts.push({ method: 'fallback-country', country: testCountry, success: false, reason: e.message });
          }
        }
      }
      
      // Strategy 4: Try adding international prefixes
      if (!phoneNumber && !cleanedPhone.startsWith('+')) {
        try {
          const prefixesToTry = [
            { prefix: '+61', country: 'AU' },  // Australia
            { prefix: '+44', country: 'GB' },  // UK
            { prefix: '+64', country: 'NZ' },  // New Zealand
            { prefix: '+1', country: 'US' },   // US/Canada
            { prefix: '+91', country: 'IN' },  // India
            { prefix: '+63', country: 'PH' }   // Philippines
          ];
          
          for (const { prefix, country } of prefixesToTry) {
            const numberWithPrefix = prefix + cleanedPhone.replace(/^0+/, '');
            const parsed = parsePhoneNumberFromString(numberWithPrefix);
            
            if (parsed && parsed.isValid()) {
              phoneNumber = parsed;
              successfulCountry = parsed.country;
              detectedMethod = 'auto-detect-with-prefix';
              requiredPrefixAdd = true;
              parseAttempts.push({ 
                method: 'auto-detect-with-prefix', 
                prefix, 
                success: true, 
                country: successfulCountry 
              });
              break;
            }
          }
        } catch (e) {
          parseAttempts.push({ method: 'auto-detect-fallback', success: false, reason: e.message });
        }
      }
      
      // If still no valid parse, return error
      if (!phoneNumber || !phoneNumber.isValid()) {
        const attemptedCountries = [...new Set(parseAttempts.filter(a => a.country).map(a => a.country))].join(', ');
        const confidence = this.calculateConfidence(null, parseAttempts, {
          providedCountry,
          detectedMethod: 'failed',
          originalHasPlus
        });
        
        return this.buildValidationResult(phone, {
          valid: false,
          error: `Invalid phone number format. Tried countries: ${attemptedCountries || 'various'}`,
          formatValid: false,
          parseAttempts,
          confidence
        }, clientId);
      }
      
      // Calculate confidence
      const confidence = this.calculateConfidence(phoneNumber, parseAttempts, {
        providedCountry,
        detectedMethod,
        originalHasPlus,
        requiredPrefixAdd,
        multipleValidCountries: validCountries.length > 1
      });
      
      // Extract phone details
      const phoneDetails = {
        valid: true,
        formatValid: true,
        e164: phoneNumber.format('E.164'),
        international: phoneNumber.format('INTERNATIONAL'),
        national: phoneNumber.format('NATIONAL'),
        countryCode: phoneNumber.countryCallingCode,
        country: phoneNumber.country || successfulCountry,
        type: phoneNumber.getType() || 'UNKNOWN',
        isMobile: phoneNumber.getType() === 'MOBILE',
        isFixedLine: phoneNumber.getType() === 'FIXED_LINE',
        isFixedLineOrMobile: phoneNumber.getType() === 'FIXED_LINE_OR_MOBILE',
        isPossible: phoneNumber.isPossible(),
        uri: phoneNumber.getURI(),
        parseAttempts,
        confidence,
        validInCountries: validCountries.length > 0 ? validCountries : [successfulCountry]
      };
      
      // For FIXED_LINE_OR_MOBILE, default to mobile for common mobile countries
      if (phoneDetails.isFixedLineOrMobile) {
        const mobileFirstCountries = ['US', 'CA', 'PH', 'IN', 'BR', 'MX', 'AU'];
        if (mobileFirstCountries.includes(phoneDetails.country)) {
          phoneDetails.isMobile = true;
        }
      }
      
      // Get country name
      phoneDetails.countryName = this.getCountryName(phoneDetails.country);
      
      const result = this.buildValidationResult(phone, phoneDetails, clientId);
      
      // Save to cache if valid
      if (useCache && result.valid) {
        await this.savePhoneCache(phone, result, clientId);
      }
      
      return result;
      
    } catch (error) {
      this.logger.error('Phone parsing failed', { phone, error: error.message });
      
      const confidence = this.calculateConfidence(null, parseAttempts, {
        providedCountry,
        detectedMethod: 'error',
        originalHasPlus
      });
      
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Failed to validate phone number',
        formatValid: false,
        parseAttempts,
        confidence
      }, clientId);
    }
  }
  
  // Clean phone number - simplified
  cleanPhoneNumber(phone) {
    // Convert to string and trim
    let cleaned = String(phone).trim();
    
    // Remove common formatting characters but keep + for international
    cleaned = cleaned.replace(/[\s\-\(\)\.]/g, '');
    
    // Remove extension markers and everything after
    cleaned = cleaned.replace(/(?:ext|extension|x|ext\.|extn|extn\.|#)[\s\.\-:#]?[\d]+$/i, '');
    
    // Handle various international prefixes by converting to +
    if (cleaned.startsWith('00')) {
      cleaned = '+' + cleaned.substring(2);
    } else if (cleaned.startsWith('011')) {
      cleaned = '+' + cleaned.substring(3);
    } else if (cleaned.startsWith('0011')) {
      cleaned = '+' + cleaned.substring(4);
    }
    
    // Handle letters in phone numbers (like 1-800-FLOWERS)
    cleaned = cleaned.replace(/[A-Za-z]/g, (match) => {
      const letterMap = {
        'A': '2', 'B': '2', 'C': '2',
        'D': '3', 'E': '3', 'F': '3',
        'G': '4', 'H': '4', 'I': '4',
        'J': '5', 'K': '5', 'L': '5',
        'M': '6', 'N': '6', 'O': '6',
        'P': '7', 'Q': '7', 'R': '7', 'S': '7',
        'T': '8', 'U': '8', 'V': '8',
        'W': '9', 'X': '9', 'Y': '9', 'Z': '9'
      };
      return letterMap[match.toUpperCase()] || match;
    });
    
    // Remove any remaining non-digit characters except +
    cleaned = cleaned.replace(/[^\d+]/g, '');
    
    return cleaned;
  }
  
  // Get country name from code
  getCountryName(countryCode) {
    const countryNames = {
      'US': 'United States',
      'CA': 'Canada',
      'GB': 'United Kingdom',
      'AU': 'Australia',
      'DE': 'Germany',
      'FR': 'France',
      'PH': 'Philippines',
      'PG': 'Papua New Guinea',
      'IN': 'India',
      'JP': 'Japan',
      'CN': 'China',
      'BR': 'Brazil',
      'MX': 'Mexico',
      'ES': 'Spain',
      'IT': 'Italy',
      'NL': 'Netherlands',
      'SE': 'Sweden',
      'NO': 'Norway',
      'DK': 'Denmark',
      'FI': 'Finland',
      'PL': 'Poland',
      'RU': 'Russia',
      'ZA': 'South Africa',
      'SG': 'Singapore',
      'MY': 'Malaysia',
      'TH': 'Thailand',
      'VN': 'Vietnam',
      'ID': 'Indonesia',
      'KR': 'South Korea',
      'TW': 'Taiwan',
      'HK': 'Hong Kong',
      'NZ': 'New Zealand',
      'AR': 'Argentina',
      'CL': 'Chile',
      'CO': 'Colombia',
      'PE': 'Peru',
      'VE': 'Venezuela',
      'EC': 'Ecuador',
      'BO': 'Bolivia',
      'PY': 'Paraguay',
      'UY': 'Uruguay',
      'CR': 'Costa Rica',
      'PA': 'Panama',
      'DO': 'Dominican Republic',
      'GT': 'Guatemala',
      'HN': 'Honduras',
      'SV': 'El Salvador',
      'NI': 'Nicaragua',
      'PR': 'Puerto Rico',
      'JM': 'Jamaica',
      'TT': 'Trinidad and Tobago',
      'BB': 'Barbados',
      'BS': 'Bahamas',
      'BM': 'Bermuda',
      'KY': 'Cayman Islands',
      'VG': 'British Virgin Islands',
      'AG': 'Antigua and Barbuda',
      'DM': 'Dominica',
      'GD': 'Grenada',
      'KN': 'Saint Kitts and Nevis',
      'LC': 'Saint Lucia',
      'VC': 'Saint Vincent and the Grenadines',
      'MQ': 'Martinique',
      'GP': 'Guadeloupe',
      'AW': 'Aruba',
      'CW': 'Curaçao',
      'SX': 'Sint Maarten',
      'BQ': 'Caribbean Netherlands',
      'TC': 'Turks and Caicos Islands',
      'VI': 'U.S. Virgin Islands',
      'AI': 'Anguilla',
      'MS': 'Montserrat',
      'GU': 'Guam',
      'AS': 'American Samoa',
      'MP': 'Northern Mariana Islands',
      'PW': 'Palau',
      'MH': 'Marshall Islands',
      // Add more as needed
    };
    
    return countryNames[countryCode] || countryCode;
  }
  
  // Build validation result
  buildValidationResult(originalPhone, validationData, clientId) {
    const isValid = validationData.valid === true;
    const formatValid = validationData.formatValid !== false;
    
    // Determine if phone was changed (formatted differently)
    const formattedPhone = validationData.international || validationData.e164 || this.cleanPhoneNumber(originalPhone);
    const wasChanged = originalPhone !== formattedPhone;
    
    // Get the country name properly
    const countryCode = validationData.country || null;
    const countryName = countryCode ? this.getCountryName(countryCode) : '';
    
    // Get confidence - either passed in or calculate a basic one
    const confidence = validationData.confidence || {
      score: isValid ? 50 : 0,
      level: isValid ? 'medium' : 'none',
      factors: isValid ? ['basic_valid'] : ['invalid']
    };
    
    const result = {
      originalPhone,
      currentPhone: validationData.e164 || this.cleanPhoneNumber(originalPhone),
      valid: isValid,
      possible: validationData.isPossible !== false,
      formatValid: formatValid,
      error: validationData.error || null,
      
      // Phone type
      type: validationData.type || 'UNKNOWN',
      
      // Location info - only show if we have a country
      location: countryName || 'Unknown',
      carrier: '', // Would need external service for carrier lookup
      
      // Phone formats
      e164: validationData.e164 || null,
      internationalFormat: validationData.international || null,
      nationalFormat: validationData.national || null,
      uri: validationData.uri || null,
      
      // Country details
      countryCode: countryCode,
      countryCallingCode: validationData.countryCode || null,
      
      // Confidence details
      confidence: confidence.level,
      confidenceScore: confidence.score,
      confidenceFactors: confidence.factors,
      
      // Unmessy fields - ensure country name not code
      um_phone: validationData.international || validationData.e164 || originalPhone,
      um_phone_status: wasChanged ? 'Changed' : 'Unchanged',
      um_phone_format: formatValid ? 'Valid' : 'Invalid',
      um_phone_country_code: countryCode || '',
      um_phone_country: countryName,
      um_phone_is_mobile: validationData.isMobile || false,
      
      // Debug info
      detectedCountry: validationData.country,
      parseError: validationData.parseError || null
    };
    
    // Add additional details if available
    if (validationData.parseAttempts) {
      result.parseAttempts = validationData.parseAttempts;
    }
    
    if (validationData.validInCountries) {
      result.validInCountries = validationData.validInCountries;
    }
    
    return result;
  }
  
  // Cache operations - Only for storing validated results, not country data
  async checkPhoneCache(e164Phone) {
    try {
      const { rows } = await db.select(
        'phone_validations',
        { e164: e164Phone },
        { limit: 1 }
      );
      
      const data = rows[0];
      
      if (data) {
        return {
          originalPhone: data.original_phone,
          currentPhone: data.e164,
          valid: data.valid,
          possible: true,
          formatValid: true,
          type: data.phone_type,
          location: this.getCountryName(data.country),
          carrier: '',
          e164: data.e164,
          internationalFormat: data.international_format,
          nationalFormat: data.national_format,
          uri: `tel:${data.e164}`,
          countryCode: data.country,
          countryCallingCode: data.country_code,
          confidence: 'high', // Cached results have high confidence
          confidenceScore: 90,
          confidenceFactors: ['cached_result', 'previously_validated'],
          um_phone: data.international_format,
          um_phone_status: data.original_phone !== data.international_format ? 'Changed' : 'Unchanged',
          um_phone_format: 'Valid',
          um_phone_country_code: data.country,
          um_phone_country: this.getCountryName(data.country),
          um_phone_is_mobile: data.is_mobile,
          isFromCache: true
        };
      }
      
      return null;
    } catch (error) {
      this.logger.error('Failed to check phone cache', error, { e164Phone });
      return null;
    }
  }
  
  async savePhoneCache(phone, validationResult, clientId) {
    // Only save valid phones
    if (!validationResult.valid || !validationResult.e164) {
      return;
    }
    
    try {
      await db.insert('phone_validations', {
        original_phone: phone,
        e164: validationResult.e164,
        international_format: validationResult.internationalFormat,
        national_format: validationResult.nationalFormat,
        country_code: validationResult.countryCallingCode,
        country: validationResult.countryCode,
        phone_type: validationResult.type,
        is_mobile: validationResult.um_phone_is_mobile,
        valid: validationResult.valid,
        confidence_score: validationResult.confidenceScore,
        confidence_level: validationResult.confidence,
        client_id: clientId
      });
      
      this.logger.debug('Phone validation saved to cache', { 
        phone: validationResult.e164,
        confidence: validationResult.confidence,
        clientId 
      });
    } catch (error) {
      // Handle duplicate key errors gracefully
      if (error.code !== '23505') { // PostgreSQL unique violation
        this.logger.error('Failed to save phone validation', error, { phone });
      }
    }
  }
  
  // Utility function to get all supported countries (for reference/UI)
  getAllSupportedCountries() {
    const countries = getCountries();
    return countries.map(country => ({
      code: country,
      name: this.getCountryName(country),
      callingCode: getCountryCallingCode(country)
    }));
  }
}

// Create singleton instance
const phoneValidationService = new PhoneValidationService();

// Export the class and instance
export { phoneValidationService, PhoneValidationService };