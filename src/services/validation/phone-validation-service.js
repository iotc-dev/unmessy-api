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
    
    // No need to load country data - libphonenumber-js has it all!
    this.logger.info('Phone validation service initialized using libphonenumber-js');
  }
  
  // Auto-detect country from phone number patterns
  detectCountryFromNumber(phone) {
    const cleaned = this.cleanPhoneNumber(phone);
    
    // If it starts with +, let libphonenumber-js handle it
    if (cleaned.startsWith('+')) {
      try {
        const phoneNumber = parsePhoneNumberFromString(cleaned);
        if (phoneNumber && phoneNumber.country) {
          return phoneNumber.country;
        }
      } catch (e) {
        // Continue with fallback detection
      }
    }
    
    // Handle specific patterns before defaulting to US
    if (cleaned.startsWith('0')) {
      // Australian numbers: 10 digits starting with 0
      if (cleaned.length === 10) {
        // Australian mobile: 04XX XXX XXX
        if (cleaned.startsWith('04')) {
          return 'AU';
        }
        // Australian landline area codes: 02, 03, 07, 08
        if (cleaned.match(/^0[2378]/)) {
          return 'AU';
        }
        // Philippines mobile: 09XX XXX XXXX
        if (cleaned.startsWith('09')) {
          return 'PH';
        }
        // New Zealand: 03, 04, 06, 07, 09 (10 digits)
        if (cleaned.match(/^0[34679]/)) {
          return 'NZ';
        }
        // South Africa: 10 digits starting with 0
        if (cleaned.match(/^0[1-8]/)) {
          return 'ZA';
        }
      }
      // UK numbers: 11 digits starting with 0
      if (cleaned.length === 11) {
        // UK mobile: 07XXX XXXXXX
        if (cleaned.startsWith('07')) {
          return 'GB';
        }
        // UK landline patterns
        if (cleaned.match(/^0[1-9]/)) {
          return 'GB';
        }
      }
      // Philippines landline: 8 digits (without area code)
      if (cleaned.length === 8 && cleaned.match(/^[2-9]/)) {
        return 'PH';
      }
    }
    
    // Singapore numbers: 8 digits starting with 6, 8, or 9
    if (cleaned.length === 8 && cleaned.match(/^[689]/)) {
      return 'SG';
    }
    
    // Hong Kong numbers: 8 digits starting with 2, 3, 5, 6, 7, 8, 9
    if (cleaned.length === 8 && cleaned.match(/^[235-9]/)) {
      return 'HK';
    }
    
    // India numbers: 10 digits not starting with 0 or 1
    if (cleaned.length === 10 && cleaned.match(/^[2-9]/)) {
      // Check if it could be Indian mobile (starting with 6-9)
      if (cleaned.match(/^[6-9]/)) {
        return 'IN';
      }
    }
    
    // Brazil numbers: 10-11 digits
    if (cleaned.length === 10 && cleaned.match(/^[1-9]{2}/)) {
      return 'BR';
    }
    if (cleaned.length === 11 && cleaned.match(/^[1-9]{2}9/)) {
      // Brazilian mobile with 9 prefix
      return 'BR';
    }
    
    // Mexico numbers: 10 digits
    if (cleaned.length === 10 && cleaned.match(/^[2-9]/)) {
      // Could be Mexico, but also could be US - need more context
      // Check for Mexican area codes patterns
      if (cleaned.match(/^(33|55|81|656|664|998|222|229|244)/)) {
        return 'MX';
      }
    }
    
    // Japan numbers: Various lengths (9-11 digits)
    if (cleaned.length >= 9 && cleaned.length <= 11) {
      // Japanese mobile: 070, 080, 090
      if (cleaned.match(/^0[789]0/)) {
        return 'JP';
      }
      // Tokyo landline: 03
      if (cleaned.startsWith('03') && cleaned.length === 10) {
        return 'JP';
      }
    }
    
    // German numbers: 11-12 digits starting with 0
    if ((cleaned.length === 11 || cleaned.length === 12) && cleaned.startsWith('0')) {
      // German mobile: 015, 016, 017
      if (cleaned.match(/^01[567]/)) {
        return 'DE';
      }
    }
    
    // French numbers: 10 digits starting with 0
    if (cleaned.length === 10 && cleaned.startsWith('0')) {
      // French mobile: 06, 07
      if (cleaned.match(/^0[67]/)) {
        return 'FR';
      }
      // French landline: 01-05, 09
      if (cleaned.match(/^0[1-59]/)) {
        return 'FR';
      }
    }
    
    // Italian numbers: 10 digits (mobiles) or 9-11 (landlines)
    if (cleaned.startsWith('3') && cleaned.length === 10) {
      return 'IT'; // Italian mobile
    }
    
    // Spanish numbers: 9 digits
    if (cleaned.length === 9) {
      // Spanish mobile: 6XX or 7XX
      if (cleaned.match(/^[67]/)) {
        return 'ES';
      }
      // Spanish landline: 8XX or 9XX
      if (cleaned.match(/^[89]/)) {
        return 'ES';
      }
    }
    
    // Russian numbers: 11 digits starting with 7 or 8
    if (cleaned.length === 11 && cleaned.match(/^[78]/)) {
      return 'RU';
    }
    
    // US/Canada numbers (10 digits not starting with 0 or 1, or 11 digits starting with 1)
    if ((cleaned.length === 10 && !cleaned.startsWith('0') && !cleaned.startsWith('1')) || 
        (cleaned.length === 11 && cleaned.startsWith('1'))) {
      return 'US';
    }
    
    // Default to configured default country
    return config.validation.phone.defaultCountry || 'US';
  }
  
  // Validate phone number using libphonenumber-js
  async validatePhoneNumber(phone, options = {}) {
    const {
      country = null, // Allow explicit country to be passed
      countryHint = null, // Alternative way to specify country
      strictCountry = false, // If true, only use provided country
      fallbackCountries = ['AU', 'GB', 'PH', 'CA', 'NZ', 'IN', 'US'], // Countries to try
      tryAllCountries = false, // If true, try parsing with many countries
      clientId = null,
      useCache = true
    } = options;
    
    // Use provided country first
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
        formatValid: false
      }, clientId);
    }
    
    // Clean phone number
    const cleanedPhone = this.cleanPhoneNumber(phone);
    
    // Check cache first if enabled
    if (useCache && cleanedPhone.startsWith('+')) {
      const cached = await this.checkPhoneCache(cleanedPhone);
      if (cached) {
        this.logger.debug('Phone found in cache', { phone: cleanedPhone });
        return cached;
      }
    }
    
    // Determine country: provided > detected > default
    let detectedCountry = providedCountry;
    if (!detectedCountry && !strictCountry) {
      detectedCountry = this.detectCountryFromNumber(cleanedPhone);
      
      // Log potential mismatches for debugging
      if (detectedCountry === 'US' && cleanedPhone.startsWith('0')) {
        this.logger.warn('Potential country mismatch: Non-US format number defaulted to US', { 
          cleanedPhone,
          detectedCountry 
        });
      }
    }
    
    this.logger.debug('Country detection', {
      provided: providedCountry,
      detected: detectedCountry,
      phone: cleanedPhone
    });
    
    // Try parsing with multiple strategies
    let phoneNumber = null;
    let successfulCountry = null;
    let parseAttempts = [];
    
    try {
      // Strategy 1: Try with provided country first (if specified)
      if (providedCountry) {
        try {
          phoneNumber = parsePhoneNumber(cleanedPhone, providedCountry);
          if (phoneNumber && phoneNumber.isValid()) {
            successfulCountry = providedCountry;
            parseAttempts.push({ country: providedCountry, success: true });
          } else {
            parseAttempts.push({ country: providedCountry, success: false, reason: 'Invalid' });
            
            // If strict mode, don't try other countries
            if (strictCountry) {
              return this.buildValidationResult(phone, {
                valid: false,
                error: `Invalid ${this.getCountryName(providedCountry)} phone number`,
                formatValid: false,
                country: providedCountry,
                parseAttempts
              }, clientId);
            }
          }
        } catch (e) {
          parseAttempts.push({ country: providedCountry, success: false, reason: e.message });
          
          if (strictCountry) {
            return this.buildValidationResult(phone, {
              valid: false,
              error: `Not a valid ${this.getCountryName(providedCountry)} phone number format`,
              formatValid: false,
              country: providedCountry,
              parseAttempts
            }, clientId);
          }
          
          this.logger.debug('Failed to parse with provided country', { 
            country: providedCountry, 
            error: e.message 
          });
        }
      }
      
      // Strategy 2: Try with detected country (if different from provided)
      if (!phoneNumber && detectedCountry && detectedCountry !== providedCountry) {
        try {
          phoneNumber = parsePhoneNumber(cleanedPhone, detectedCountry);
          if (phoneNumber && phoneNumber.isValid()) {
            successfulCountry = detectedCountry;
            parseAttempts.push({ country: detectedCountry, success: true });
            
            // Warn if provided country was wrong
            if (providedCountry) {
              this.logger.warn('Provided country was incorrect', {
                provided: providedCountry,
                actual: detectedCountry,
                phone: cleanedPhone
              });
            }
          } else {
            parseAttempts.push({ country: detectedCountry, success: false, reason: 'Invalid' });
          }
        } catch (e) {
          parseAttempts.push({ country: detectedCountry, success: false, reason: e.message });
        }
      }
      
      // Strategy 3: If number has + prefix, try without country
      if (!phoneNumber && cleanedPhone.startsWith('+')) {
        try {
          phoneNumber = parsePhoneNumberFromString(cleanedPhone);
          if (phoneNumber && phoneNumber.isValid()) {
            successfulCountry = phoneNumber.country;
            parseAttempts.push({ country: 'AUTO', success: true, detected: successfulCountry });
            
            // Warn if provided country was wrong
            if (providedCountry && providedCountry !== successfulCountry) {
              this.logger.warn('Provided country was incorrect', {
                provided: providedCountry,
                actual: successfulCountry,
                phone: cleanedPhone
              });
            }
          }
        } catch (e) {
          parseAttempts.push({ country: 'AUTO', success: false, reason: e.message });
        }
      }
      
      // Strategy 4: Try fallback countries
      if (!phoneNumber && !cleanedPhone.startsWith('+')) {
        const countriesToTry = tryAllCountries ? 
          getCountries() : // All countries from libphonenumber-js
          fallbackCountries;
          
        for (const fallbackCountry of countriesToTry) {
          try {
            // Skip if we already tried this country
            if (parseAttempts.some(a => a.country === fallbackCountry)) continue;
            
            // For local numbers, add country code
            const callingCode = getCountryCallingCode(fallbackCountry);
            const withCountryCode = '+' + callingCode + cleanedPhone.replace(/^0+/, '');
            
            phoneNumber = parsePhoneNumberFromString(withCountryCode);
            if (phoneNumber && phoneNumber.isValid()) {
              successfulCountry = fallbackCountry;
              parseAttempts.push({ country: fallbackCountry, success: true });
              
              // Warn if provided country was wrong
              if (providedCountry && providedCountry !== successfulCountry) {
                this.logger.warn('Provided country was incorrect', {
                  provided: providedCountry,
                  actual: successfulCountry,
                  phone: cleanedPhone
                });
              }
              
              this.logger.debug('Successfully parsed with fallback country', { 
                country: fallbackCountry 
              });
              break;
            } else {
              parseAttempts.push({ country: fallbackCountry, success: false, reason: 'Invalid' });
            }
          } catch (e) {
            parseAttempts.push({ country: fallbackCountry, success: false, reason: e.message });
          }
        }
      }
      
      // If still no valid parse, return error with attempts info
      if (!phoneNumber || !phoneNumber.isValid()) {
        const attemptedCountries = parseAttempts.map(a => a.country).join(', ');
        return this.buildValidationResult(phone, {
          valid: false,
          error: `Invalid phone number format. Tried countries: ${attemptedCountries}`,
          formatValid: false,
          country: providedCountry || detectedCountry,
          parseAttempts,
          suggestedCountry: detectedCountry !== providedCountry ? detectedCountry : null
        }, clientId);
      }
      
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
        providedCountryWasWrong: providedCountry && providedCountry !== successfulCountry
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
      
      // Add warning if country was corrected
      if (phoneDetails.providedCountryWasWrong) {
        result.warning = `Phone number is from ${phoneDetails.countryName}, not ${this.getCountryName(providedCountry)}`;
        result.correctedCountry = phoneDetails.country;
      }
      
      // Save to cache if valid
      if (useCache && result.valid) {
        await this.savePhoneCache(phone, result, clientId);
      }
      
      return result;
      
    } catch (error) {
      this.logger.error('Phone parsing failed', { phone, error: error.message });
      
      // Handle specific parse errors
      if (error instanceof ParseError) {
        let errorMessage = 'Invalid phone number';
        
        switch (error.message) {
          case 'INVALID_COUNTRY':
            errorMessage = 'Invalid country code';
            break;
          case 'TOO_SHORT':
            errorMessage = 'Phone number is too short';
            break;
          case 'TOO_LONG':
            errorMessage = 'Phone number is too long';
            break;
          case 'NOT_A_NUMBER':
            errorMessage = 'Not a valid phone number';
            break;
        }
        
        return this.buildValidationResult(phone, {
          valid: false,
          error: errorMessage,
          formatValid: false,
          parseError: error.message,
          country: providedCountry || detectedCountry,
          parseAttempts
        }, clientId);
      }
      
      // Generic error
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Failed to validate phone number',
        formatValid: false,
        country: providedCountry || detectedCountry,
        parseAttempts
      }, clientId);
    }
  }
  
  // Clean phone number
  cleanPhoneNumber(phone) {
    // Convert to string and trim
    let cleaned = String(phone).trim();
    
    // Remove common formatting characters but keep + for international
    cleaned = cleaned.replace(/[\s\-\(\)\.]/g, '');
    
    // Remove extension markers and everything after
    cleaned = cleaned.replace(/(?:ext|extension|x|ext\.|extn|extn\.|#)[\s\.\-:#]?[\d]+$/i, '');
    
    // Handle various international prefixes
    if (cleaned.startsWith('00')) {
      // International prefix used in many countries
      cleaned = '+' + cleaned.substring(2);
    } else if (cleaned.startsWith('011')) {
      // US international prefix
      cleaned = '+' + cleaned.substring(3);
    } else if (cleaned.startsWith('0011')) {
      // Australian international prefix
      cleaned = '+' + cleaned.substring(4);
    } else if (cleaned.startsWith('010')) {
      // Japanese international prefix
      cleaned = '+' + cleaned.substring(3);
    } else if (cleaned.startsWith('009')) {
      // Nigerian international prefix
      cleaned = '+' + cleaned.substring(3);
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
      'PG': 'Papua New Guinea', // This was missing!
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
    
    const result = {
      originalPhone,
      currentPhone: validationData.e164 || this.cleanPhoneNumber(originalPhone),
      valid: isValid,
      possible: validationData.isPossible !== false,
      formatValid: formatValid,
      error: validationData.error || null,
      
      // Phone type
      type: validationData.type || 'UNKNOWN',
      
      // Location info
      location: validationData.countryName || countryName,
      carrier: '', // Would need external service for carrier lookup
      
      // Phone formats
      e164: validationData.e164 || null,
      internationalFormat: validationData.international || null,
      nationalFormat: validationData.national || null,
      uri: validationData.uri || null,
      
      // Country details
      countryCode: countryCode,
      countryCallingCode: validationData.countryCode || null,
      
      // Additional details
      confidence: isValid ? 'high' : 'low',
      
      // Unmessy fields - ensure country name not code
      um_phone: validationData.international || validationData.e164 || originalPhone,
      um_phone_status: wasChanged ? 'Changed' : 'Unchanged',
      um_phone_format: formatValid ? 'Valid' : 'Invalid',
      um_phone_country_code: countryCode || '',
      um_phone_country: countryName, // This should be the NAME not CODE
      um_phone_is_mobile: validationData.isMobile || false,
      
      // Debug info
      detectedCountry: countryCode,
      parseError: validationData.parseError || null
    };
    
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
          confidence: 'high',
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
        client_id: clientId
      });
      
      this.logger.debug('Phone validation saved to cache', { 
        phone: validationResult.e164,
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