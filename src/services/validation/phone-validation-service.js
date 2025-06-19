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
      }
      // UK numbers: 11 digits starting with 0
      if (cleaned.length === 11) {
        return 'GB';
      }
    }
    
    // US/Canada numbers (10 digits not starting with 0, or 11 digits starting with 1)
    if ((cleaned.length === 10 && !cleaned.startsWith('0')) || 
        (cleaned.length === 11 && cleaned.startsWith('1'))) {
      return 'US';
    }
    
    // Default to configured default country
    return config.validation.phone.defaultCountry || 'US';
  }
  
  // Validate phone number using libphonenumber-js
  async validatePhoneNumber(phone, options = {}) {
    const {
      country = null, // Allow null to trigger auto-detection
      clientId = null,
      useCache = true
    } = options;
    
    this.logger.debug('Starting phone validation', {
      phone,
      country,
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
    
    // Auto-detect country if not provided
    const detectedCountry = country || this.detectCountryFromNumber(cleanedPhone);
    
    this.logger.debug('Country detection', {
      provided: country,
      detected: detectedCountry,
      phone: cleanedPhone
    });
    
    try {
      // Try parsing phone number
      let phoneNumber;
      
      // First, try parsing with detected/provided country
      try {
        phoneNumber = parsePhoneNumber(cleanedPhone, detectedCountry);
      } catch (parseError) {
        // If that fails and number has international format, try without country
        if (cleanedPhone.startsWith('+')) {
          try {
            phoneNumber = parsePhoneNumberFromString(cleanedPhone);
          } catch (e) {
            throw parseError; // Throw original error
          }
        } else {
          // Try adding country code for local numbers using library function
          try {
            const callingCode = getCountryCallingCode(detectedCountry);
            const withCountryCode = '+' + callingCode + cleanedPhone.replace(/^0+/, '');
            phoneNumber = parsePhoneNumberFromString(withCountryCode);
          } catch (e) {
            throw parseError; // Throw original error
          }
        }
      }
      
      // Validate the parsed number
      const isValid = phoneNumber && phoneNumber.isValid();
      
      if (!isValid) {
        return this.buildValidationResult(phone, {
          valid: false,
          error: 'Invalid phone number format',
          formatValid: false,
          country: detectedCountry
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
        country: phoneNumber.country || detectedCountry,
        type: phoneNumber.getType() || 'UNKNOWN',
        isMobile: phoneNumber.getType() === 'MOBILE',
        isFixedLine: phoneNumber.getType() === 'FIXED_LINE',
        isFixedLineOrMobile: phoneNumber.getType() === 'FIXED_LINE_OR_MOBILE',
        isPossible: phoneNumber.isPossible(),
        uri: phoneNumber.getURI()
      };
      
      // For FIXED_LINE_OR_MOBILE, default to mobile for common mobile countries
      if (phoneDetails.isFixedLineOrMobile) {
        // Common mobile-first countries
        const mobileFirstCountries = ['US', 'CA', 'PH', 'IN', 'BR', 'MX'];
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
      this.logger.debug('Phone parsing failed', { phone, error: error.message });
      
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
          country: detectedCountry
        }, clientId);
      }
      
      // Generic error
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Failed to validate phone number',
        formatValid: false,
        country: detectedCountry
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
    cleaned = cleaned.replace(/(?:ext|x|extension).*$/i, '');
    
    // Handle common prefixes
    if (cleaned.startsWith('00')) {
      // International prefix used in some countries
      cleaned = '+' + cleaned.substring(2);
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
      location: validationData.countryName || this.getCountryName(validationData.country),
      carrier: '', // Would need external service for carrier lookup
      
      // Phone formats
      e164: validationData.e164 || null,
      internationalFormat: validationData.international || null,
      nationalFormat: validationData.national || null,
      uri: validationData.uri || null,
      
      // Country details
      countryCode: validationData.country || null,
      countryCallingCode: validationData.countryCode || null,
      
      // Additional details
      confidence: isValid ? 'high' : 'low',
      
      // Unmessy fields
      um_phone: validationData.international || validationData.e164 || originalPhone,
      um_phone_status: wasChanged ? 'Changed' : 'Unchanged',
      um_phone_format: formatValid ? 'Valid' : 'Invalid',
      um_phone_country_code: validationData.country || '',
      um_phone_country: validationData.countryName || this.getCountryName(validationData.country) || '',
      um_phone_is_mobile: validationData.isMobile || false,
      
      // Debug info
      detectedCountry: validationData.country,
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