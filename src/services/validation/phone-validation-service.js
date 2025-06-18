// src/services/validation/phone-validation-service.js
import db from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { ValidationError } from '../../core/errors.js';
import { parsePhoneNumber, isValidPhoneNumber, ParseError } from 'libphonenumber-js';

const logger = createServiceLogger('phone-validation-service');

class PhoneValidationService {
  constructor() {
    this.logger = logger;
    
    // Initialize reference data
    this.countryPhoneData = new Map();
    this.mobilePatterns = new Map();
    
    // Load normalization data on startup
    this.loadNormalizationData();
  }
  
  async loadNormalizationData() {
    try {
      // Load country phone data from database using proper Supabase method
      const countryData = await db.select(
        'country_phone_data',
        {},
        { columns: 'country_code, calling_code, mobile_begins_with' }
      ).catch(() => ({ rows: [] }));
      
      if (countryData?.rows) {
        countryData.rows.forEach(row => {
          this.countryPhoneData.set(row.country_code, {
            callingCode: row.calling_code,
            mobileBeginnsWith: row.mobile_begins_with ? row.mobile_begins_with.split(',') : []
          });
        });
      }
      
      // Initialize default data if database is empty
      this.initializeDefaultData();
      
      this.logger.info('Phone normalization data loaded', {
        countries: this.countryPhoneData.size
      });
    } catch (error) {
      this.logger.error('Failed to load normalization data', error);
      // Initialize with defaults
      this.initializeDefaultData();
    }
  }
  
  initializeDefaultData() {
    // Default country phone data if not loaded from DB
    if (this.countryPhoneData.size === 0) {
      // US
      this.countryPhoneData.set('US', {
        callingCode: '+1',
        mobileBeginnsWith: ['201', '202', '203', '205', '206', '207', '208', '209', '210', '212', '213', '214', '215', '216', '217', '218', '219', '220', '223', '224', '225', '228', '229', '231', '234', '239', '240', '248', '251', '252', '253', '254', '256', '260', '262', '267', '269', '270', '272', '274', '276', '281', '283', '301', '302', '303', '304', '305', '307', '308', '309', '310', '312', '313', '314', '315', '316', '317', '318', '319', '320', '321', '323', '325', '326', '330', '331', '332', '334', '336', '337', '339', '340', '341', '346', '347', '351', '352', '360', '361', '364', '380', '385', '386', '401', '402', '404', '405', '406', '407', '408', '409', '410', '412', '413', '414', '415', '417', '419', '423', '424', '425', '430', '432', '434', '435', '440', '442', '443', '445', '447', '458', '463', '464', '469', '470', '475', '478', '479', '480', '484', '501', '502', '503', '504', '505', '507', '508', '509', '510', '512', '513', '515', '516', '517', '518', '520', '530', '531', '534', '539', '540', '541', '551', '559', '561', '562', '563', '564', '567', '570', '571', '572', '573', '574', '575', '580', '582', '585', '586', '601', '602', '603', '605', '606', '607', '608', '609', '610', '612', '614', '615', '616', '617', '618', '619', '620', '623', '626', '628', '629', '630', '631', '636', '640', '641', '646', '650', '651', '656', '657', '659', '660', '661', '662', '667', '669', '678', '680', '681', '682', '684', '689', '701', '702', '703', '704', '706', '707', '708', '712', '713', '714', '715', '716', '717', '718', '719', '720', '724', '725', '726', '727', '730', '731', '732', '734', '737', '740', '743', '747', '754', '757', '760', '762', '763', '765', '769', '770', '771', '772', '773', '774', '775', '779', '781', '785', '786', '787', '801', '802', '803', '804', '805', '806', '808', '810', '812', '813', '814', '815', '816', '817', '818', '820', '826', '828', '830', '831', '832', '838', '839', '840', '843', '845', '847', '848', '850', '854', '856', '857', '858', '859', '860', '862', '863', '864', '865', '870', '872', '878', '901', '903', '904', '906', '907', '908', '909', '910', '912', '913', '914', '915', '916', '917', '918', '919', '920', '925', '928', '929', '930', '931', '934', '936', '937', '938', '939', '940', '941', '943', '945', '947', '949', '951', '952', '954', '956', '959', '970', '971', '972', '973', '975', '978', '979', '980', '984', '985', '986', '989']
      });
      
      // UK
      this.countryPhoneData.set('GB', {
        callingCode: '+44',
        mobileBeginnsWith: ['7']
      });
      
      // Canada
      this.countryPhoneData.set('CA', {
        callingCode: '+1',
        mobileBeginnsWith: [] // Same as US
      });
      
      // Australia
      this.countryPhoneData.set('AU', {
        callingCode: '+61',
        mobileBeginnsWith: ['4']
      });
      
      // Germany
      this.countryPhoneData.set('DE', {
        callingCode: '+49',
        mobileBeginnsWith: ['15', '16', '17']
      });
      
      // France
      this.countryPhoneData.set('FR', {
        callingCode: '+33',
        mobileBeginnsWith: ['6', '7']
      });
      
      // Philippines
      this.countryPhoneData.set('PH', {
        callingCode: '+63',
        mobileBeginnsWith: ['9']
      });
    }
  }
  
  // Validate phone number using libphonenumber-js
  async validatePhoneNumber(phone, options = {}) {
    const {
      country = config.validation.phone.defaultCountry,
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
    
    try {
      // Parse phone number
      let phoneNumber;
      
      // Try parsing with country first
      try {
        phoneNumber = parsePhoneNumber(cleanedPhone, country);
      } catch (parseError) {
        // If parsing with country fails, try without country (for international format)
        if (cleanedPhone.startsWith('+')) {
          phoneNumber = parsePhoneNumber(cleanedPhone);
        } else {
          throw parseError;
        }
      }
      
      // Validate the parsed number
      const isValid = phoneNumber && phoneNumber.isValid();
      
      if (!isValid) {
        return this.buildValidationResult(phone, {
          valid: false,
          error: 'Invalid phone number format',
          formatValid: false,
          country: country
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
        country: phoneNumber.country,
        type: phoneNumber.getType(),
        isMobile: phoneNumber.getType() === 'MOBILE',
        isFixedLine: phoneNumber.getType() === 'FIXED_LINE',
        isPossible: phoneNumber.isPossible()
      };
      
      // Additional mobile detection for specific countries
      if (!phoneDetails.isMobile && !phoneDetails.isFixedLine) {
        phoneDetails.isMobile = this.checkIfMobile(phoneDetails.e164, phoneDetails.country);
      }
      
      return this.buildValidationResult(phone, phoneDetails, clientId);
      
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
          parseError: error.message
        }, clientId);
      }
      
      // Generic error
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Failed to validate phone number',
        formatValid: false
      }, clientId);
    }
  }
  
  // Clean phone number
  cleanPhoneNumber(phone) {
    // Convert to string and trim
    let cleaned = String(phone).trim();
    
    // Remove common formatting characters but keep + for international
    cleaned = cleaned.replace(/[\s\-\(\)\.]/g, '');
    
    // Handle common prefixes
    if (cleaned.startsWith('00')) {
      // International prefix used in some countries
      cleaned = '+' + cleaned.substring(2);
    }
    
    return cleaned;
  }
  
  // Check if number is mobile based on patterns
  checkIfMobile(e164, countryCode) {
    const countryData = this.countryPhoneData.get(countryCode);
    if (!countryData || countryData.mobileBeginnsWith.length === 0) {
      return false;
    }
    
    // Remove country calling code to get national number
    const nationalNumber = e164.replace(countryData.callingCode, '');
    
    // Check if it starts with any mobile pattern
    return countryData.mobileBeginnsWith.some(pattern => 
      nationalNumber.startsWith(pattern)
    );
  }
  
  // Build validation result
  buildValidationResult(originalPhone, validationData, clientId) {
    const isValid = validationData.valid === true;
    
    const result = {
      originalPhone,
      valid: isValid,
      formatValid: validationData.formatValid !== false,
      error: validationData.error || null,
      
      // Phone formats
      e164: validationData.e164 || null,
      international: validationData.international || null,
      national: validationData.national || null,
      
      // Phone details
      countryCode: validationData.countryCode || null,
      country: validationData.country || null,
      type: validationData.type || 'UNKNOWN',
      isMobile: validationData.isMobile || false,
      isFixedLine: validationData.isFixedLine || false,
      
      // Unmessy fields
      um_phone: validationData.e164 || originalPhone,
      um_phone_status: isValid ? 'Valid' : 'Invalid',
      um_phone_format: validationData.international || originalPhone,
      um_phone_country_code: validationData.countryCode || '',
      um_phone_country: validationData.country || '',
      um_phone_is_mobile: validationData.isMobile ? 'Yes' : 'No',
      
      // Validation steps
      validationSteps: [
        {
          step: 'format_check',
          passed: validationData.formatValid !== false
        },
        {
          step: 'number_parsing',
          passed: isValid
        },
        {
          step: 'type_detection',
          type: validationData.type || 'UNKNOWN'
        }
      ]
    };
    
    return result;
  }
  
  // Cache operations
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
          valid: data.valid,
          formatValid: true,
          e164: data.e164,
          international: data.international_format,
          national: data.national_format,
          countryCode: data.country_code,
          country: data.country,
          type: data.phone_type,
          isMobile: data.is_mobile,
          isFixedLine: data.phone_type === 'FIXED_LINE',
          um_phone: data.e164,
          um_phone_status: data.valid ? 'Valid' : 'Invalid',
          um_phone_format: data.international_format,
          um_phone_country_code: data.country_code,
          um_phone_country: data.country,
          um_phone_is_mobile: data.is_mobile ? 'Yes' : 'No',
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
        international_format: validationResult.international,
        national_format: validationResult.national,
        country_code: validationResult.countryCode,
        country: validationResult.country,
        phone_type: validationResult.type,
        is_mobile: validationResult.isMobile,
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
}

// Create singleton instance
const phoneValidationService = new PhoneValidationService();

// Export the class and instance
export { phoneValidationService, PhoneValidationService };