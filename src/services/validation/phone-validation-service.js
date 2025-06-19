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
import countryMappings from './data/country-mappings.json';

const logger = createServiceLogger('phone-validation-service');

class PhoneValidationService {
  constructor() {
    this.logger = logger;
    this.logger.info('Phone validation service initialized using libphonenumber-js');
    
    // Initialize country mappings and territory info
    this.initializeCountryMappings();
    this.initializeTerritoryMappings();
    this.initializeCallingCodeCache();
  }
  
  // Initialize country mappings from JSON
  initializeCountryMappings() {
    this.countryNameToCode = new Map();
    
    // Build the mapping from the JSON data
    Object.entries(countryMappings).forEach(([isoCode, aliases]) => {
      aliases.forEach(alias => {
        const normalized = this.normalizeCountryInput(alias);
        this.countryNameToCode.set(normalized, isoCode);
      });
    });
    
    this.logger.info('Country mappings loaded', {
      totalMappings: this.countryNameToCode.size
    });
  }
  
  // Initialize territory mappings
  initializeTerritoryMappings() {
    // Define territories and their parent countries
    this.territoryMap = {
      'CC': 'AU', // Cocos Islands uses Australian numbering
      'CX': 'AU', // Christmas Island uses Australian numbering
      'GU': 'US', // Guam uses US numbering
      'PR': 'US', // Puerto Rico uses US numbering
      'VI': 'US', // US Virgin Islands
      'AS': 'US', // American Samoa
      'MP': 'US', // Northern Mariana Islands
      'BM': 'GB', // Bermuda uses UK-like numbering
      'GI': 'GB', // Gibraltar
      'IM': 'GB', // Isle of Man
      'JE': 'GB', // Jersey
      'GG': 'GB', // Guernsey
      'RE': 'FR', // Réunion uses French numbering
      'GP': 'FR', // Guadeloupe
      'MQ': 'FR', // Martinique
      'GF': 'FR', // French Guiana
      'YT': 'FR', // Mayotte
      'PM': 'FR', // Saint Pierre and Miquelon
      'BL': 'FR', // Saint Barthélemy
      'MF': 'FR', // Saint Martin
      'PF': 'FR', // French Polynesia
      'NC': 'FR', // New Caledonia
      'WF': 'FR', // Wallis and Futuna
      'AX': 'FI', // Åland Islands uses Finnish numbering
      'SJ': 'NO', // Svalbard uses Norwegian numbering
      'BV': 'NO', // Bouvet Island
    };
  }
  
  // Initialize calling code to country cache using libphonenumber-js
  initializeCallingCodeCache() {
    this.callingCodeToCountries = new Map();
    
    // Build reverse mapping from calling code to countries
    const countries = getCountries();
    countries.forEach(country => {
      try {
        const callingCode = getCountryCallingCode(country);
        if (callingCode) {
          if (!this.callingCodeToCountries.has(callingCode)) {
            this.callingCodeToCountries.set(callingCode, []);
          }
          this.callingCodeToCountries.get(callingCode).push(country);
        }
      } catch (e) {
        // Some territories might not have calling codes
      }
    });
    
    this.logger.info('Calling code cache initialized', {
      totalCodes: this.callingCodeToCountries.size
    });
  }
  
  // Normalize country input for matching
  normalizeCountryInput(input) {
    if (!input) return '';
    
    return input
      .toString()
      .toUpperCase()
      .trim()
      .replace(/[^A-Z0-9\u0080-\uFFFF\s]/g, '') // Keep unicode chars for non-Latin scripts
      .replace(/\s+/g, ' ')
      .trim();
  }
  
  // Resolve country input to ISO code
  resolveCountryCode(countryInput) {
    if (!countryInput) return null;
    
    const normalized = this.normalizeCountryInput(countryInput);
    
    // First, check if it's already a valid ISO code
    const upperInput = countryInput.toUpperCase().trim();
    if (upperInput.length === 2 && getCountries().includes(upperInput)) {
      return upperInput;
    }
    
    // Check our mappings
    const mappedCode = this.countryNameToCode.get(normalized);
    if (mappedCode) {
      return mappedCode;
    }
    
    // Try fuzzy matching for common variations
    for (const [key, value] of this.countryNameToCode) {
      if (key.includes(normalized) || normalized.includes(key)) {
        if (normalized.length > 3 && key.length > 3) { // Avoid false matches on short strings
          return value;
        }
      }
    }
    
    // Check if it's a phone prefix (like +1, +44, etc.)
    if (countryInput.startsWith('+') || /^\d+$/.test(countryInput)) {
      const prefix = countryInput.replace('+', '');
      
      // Use libphonenumber-js's data to find country by calling code
      const countries = this.callingCodeToCountries.get(prefix);
      if (countries && countries.length > 0) {
        // Return the main country for this calling code
        // For shared codes like +1, this returns 'US' as the primary
        return countries[0];
      }
    }
    
    // Log unmatched country for debugging
    this.logger.debug('Could not resolve country code', { 
      input: countryInput, 
      normalized 
    });
    
    return null;
  }
  
  // Get countries by calling code using library
  getCountriesByCallingCode(callingCode) {
    return this.callingCodeToCountries.get(callingCode) || [];
  }
  
  // External API placeholder - Numverify
  async validateWithNumverify(phone, country = null) {
    // TODO: Implement Numverify API call
    // This is a placeholder for the external validation service
    this.logger.info('Numverify validation placeholder called', { phone, country });
    
    // Placeholder response structure
    return {
      valid: false,
      number: phone,
      local_format: '',
      international_format: '',
      country_prefix: '',
      country_code: '',
      country_name: '',
      location: '',
      carrier: '',
      line_type: 'unknown',
      error: 'Numverify integration not implemented'
    };
  }
  
  // Calculate confidence for a specific country match
  calculateCountryConfidence(phoneNumber, originalPhone, country, hintCountry = null) {
    let score = 0;
    const factors = [];
    
    // Base score for valid numbers - the library already checked patterns!
    if (phoneNumber.isValid()) {
      score += 40;  // Base score for valid format
      factors.push('valid_format');
    } else if (phoneNumber.isPossible()) {
      score += 20;
      factors.push('possible_format');
    }
    
    // BIG BONUS: Is this an actual country (not a territory)?
    const isActualCountry = !this.territoryMap[country];
    if (isActualCountry) {
      score += 25;  // Significant bonus for actual countries
      factors.push('actual_country');
    } else {
      score += 5;   // Small bonus for territories
      factors.push('territory');
    }
    
    // BIG BONUS: Does the predicted country match the hint?
    if (hintCountry && country === hintCountry) {
      score += 30;  // Major bonus when hint matches
      factors.push('hint_country_match');
    }
    
    // Phone type clarity
    const phoneType = phoneNumber.getType();
    if (phoneType === 'MOBILE' || phoneType === 'FIXED_LINE') {
      score += 15;
      factors.push('definite_type');
    } else if (phoneType === 'FIXED_LINE_OR_MOBILE') {
      score += 8;
      factors.push('ambiguous_type');
    } else {
      score += 3;
      factors.push('unknown_type');
    }
    
    // National number completeness
    const nationalNumber = phoneNumber.nationalNumber;
    if (nationalNumber && nationalNumber.length >= 6) {
      score += 5;
      factors.push('complete_number');
    }
    
    // Convert to level
    let level;
    if (score >= 90) level = 'very_high';
    else if (score >= 70) level = 'high';
    else if (score >= 50) level = 'medium';
    else if (score >= 30) level = 'low';
    else level = 'very_low';
    
    return { score, level, factors };
  }
  
  // Predict possible countries for a phone number
  predictCountries(cleanedPhone, hintCountry = null) {
    const predictions = [];
    const allCountries = getCountries();
    
    // Try parsing with each country and collect valid matches
    for (const country of allCountries) {
      try {
        const phoneNumber = parsePhoneNumber(cleanedPhone, country);
        if (phoneNumber && phoneNumber.isPossible()) {
          const isValid = phoneNumber.isValid();
          const confidence = this.calculateCountryConfidence(phoneNumber, cleanedPhone, country, hintCountry);
          
          predictions.push({
            country,
            countryName: this.getCountryName(country),
            valid: isValid,
            possible: true,
            phoneType: phoneNumber.getType(),
            confidence: confidence.score,
            confidenceLevel: confidence.level,
            confidenceFactors: confidence.factors,
            isActualCountry: !this.territoryMap[country],
            parentCountry: this.territoryMap[country] || null,
            format: {
              e164: phoneNumber.format('E.164'),
              international: phoneNumber.format('INTERNATIONAL'),
              national: phoneNumber.format('NATIONAL')
            }
          });
        }
      } catch (e) {
        // Country doesn't match this number format
      }
    }
    
    // Sort by confidence score (highest first)
    predictions.sort((a, b) => b.confidence - a.confidence);
    
    return predictions;
  }
  
  // Find best matching country from predictions
  findBestCountryMatch(predictions, hintCountry = null) {
    if (!predictions || predictions.length === 0) {
      return null;
    }
    
    // If hint country provided, check if it's in predictions with good confidence
    if (hintCountry) {
      const hintMatch = predictions.find(p => p.country === hintCountry);
      if (hintMatch && hintMatch.confidence >= 50) {  // Slightly higher threshold for hints
        return hintMatch;
      }
    }
    
    // Otherwise, return highest confidence match
    return predictions[0];
  }
  
  // Deduplicate territory predictions
  deduplicateTerritoryPredictions(predictions) {
    const seen = new Map();
    const deduplicated = [];
    
    for (const prediction of predictions) {
      const mainCountry = this.territoryMap[prediction.country] || prediction.country;
      const e164 = prediction.format.e164;
      
      // Create a key based on the main country and phone format
      const key = `${mainCountry}-${e164}`;
      
      if (!seen.has(key)) {
        // First time seeing this combination
        seen.set(key, {
          mainCountry,
          territories: [prediction.country],
          prediction: prediction
        });
      } else {
        // Add this territory to the existing entry
        const entry = seen.get(key);
        if (!entry.territories.includes(prediction.country)) {
          entry.territories.push(prediction.country);
        }
        // Update prediction if this one has higher confidence
        if (prediction.confidence > entry.prediction.confidence) {
          entry.prediction = prediction;
        }
      }
    }
    
    // Convert map back to array
    for (const entry of seen.values()) {
      const prediction = entry.prediction;
      
      // If this is a territory, update to use main country
      if (this.territoryMap[prediction.country]) {
        prediction.originalCountry = prediction.country;
        prediction.country = entry.mainCountry;
        prediction.countryName = this.getCountryName(entry.mainCountry);
        prediction.note = `Also valid for: ${entry.territories.join(', ')}`;
      }
      
      deduplicated.push(prediction);
    }
    
    // Re-sort by confidence
    deduplicated.sort((a, b) => b.confidence - a.confidence);
    
    return deduplicated;
  }
  
  // Main validation method
  async validatePhoneNumber(phone, options = {}) {
    const {
      country = null,
      countryHint = null,
      useExternalApi = true,
      confidenceThreshold = 60,
      clientId = null,
      useCache = true
    } = options;
    
    // Resolve country input to ISO code
    const providedCountryRaw = country || countryHint;
    const providedCountry = this.resolveCountryCode(providedCountryRaw);
    
    if (providedCountryRaw && !providedCountry) {
      this.logger.warn('Could not resolve country input', { 
        input: providedCountryRaw 
      });
    }
    
    this.logger.debug('Starting phone validation', {
      phone,
      providedCountryRaw,
      providedCountry,
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
    let validationMethod = null;
    let confidence = null;
    let predictions = [];
    
    try {
      // Step 1: If number has international format, try auto-detection first
      if (originalHasPlus) {
        try {
          phoneNumber = parsePhoneNumberFromString(cleanedPhone);
          if (phoneNumber && phoneNumber.isValid()) {
            successfulCountry = phoneNumber.country;
            validationMethod = 'international_format';
            
            // Calculate confidence
            confidence = this.calculateValidationConfidence(phoneNumber, {
              method: validationMethod,
              originalHasPlus: true,
              providedCountry,
              matchedCountry: successfulCountry
            });
            
            this.logger.debug('Validated with international format', {
              country: successfulCountry,
              confidence: confidence.score
            });
          }
        } catch (e) {
          this.logger.debug('International format parsing failed', { error: e.message });
        }
      }
      
      // Step 2: If not validated yet, predict possible countries
      if (!phoneNumber || !phoneNumber.isValid()) {
        // Pass hint country to predictions for better scoring
        predictions = this.predictCountries(cleanedPhone, providedCountry);
        
        // Filter out duplicate territories (CC and CX share AU's numbering)
        const uniquePredictions = this.deduplicateTerritoryPredictions(predictions);
        
        this.logger.debug('Country predictions', {
          totalPredictions: uniquePredictions.length,
          topPredictions: uniquePredictions.slice(0, 5).map(p => ({
            country: p.country,
            confidence: p.confidence,
            valid: p.valid,
            factors: p.confidenceFactors
          }))
        });
        
        // Find best match considering the hint
        const bestMatch = this.findBestCountryMatch(uniquePredictions, providedCountry);
        
        if (bestMatch && bestMatch.valid) {
          // We already have the parsed phone number data from predictions
          successfulCountry = bestMatch.country;
          validationMethod = providedCountry === bestMatch.country ? 'hint_match' : 'predicted';
          
          // Use the confidence from prediction
          confidence = {
            score: bestMatch.confidence,
            level: bestMatch.confidenceLevel,
            factors: bestMatch.confidenceFactors
          };
          
          // Create a successful phoneNumber object using the prediction data
          try {
            phoneNumber = parsePhoneNumber(cleanedPhone, bestMatch.country);
            
            this.logger.debug('Validated with predicted country', {
              country: successfulCountry,
              confidence: bestMatch.confidence,
              wasHintUsed: providedCountry === bestMatch.country,
              factors: bestMatch.confidenceFactors
            });
          } catch (e) {
            this.logger.error('Failed to re-parse with best match country', {
              country: bestMatch.country,
              error: e.message
            });
          }
        }
      }
      
      // Step 3: Check if we need external validation
      const needsExternalValidation = useExternalApi && (
        !phoneNumber || 
        !phoneNumber.isValid() || 
        (confidence && confidence.score < confidenceThreshold) ||
        (phoneNumber && phoneNumber.getType() === 'UNKNOWN')
      );
      
      if (needsExternalValidation) {
        this.logger.info('Using external API due to low confidence or validation failure', {
          hasPhoneNumber: !!phoneNumber,
          isValid: phoneNumber?.isValid(),
          confidenceScore: confidence?.score,
          phoneType: phoneNumber?.getType()
        });
        
        // Call external API (Numverify)
        const externalResult = await this.validateWithNumverify(
          cleanedPhone,
          successfulCountry || providedCountry
        );
        
        // If external API provides better results, use them
        if (externalResult.valid) {
          // Build result from external API data
          return this.buildValidationResult(phone, {
            valid: true,
            formatValid: true,
            e164: externalResult.international_format,
            international: externalResult.international_format,
            national: externalResult.local_format,
            countryCode: externalResult.country_prefix,
            country: externalResult.country_code,
            type: externalResult.line_type?.toUpperCase() || 'UNKNOWN',
            isMobile: externalResult.line_type === 'mobile',
            isFixedLine: externalResult.line_type === 'fixed_line',
            carrier: externalResult.carrier,
            location: externalResult.location,
            confidence: {
              score: 95,
              level: 'very_high',
              factors: ['external_api_verified', 'numverify']
            },
            validationMethod: 'external_api',
            externalApiUsed: true
          }, clientId);
        }
      }
      
      // Step 4: Return validation result
      if (!phoneNumber || !phoneNumber.isValid()) {
        // But wait! Check if we have valid predictions even if parsing failed
        const validPrediction = predictions.find(p => p.valid);
        if (validPrediction) {
          // Use the first valid prediction
          return this.buildValidationResult(phone, {
            valid: true,
            formatValid: true,
            e164: validPrediction.format.e164,
            international: validPrediction.format.international,
            national: validPrediction.format.national,
            countryCode: validPrediction.format.e164.match(/^\+(\d+)/)?.[1],
            country: validPrediction.country,
            type: validPrediction.phoneType,
            isMobile: validPrediction.phoneType === 'MOBILE',
            isFixedLine: validPrediction.phoneType === 'FIXED_LINE',
            isFixedLineOrMobile: validPrediction.phoneType === 'FIXED_LINE_OR_MOBILE',
            isPossible: true,
            confidence: {
              score: validPrediction.confidence,
              level: validPrediction.confidenceLevel,
              factors: validPrediction.confidenceFactors
            },
            validationMethod: 'prediction_fallback',
            hintCountryUsed: providedCountry === validPrediction.country,
            externalApiUsed: false,
            countryName: validPrediction.countryName
          }, clientId);
        }
        
        // Really failed validation
        return this.buildValidationResult(phone, {
          valid: false,
          error: 'Invalid phone number format',
          formatValid: false,
          attemptedCountry: providedCountry,
          attemptedCountryInput: providedCountryRaw,
          predictions: uniquePredictions.slice(0, 3), // Top 3 predictions
          confidence: {
            score: 0,
            level: 'none',
            factors: ['validation_failed']
          }
        }, clientId);
      }
      
      // Successful validation
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
        confidence,
        validationMethod,
        hintCountryUsed: providedCountry === successfulCountry,
        externalApiUsed: false
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
      
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Failed to validate phone number',
        formatValid: false,
        confidence: {
          score: 0,
          level: 'none',
          factors: ['system_error']
        }
      }, clientId);
    }
  }
  
  // Calculate overall validation confidence
  calculateValidationConfidence(phoneNumber, context = {}) {
    const {
      method,
      originalHasPlus,
      providedCountry,
      matchedCountry
    } = context;
    
    let score = 0;
    const factors = [];
    
    // Method scoring
    if (method === 'international_format' && originalHasPlus) {
      score += 40;
      factors.push('international_format');
    } else if (method === 'hint_match' && providedCountry === matchedCountry) {
      score += 35;
      factors.push('country_hint_matched');
    } else if (method === 'predicted') {
      score += 25;
      factors.push('country_predicted');
    }
    
    // Validity scoring
    if (phoneNumber.isValid() && phoneNumber.isPossible()) {
      score += 30;
      factors.push('valid_and_possible');
    } else if (phoneNumber.isValid()) {
      score += 20;
      factors.push('valid_only');
    }
    
    // Type scoring
    const phoneType = phoneNumber.getType();
    if (phoneType === 'MOBILE' || phoneType === 'FIXED_LINE') {
      score += 20;
      factors.push('definite_line_type');
    } else if (phoneType === 'FIXED_LINE_OR_MOBILE') {
      score += 10;
      factors.push('ambiguous_line_type');
    } else {
      score += 0;
      factors.push('unknown_line_type');
    }
    
    // Additional factors
    if (providedCountry && providedCountry !== matchedCountry) {
      score -= 10;
      factors.push('country_hint_mismatch');
    }
    
    // Ensure score is between 0 and 100
    score = Math.max(0, Math.min(100, score));
    
    // Convert to level
    let level;
    if (score >= 85) level = 'very_high';
    else if (score >= 70) level = 'high';
    else if (score >= 50) level = 'medium';
    else if (score >= 30) level = 'low';
    else level = 'very_low';
    
    return { score, level, factors };
  }
  
  // Clean phone number
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
  
  // Get country name from code (keeping abbreviated for space)
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
      'CC': 'Cocos Islands',
      'CX': 'Christmas Island',
      'GU': 'Guam',
      'AS': 'American Samoa',
      'MP': 'Northern Mariana Islands',
      // Add all other countries...
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
      
      // Location info
      location: validationData.location || countryName || 'Unknown',
      carrier: validationData.carrier || '',
      
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
      
      // Validation method
      validationMethod: validationData.validationMethod || 'unknown',
      externalApiUsed: validationData.externalApiUsed || false,
      
      // Unmessy fields
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
    if (validationData.predictions) {
      result.possibleCountries = validationData.predictions;
    }
    
    if (validationData.hintCountryUsed !== undefined) {
      result.hintCountryUsed = validationData.hintCountryUsed;
    }
    
    if (validationData.attemptedCountryInput) {
      result.attemptedCountryInput = validationData.attemptedCountryInput;
    }
    
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
          currentPhone: data.e164,
          valid: data.valid,
          possible: true,
          formatValid: true,
          type: data.phone_type,
          location: this.getCountryName(data.country),
          carrier: data.carrier || '',
          e164: data.e164,
          internationalFormat: data.international_format,
          nationalFormat: data.national_format,
          uri: `tel:${data.e164}`,
          countryCode: data.country,
          countryCallingCode: data.country_code,
          confidence: data.confidence_level || 'high',
          confidenceScore: data.confidence_score || 90,
          confidenceFactors: ['cached_result', 'previously_validated'],
          validationMethod: data.validation_method || 'cached',
          externalApiUsed: data.external_api_used || false,
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
        validation_method: validationResult.validationMethod,
        external_api_used: validationResult.externalApiUsed,
        carrier: validationResult.carrier,
        client_id: clientId
      });
      
      this.logger.debug('Phone validation saved to cache', { 
        phone: validationResult.e164,
        confidence: validationResult.confidence,
        method: validationResult.validationMethod,
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
      callingCode: getCountryCallingCode(country),
      isTerritory: !!this.territoryMap[country],
      parentCountry: this.territoryMap[country] || null
    }));
  }
}

// Create singleton instance
const phoneValidationService = new PhoneValidationService();

// Export the class and instance
export { phoneValidationService, PhoneValidationService };