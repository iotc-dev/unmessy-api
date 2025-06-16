// api/routes/validate.js - Phone validation endpoint
// This would be part of your Express routes in the new structure

import { parsePhoneNumber } from 'libphonenumber-js';
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { logger } from '../../core/logger.js';
import { ValidationError, RateLimitError } from '../../core/errors.js';
import { clientService } from '../../services/client-service.js';
import { validationService } from '../../services/validation-service.js';

// Phone validation endpoint handler
export async function validatePhone(req, res, next) {
  const startTime = Date.now();
  const { clientId } = req.auth; // Assuming auth middleware sets this
  
  try {
    // Get phone number from request
    const { phone } = req.body;
    
    if (!phone) {
      throw new ValidationError('Phone number is required');
    }
    
    // Check rate limit
    const rateLimitCheck = await clientService.checkRateLimit(clientId, 'phone');
    if (rateLimitCheck.limited) {
      throw new RateLimitError('Phone validation rate limit exceeded', {
        limit: rateLimitCheck.limit,
        used: rateLimitCheck.used,
        remaining: rateLimitCheck.remaining
      });
    }
    
    // Log request
    logger.info('Phone validation request', { 
      clientId,
      phone,
      timestamp: new Date().toISOString()
    });
    
    // Validate the phone number
    const validationResult = await validatePhoneNumber(phone);
    
    // Increment usage count
    await clientService.incrementUsage(clientId, 'phone');
    
    // Get client stats for response
    const clientStats = await clientService.getClientStats(clientId);
    
    // Build response
    const response = buildPhoneValidationResponse(
      phone, 
      validationResult, 
      clientStats, 
      clientId
    );
    
    // Record metrics
    const responseTime = Date.now() - startTime;
    await recordValidationMetric(clientId, 'phone', validationResult.valid, responseTime);
    
    // Log success
    logger.info('Phone validation completed', {
      clientId,
      valid: validationResult.valid,
      responseTime: `${responseTime}ms`
    });
    
    return res.status(200).json(response);
    
  } catch (error) {
    // Error will be handled by error-handler middleware
    next(error);
  }
}

// Helper function to validate and parse phone number
async function validatePhoneNumber(phoneNumber) {
  try {
    logger.debug('Validating phone number', { phoneNumber });
    
    // Clean the phone number
    let cleanedNumber = phoneNumber.toString().trim();
    
    // Track if we modified the input
    let wasModified = false;
    
    // Try to parse the phone number
    let parsed;
    try {
      // Try parsing assuming it includes country code
      parsed = parsePhoneNumber(cleanedNumber);
    } catch (error) {
      logger.debug('Failed to parse phone number', { 
        phoneNumber: cleanedNumber, 
        error: error.message 
      });
      return {
        valid: false,
        formatValid: false,
        wasModified: false,
        error: 'Invalid phone number format'
      };
    }
    
    if (!parsed) {
      return {
        valid: false,
        formatValid: false,
        wasModified: false,
        error: 'Unable to parse phone number'
      };
    }
    
    // Check if the number is valid
    const isValid = parsed.isValid();
    
    if (!isValid) {
      return {
        valid: false,
        formatValid: false,
        wasModified: false,
        error: 'Invalid phone number',
        country: parsed.country || 'unknown'
      };
    }
    
    // Extract all components
    const nationalNumber = parsed.nationalNumber;
    const countryCode = parsed.countryCallingCode;
    const country = parsed.country || 'unknown';
    const type = parsed.getType();
    
    // Determine if mobile
    const isMobile = type === 'MOBILE' || type === 'FIXED_LINE_OR_MOBILE';
    
    // Check if the formatted version differs from input
    const internationalFormat = parsed.formatInternational();
    const e164Format = parsed.format('E.164');
    
    // Compare cleaned input with E.164 format to detect changes
    const inputWithoutSpaces = cleanedNumber.replace(/[\s\-\(\)\.]/g, '');
    const e164WithoutPlus = e164Format.replace('+', '');
    wasModified = inputWithoutSpaces !== e164WithoutPlus;
    
    return {
      valid: true,
      formatValid: true,
      wasModified: wasModified,
      parsed: parsed,
      formatted: {
        international: internationalFormat,
        national: parsed.formatNational(),
        e164: e164Format,
        rfc3966: parsed.format('RFC3966')
      },
      components: {
        countryCode: countryCode,
        country: country,
        nationalNumber: nationalNumber,
        type: type,
        isMobile: isMobile
      }
    };
  } catch (error) {
    logger.error('Unexpected error in phone validation', { error: error.message });
    return {
      valid: false,
      formatValid: false,
      wasModified: false,
      error: error.message
    };
  }
}

// Helper function to generate um_check_id
function generateUmCheckId(clientId) {
  const epochTime = Date.now();
  const lastSixDigits = String(epochTime).slice(-6);
  const clientIdStr = clientId || '0001';
  const firstThreeDigits = String(epochTime).slice(0, 3);
  const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
  const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
  const unmessyVersion = config.unmessyVersion || '100';
  return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${unmessyVersion}`);
}

// Build phone validation response
function buildPhoneValidationResponse(phoneNumber, validationResult, clientStats, clientId) {
  const now = new Date();
  const epochMs = now.getTime();
  const umCheckId = generateUmCheckId(clientId);
  
  // Base response structure
  const response = {
    // Original input
    original_phone: phoneNumber,
    
    // Unmessy phone fields
    um_phone: validationResult.valid ? validationResult.formatted.international : phoneNumber,
    um_phone_country_code: validationResult.valid ? `+${validationResult.components.countryCode}` : '',
    um_phone_status: validationResult.wasModified ? 'Changed' : 'Unchanged',
    um_phone_format: validationResult.formatValid ? 'Valid' : 'Invalid',
    um_phone_is_mobile: validationResult.valid ? validationResult.components.isMobile : false,
    um_phone_country: validationResult.valid ? validationResult.components.country : 'unknown',
    // Area code fields removed - set to empty strings
    um_phone_area_code: '',
    um_phone_area: '',
    
    // Timestamps
    date_last_um_check: now.toISOString(),
    date_last_um_check_epoch: epochMs,
    um_check_id: umCheckId,
    
    // Client usage information
    client: {
      id: clientStats.clientId,
      name: clientStats.name,
      um_account_type: clientStats.um_account_type || 'basic',
      processed_count: clientStats.totalPhoneCount || 0,
      daily_count: clientStats.phoneCount,
      daily_limit: clientStats.dailyPhoneLimit,
      remaining: clientStats.remainingPhone
    }
  };
  
  // Add error if validation failed
  if (!validationResult.valid) {
    response.error = validationResult.error;
  }
  
  // Add validation details
  response.validation_details = {
    valid: validationResult.valid,
    type: validationResult.components?.type || 'unknown',
    formatted: validationResult.formatted || {},
    was_modified: validationResult.wasModified
  };
  
  return response;
}

// Record validation metrics
async function recordValidationMetric(clientId, validationType, success, responseTime, errorType = null) {
  try {
    await db.query(
      `SELECT record_validation_metric($1, $2, $3, $4, $5)`,
      [clientId, validationType, success, responseTime, errorType]
    );
  } catch (error) {
    logger.error('Failed to record validation metrics', { error: error.message });
    // Don't throw - metrics are non-critical
  }
}

// Export for use in routes
export default {
  validatePhone
};