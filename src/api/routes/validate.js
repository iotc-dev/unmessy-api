// src/api/routes/validate.js
import express from 'express';
import Joi from 'joi';
import { asyncHandler } from '../middleware/error-handler.js';
import { authMiddleware } from '../middleware/auth.js';
import { validate } from '../middleware/validate-input.js';
import { ValidationError, RateLimitError } from '../../core/errors.js';
import { createServiceLogger } from '../../core/logger.js';
import clientService from '../../services/client-service.js';
import validationService from '../../services/validation-service.js';
import { config } from '../../core/config.js';

// Create logger instance
const logger = createServiceLogger('validation-api');

const router = express.Router();

/**
 * Email validation endpoint
 * Validates email format, corrects typos, and checks deliverability
 */
router.post('/email', 
  authMiddleware(), 
  validate.email(),
  asyncHandler(async (req, res) => {
    const startTime = Date.now();
    const { clientId } = req;
    let validationResult = null;
    let errorType = null;
    
    try {
      // Get email from request
      const { email } = req.body;
      
      // Check rate limit
      const rateLimitCheck = await clientService.checkRateLimit(clientId, 'email');
      if (!rateLimitCheck.allowed) {
        throw new RateLimitError('email', 
          rateLimitCheck.limit, 
          rateLimitCheck.limit - rateLimitCheck.remaining, 
          rateLimitCheck.remaining
        );
      }
      
      // Log request
      logger.info('Email validation request', { 
        clientId,
        email: email.substring(0, email.indexOf('@') + 1) + '***',
        timestamp: new Date().toISOString()
      });
      
      // Validate email
      validationResult = await validationService.validateEmail(email, {
        clientId,
        skipZeroBounce: req.query.skipExternal === 'true'
      });
      
      // Increment usage count
      await clientService.incrementUsage(clientId, 'email');
      
      // Get client stats for response
      const clientStats = await clientService.getClientStats(clientId);
      
      // Build response
      const response = {
        ...validationResult,
        client: {
          id: clientStats.clientId,
          name: clientStats.name,
          um_account_type: clientStats.um_account_type || 'basic',
          processed_count: clientStats.totalEmailCount || 0,
          daily_count: clientStats.emailCount,
          emailLimit: clientStats.dailyEmailLimit,
          remaining: clientStats.remainingEmail
        }
      };
      
      // Record metrics
      const responseTime = Date.now() - startTime;
      const isSuccess = validationResult.status === 'valid' || 
                       validationResult.status === 'catch-all' || 
                       validationResult.status === 'unknown';
                       
      await clientService.recordValidationMetric(
        clientId, 
        'email', 
        isSuccess,
        responseTime,
        errorType
      );
      
      // Log success
      logger.info('Email validation completed', {
        clientId,
        status: validationResult.status,
        responseTime: `${responseTime}ms`
      });
      
      return res.status(200).json(response);
      
    } catch (error) {
      // Record failed metric
      const responseTime = Date.now() - startTime;
      
      // Determine error type
      if (error.name === 'ExternalServiceError') {
        errorType = 'external_api';
      } else if (error.name === 'TimeoutError') {
        errorType = 'timeout';
      } else {
        errorType = 'internal';
      }
      
      await clientService.recordValidationMetric(
        clientId, 
        'email', 
        false,
        responseTime,
        errorType
      );
      
      // Re-throw error to be handled by error-handler middleware
      throw error;
    }
  })
);

/**
 * Name validation endpoint
 * Validates and formats names with proper capitalization
 */
router.post('/name', 
  authMiddleware(), 
  validate.name(),
  asyncHandler(async (req, res) => {
    const startTime = Date.now();
    const { clientId } = req;
    let validationResult = null;
    let errorType = null;
    
    try {
      // Get name from request (either full name or first/last)
      const { name, first_name, last_name } = req.body;
      
      // Check rate limit
      const rateLimitCheck = await clientService.checkRateLimit(clientId, 'name');
      if (!rateLimitCheck.allowed) {
        throw new RateLimitError('name', 
          rateLimitCheck.limit, 
          rateLimitCheck.limit - rateLimitCheck.remaining, 
          rateLimitCheck.remaining
        );
      }
      
      // Log request
      logger.info('Name validation request', { 
        clientId,
        hasFullName: !!name,
        hasFirstLast: !!(first_name || last_name),
        timestamp: new Date().toISOString()
      });
      
      // Validate name (either full name or separate first/last)
      if (name) {
        validationResult = await validationService.validateFullName(name, { clientId });
      } else if (first_name || last_name) {
        validationResult = await validationService.validateSeparateNames(first_name, last_name, { clientId });
      } else {
        throw new ValidationError('Either name or first_name/last_name is required');
      }
      
      // Increment usage count
      await clientService.incrementUsage(clientId, 'name');
      
      // Get client stats for response
      const clientStats = await clientService.getClientStats(clientId);
      
      // Build response
      const response = {
        ...validationResult,
        client: {
          id: clientStats.clientId,
          name: clientStats.name,
          um_account_type: clientStats.um_account_type || 'basic',
          processed_count: clientStats.totalNameCount || 0,
          daily_count: clientStats.nameCount,
          daily_limit: clientStats.dailyNameLimit,
          remaining: clientStats.remainingName
        }
      };
      
      // Record metrics
      const responseTime = Date.now() - startTime;
      const isSuccess = validationResult.status === 'valid';
      
      await clientService.recordValidationMetric(
        clientId, 
        'name', 
        isSuccess,
        responseTime,
        errorType
      );
      
      // Log success
      logger.info('Name validation completed', {
        clientId,
        status: validationResult.status,
        responseTime: `${responseTime}ms`
      });
      
      return res.status(200).json(response);
      
    } catch (error) {
      // Record failed metric
      const responseTime = Date.now() - startTime;
      
      // Determine error type
      if (error.name === 'ExternalServiceError') {
        errorType = 'external_api';
      } else if (error.name === 'TimeoutError') {
        errorType = 'timeout';
      } else {
        errorType = 'internal';
      }
      
      await clientService.recordValidationMetric(
        clientId, 
        'name', 
        false,
        responseTime,
        errorType
      );
      
      // Re-throw error to be handled by error-handler middleware
      throw error;
    }
  })
);

/**
 * Phone validation endpoint
 * Validates phone numbers with international formatting
 */
router.post('/phone', 
  authMiddleware(), 
  validate.phone(),
  asyncHandler(async (req, res) => {
    const startTime = Date.now();
    const { clientId } = req;
    let validationResult = null;
    let errorType = null;
    
    try {
      // Get phone number from request
      const { phone, country } = req.body;
      
      // Check rate limit
      const rateLimitCheck = await clientService.checkRateLimit(clientId, 'phone');
      if (!rateLimitCheck.allowed) {
        throw new RateLimitError('phone', 
          rateLimitCheck.limit, 
          rateLimitCheck.limit - rateLimitCheck.remaining, 
          rateLimitCheck.remaining
        );
      }
      
      // Log request
      logger.info('Phone validation request', { 
        clientId,
        phone: phone.substring(0, 3) + '***' + phone.substring(phone.length - 2),
        country: country || config.validation.phone.defaultCountry,
        timestamp: new Date().toISOString()
      });
      
      // Validate phone number
      validationResult = await validationService.validatePhone(phone, {
        clientId,
        country: country || config.validation.phone.defaultCountry
      });
      
      // Increment usage count
      await clientService.incrementUsage(clientId, 'phone');
      
      // Get client stats for response
      const clientStats = await clientService.getClientStats(clientId);
      
      // Build response
      const response = {
        ...validationResult,
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
      
      // Record metrics - Fixed to check isValid instead of valid
      const responseTime = Date.now() - startTime;
      const isSuccess = validationResult.isValid === true;
      
      await clientService.recordValidationMetric(
        clientId, 
        'phone', 
        isSuccess,
        responseTime,
        errorType
      );
      
      // Log success
      logger.info('Phone validation completed', {
        clientId,
        valid: validationResult.isValid,
        responseTime: `${responseTime}ms`
      });
      
      return res.status(200).json(response);
      
    } catch (error) {
      // Record failed metric
      const responseTime = Date.now() - startTime;
      
      // Determine error type
      if (error.name === 'ExternalServiceError') {
        errorType = 'external_api';
      } else if (error.name === 'TimeoutError') {
        errorType = 'timeout';
      } else {
        errorType = 'internal';
      }
      
      await clientService.recordValidationMetric(
        clientId, 
        'phone', 
        false,
        responseTime,
        errorType
      );
      
      // Re-throw error to be handled by error-handler middleware
      throw error;
    }
  })
);

/**
 * Address validation endpoint
 * Validates and standardizes addresses
 */
router.post('/address', 
  authMiddleware(), 
  validate.address(),
  asyncHandler(async (req, res) => {
    const startTime = Date.now();
    const { clientId } = req;
    let validationResult = null;
    let errorType = null;
    
    try {
      // Get address data from request
      const addressData = req.body;
      
      // Check rate limit
      const rateLimitCheck = await clientService.checkRateLimit(clientId, 'address');
      if (!rateLimitCheck.allowed) {
        throw new RateLimitError('address', 
          rateLimitCheck.limit, 
          rateLimitCheck.limit - rateLimitCheck.remaining, 
          rateLimitCheck.remaining
        );
      }
      
      // Log request
      logger.info('Address validation request', { 
        clientId,
        hasFullAddress: !!addressData.address,
        hasComponents: !!(addressData.address_line_1 || addressData.street_name),
        timestamp: new Date().toISOString()
      });
      
      // Validate address
      validationResult = await validationService.validateAddress(addressData, {
        clientId,
        useOpenCage: req.query.skipGeocoding !== 'true' && config.validation.address.geocode,
        country: addressData.country_code || config.validation.address.defaultCountry
      });
      
      // Increment usage count
      await clientService.incrementUsage(clientId, 'address');
      
      // Get client stats for response
      const clientStats = await clientService.getClientStats(clientId);
      
      // Build response
      const response = {
        ...validationResult,
        client: {
          id: clientStats.clientId,
          name: clientStats.name,
          um_account_type: clientStats.um_account_type || 'basic',
          processed_count: clientStats.totalAddressCount || 0,
          daily_count: clientStats.addressCount,
          daily_limit: clientStats.dailyAddressLimit,
          remaining: clientStats.remainingAddress
        }
      };
      
      // Record metrics - Fixed to check isValid instead of valid
      const responseTime = Date.now() - startTime;
      const isSuccess = validationResult.isValid === true;
      
      await clientService.recordValidationMetric(
        clientId, 
        'address', 
        isSuccess,
        responseTime,
        errorType
      );
      
      // Log success
      logger.info('Address validation completed', {
        clientId,
        valid: validationResult.isValid,
        confidence: validationResult.confidence,
        responseTime: `${responseTime}ms`
      });
      
      return res.status(200).json(response);
      
    } catch (error) {
      // Record failed metric
      const responseTime = Date.now() - startTime;
      
      // Determine error type
      if (error.name === 'ExternalServiceError') {
        errorType = 'external_api';
      } else if (error.name === 'TimeoutError') {
        errorType = 'timeout';
      } else {
        errorType = 'internal';
      }
      
      await clientService.recordValidationMetric(
        clientId, 
        'address', 
        false,
        responseTime,
        errorType
      );
      
      // Re-throw error to be handled by error-handler middleware
      throw error;
    }
  })
);

/**
 * Batch validation endpoint
 * Validates multiple items in a single request
 */
router.post('/batch', 
  authMiddleware(), 
  asyncHandler(async (req, res) => {
    const startTime = Date.now();
    const { clientId } = req;
    let errorType = null;
    
    try {
      const { items, type } = req.body;
      
      if (!items || !Array.isArray(items)) {
        throw new ValidationError('Items must be an array');
      }
      
      if (!type || !['email', 'name', 'phone', 'address'].includes(type)) {
        throw new ValidationError('Type must be one of: email, name, phone, address');
      }
      
      // Check batch size
      const maxBatchSize = 100;
      if (items.length > maxBatchSize) {
        throw new ValidationError(`Batch size cannot exceed ${maxBatchSize}`);
      }
      
      // Check rate limit for the entire batch
      const rateLimitCheck = await clientService.checkRateLimit(clientId, type, items.length);
      if (!rateLimitCheck.allowed) {
        throw new RateLimitError(type, 
          rateLimitCheck.limit, 
          rateLimitCheck.limit - rateLimitCheck.remaining, 
          rateLimitCheck.remaining
        );
      }
      
      // Log request
      logger.info('Batch validation request', { 
        clientId,
        type,
        itemCount: items.length,
        timestamp: new Date().toISOString()
      });
      
      // Process each item
      let results = [];
      let successCount = 0;
      let failureCount = 0;
      
      switch (type) {
        case 'email':
          results = await Promise.all(
            items.map(async (email) => {
              try {
                const result = await validationService.validateEmail(email, { clientId });
                if (result.status === 'valid' || result.status === 'catch-all' || result.status === 'unknown') {
                  successCount++;
                } else {
                  failureCount++;
                }
                return result;
              } catch (error) {
                failureCount++;
                return { error: error.message };
              }
            })
          );
          break;
        case 'name':
          results = await Promise.all(
            items.map(async (name) => {
              try {
                let result;
                if (typeof name === 'string') {
                  result = await validationService.validateFullName(name, { clientId });
                } else if (name.first_name || name.last_name) {
                  result = await validationService.validateSeparateNames(
                    name.first_name, 
                    name.last_name, 
                    { clientId }
                  );
                } else {
                  failureCount++;
                  return { error: 'Invalid name format' };
                }
                
                if (result.status === 'valid') {
                  successCount++;
                } else {
                  failureCount++;
                }
                return result;
              } catch (error) {
                failureCount++;
                return { error: error.message };
              }
            })
          );
          break;
        case 'phone':
          results = await Promise.all(
            items.map(async (item) => {
              try {
                const phone = typeof item === 'string' ? item : item.phone;
                const country = item.country || config.validation.phone.defaultCountry;
                const result = await validationService.validatePhone(phone, { clientId, country });
                
                if (result.isValid) {
                  successCount++;
                } else {
                  failureCount++;
                }
                return result;
              } catch (error) {
                failureCount++;
                return { error: error.message };
              }
            })
          );
          break;
        case 'address':
          results = await Promise.all(
            items.map(async (address) => {
              try {
                const result = await validationService.validateAddress(address, { clientId });
                
                if (result.isValid) {
                  successCount++;
                } else {
                  failureCount++;
                }
                return result;
              } catch (error) {
                failureCount++;
                return { error: error.message };
              }
            })
          );
          break;
      }
      
      // Increment usage count
      await clientService.incrementUsage(clientId, type, items.length);
      
      // Get client stats for response
      const clientStats = await clientService.getClientStats(clientId);
      
      // Record metrics with success/failure counts
      const responseTime = Date.now() - startTime;
      const overallSuccess = failureCount === 0;
      
      await clientService.recordValidationMetric(
        clientId, 
        type, 
        overallSuccess,
        responseTime,
        errorType
      );
      
      // Log success
      logger.info('Batch validation completed', {
        clientId,
        type,
        itemCount: items.length,
        successCount,
        failureCount,
        responseTime: `${responseTime}ms`
      });
      
      // Return results
      return res.status(200).json({
        results,
        count: results.length,
        successCount,
        failureCount,
        client: {
          id: clientStats.clientId,
          name: clientStats.name,
          processed_count: clientStats[`total${type.charAt(0).toUpperCase() + type.slice(1)}Count`] || 0,
          daily_count: clientStats[`${type}Count`],
          daily_limit: clientStats[`daily${type.charAt(0).toUpperCase() + type.slice(1)}Limit`],
          remaining: clientStats[`remaining${type.charAt(0).toUpperCase() + type.slice(1)}`]
        }
      });
      
    } catch (error) {
      // Record failed metric
      const responseTime = Date.now() - startTime;
      
      // Determine error type
      if (error.name === 'ExternalServiceError') {
        errorType = 'external_api';
      } else if (error.name === 'TimeoutError') {
        errorType = 'timeout';
      } else {
        errorType = 'internal';
      }
      
      await clientService.recordValidationMetric(
        clientId, 
        'batch', 
        false,
        responseTime,
        errorType
      );
      
      // Re-throw error to be handled by error-handler middleware
      throw error;
    }
  })
);

/**
 * Generate validation field mappings
 * Helps clients map validation results to their own systems
 */
router.get('/field-mappings', 
  authMiddleware({ required: false }),
  asyncHandler(async (req, res) => {
    // Return field mappings for all validation types
    const mappings = {
      email: {
        input: ['email'],
        output: [
          'um_email',
          'um_email_status',
          'um_bounce_status',
          'date_last_um_check',
          'date_last_um_check_epoch',
          'um_check_id'
        ]
      },
      name: {
        input: ['name', 'first_name', 'last_name'],
        output: [
          'um_first_name',
          'um_last_name',
          'um_middle_name',
          'um_name',
          'um_name_status',
          'um_name_format',
          'um_honorific',
          'um_suffix'
        ]
      },
      phone: {
        input: ['phone', 'country'],
        output: [
          'um_phone',
          'um_phone_status',
          'um_phone_format',
          'um_phone_country_code',
          'um_phone_country',
          'um_phone_is_mobile'
        ]
      },
      address: {
        input: [
          'address', 
          'address_line_1', 
          'address_line_2',
          'city',
          'state',
          'state_province',
          'postal_code',
          'country',
          'country_code'
        ],
        output: [
          'um_house_number',
          'um_street_name',
          'um_street_type',
          'um_street_direction',
          'um_unit_type',
          'um_unit_number',
          'um_address_line_1',
          'um_address_line_2',
          'um_city',
          'um_state_province',
          'um_country',
          'um_country_code',
          'um_postal_code',
          'um_address_status'
        ]
      }
    };
    
    return res.status(200).json(mappings);
  })
);

export default router;