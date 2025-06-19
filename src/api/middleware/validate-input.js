// src/api/middleware/validate-input.js
import Joi from 'joi';
import { ValidationError } from '../../core/errors.js';
import { createServiceLogger } from '../../core/logger.js';

// Create logger instance
const logger = createServiceLogger('validate-input');

/**
 * Create validation middleware for request data
 * 
 * @param {Object} schema - Joi validation schema object with keys for body, query, params
 * @returns {Function} Express middleware function
 */
export function validateRequest(schema) {
  return (req, res, next) => {
    const validationErrors = [];
    
    // Validate each part of the request
    for (const [part, partSchema] of Object.entries(schema)) {
      const data = req[part];
      
      if (partSchema && data !== undefined) {
        const { error, value } = partSchema.validate(data, {
          abortEarly: false,
          stripUnknown: true,
          convert: true
        });
        
        if (error) {
          const errors = error.details.map(detail => ({
            field: detail.path.join('.'),
            message: detail.message,
            type: detail.type
          }));
          
          validationErrors.push(...errors);
        } else {
          // Replace request data with validated data
          req[part] = value;
        }
      }
    }
    
    // If there are validation errors, return them
    if (validationErrors.length > 0) {
      logger.debug('Input validation failed', {
        errors: validationErrors,
        path: req.path
      });
      
      return next(new ValidationError(
        'Invalid request data',
        validationErrors
      ));
    }
    
    next();
  };
}

/**
 * Predefined schemas for common validation needs
 */
export const schemas = {
  // Email validation schema
  email: {
    body: Joi.object({
      email: Joi.string().email().required()
        .messages({
          'string.email': 'Invalid email format',
          'string.empty': 'Email is required',
          'any.required': 'Email is required'
        })
    })
  },
  
  // Name validation schema
  name: {
    body: Joi.object({
      name: Joi.string().min(1).max(100),
      first_name: Joi.string().min(1).max(50),
      last_name: Joi.string().min(1).max(50)
    }).or('name', 'first_name', 'last_name')  // Fixed: removed array syntax
      .messages({
        'object.missing': 'Either name or first_name/last_name is required'
      })
  },
  
  // Phone validation schema
  phone: {
    body: Joi.object({
      phone: Joi.string().required()
        .messages({
          'string.empty': 'Phone number is required',
          'any.required': 'Phone number is required'
        }),
      country: Joi.string().max(50).optional()  // Allow up to 50 characters
    })
  },
  
  // Address validation schema
  address: {
    body: Joi.object({
      address: Joi.string(),
      address_line_1: Joi.string(),
      address_line_2: Joi.string(),
      city: Joi.string(),
      state: Joi.string(),
      state_province: Joi.string(),
      postal_code: Joi.string(),
      country: Joi.string(),
      country_code: Joi.string().min(2).max(2),
      
      // Address components
      house_number: Joi.string(),
      street_name: Joi.string(),
      street_type: Joi.string(),
      street_direction: Joi.string(),
      unit_type: Joi.string(),
      unit_number: Joi.string()
    })
    .custom((value, helpers) => {
      // At least one address component must be provided
      const hasFullAddress = !!value.address;
      const hasAddressLine = !!value.address_line_1;
      const hasComponents = !!(value.house_number || value.street_name || value.city);
      
      if (!hasFullAddress && !hasAddressLine && !hasComponents) {
        return helpers.error('custom.noAddress');
      }
      
      return value;
    })
    .messages({
      'custom.noAddress': 'At least one address field must be provided'
    })
  },
  
  // Batch validation schema
  batch: {
    body: Joi.object({
      items: Joi.array()
        .items(Joi.any())
        .min(1)
        .max(100)
        .required()
        .messages({
          'array.min': 'At least one item is required',
          'array.max': 'Maximum 100 items allowed per batch',
          'any.required': 'Items array is required'
        }),
      type: Joi.string()
        .valid('email', 'name', 'phone', 'address')
        .required()
        .messages({
          'any.only': 'Type must be one of: email, name, phone, address',
          'any.required': 'Type is required'
        })
    })
  },
  
  // Pagination schema for list endpoints
  pagination: {
    query: Joi.object({
      page: Joi.number().integer().min(1).default(1),
      limit: Joi.number().integer().min(1).max(100).default(20)
    })
  },
  
  // ID parameter schema
  id: {
    params: Joi.object({
      id: Joi.alternatives().try(
        Joi.number().integer().positive(),
        Joi.string().uuid()
      ).required()
        .messages({
          'any.required': 'ID parameter is required'
        })
    })
  },
  
  // Custom validation schema
  custom: (schemaDefinition) => schemaDefinition
};

/**
 * Convenience middleware creators
 */
export const validate = {
  email: () => validateRequest(schemas.email),
  name: () => validateRequest(schemas.name),
  phone: () => validateRequest(schemas.phone),
  address: () => validateRequest(schemas.address),
  batch: () => validateRequest(schemas.batch),
  pagination: () => validateRequest(schemas.pagination),
  id: () => validateRequest(schemas.id),
  custom: (schema) => validateRequest(schema)
};

// Export both named and default
export default validate;