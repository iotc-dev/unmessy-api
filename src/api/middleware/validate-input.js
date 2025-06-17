// src/api/middleware/validate-input.js
import Joi from 'joi';
import { createServiceLogger } from '../../core/logger.js';
import { ValidationError } from '../../core/errors.js';

// Create logger instance
const logger = createServiceLogger('validate-input');

/**
 * Creates a validation middleware using Joi schema
 * 
 * @param {Object} schema - Joi validation schema with request parts
 * @param {Object} options - Validation options
 * @returns {Function} Express middleware function
 */
function validateSchema(schema, options = {}) {
  const {
    abortEarly = false,
    stripUnknown = true,
    allowUnknown = true
  } = options;
  
  // Default Joi options
  const validationOptions = {
    abortEarly,
    stripUnknown,
    allowUnknown
  };
  
  return (req, res, next) => {
    // Parts of the request to validate
    const validationParts = ['params', 'query', 'body'];
    const validationErrors = [];
    
    // Validate each part if schema is provided
    for (const part of validationParts) {
      if (schema[part]) {
        const { error, value } = schema[part].validate(
          req[part],
          validationOptions
        );
        
        if (error) {
          // Format validation errors
          const errors = error.details.map(detail => ({
            path: detail.path.join('.'),
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
    }).or('name', ['first_name', 'last_name'])
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
      country: Joi.string().min(2).max(2)
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
      const hasComponents = !!(value.house_number || value.street_name);
      
      if (!hasFullAddress && !hasAddressLine && !hasComponents) {
        return helpers.error('object.missing');
      }
      
      return value;
    })
    .messages({
      'object.missing': 'At least one address component is required'
    })
  },
  
  // Pagination schema
  pagination: {
    query: Joi.object({
      page: Joi.number().integer().min(1).default(1),
      limit: Joi.number().integer().min(1).max(100).default(20)
    })
  },
  
  // ID parameter schema
  id: {
    params: Joi.object({
      id: Joi.string().required()
        .messages({
          'string.empty': 'ID is required',
          'any.required': 'ID is required'
        })
    })
  }
};

/**
 * Create validator middleware for specific validation types
 */
export const validate = {
  // Email validation middleware
  email: (customSchema) => validateSchema(customSchema || schemas.email),
  
  // Name validation middleware
  name: (customSchema) => validateSchema(customSchema || schemas.name),
  
  // Phone validation middleware
  phone: (customSchema) => validateSchema(customSchema || schemas.phone),
  
  // Address validation middleware
  address: (customSchema) => validateSchema(customSchema || schemas.address),
  
  // Custom schema validation middleware
  custom: (schema) => validateSchema(schema),
  
  // Pagination validation middleware
  pagination: (customSchema) => validateSchema(customSchema || schemas.pagination),
  
  // ID parameter validation middleware
  id: (customSchema) => validateSchema(customSchema || schemas.id)
};

// Export validation middleware factory and schemas
export { validateSchema };

// Export default
export default {
  validate,
  schemas,
  validateSchema
};