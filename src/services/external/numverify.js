// src/services/external/numverify.js
import axios from 'axios';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { ExternalServiceError } from '../../core/errors.js';

const logger = createServiceLogger('numverify');

class NumverifyService {
  constructor() {
    this.apiKey = process.env.NUMVERIFY_API_KEY || '';
    this.enabled = process.env.NUMVERIFY_ENABLED === 'true';
    this.baseUrl = 'http://apilayer.net/api';
    this.timeout = 10000; // 10 seconds
    this.logger = logger;
    
    // Log configuration on initialization
    this.logger.info('Numverify service initialized', {
      enabled: this.enabled,
      hasApiKey: !!this.apiKey,
      baseUrl: this.baseUrl
    });
  }
  
  /**
   * Check if Numverify is enabled and configured
   */
  isEnabled() {
    if (!this.enabled) {
      return false;
    }
    
    if (!this.apiKey) {
      this.logger.warn('Numverify is enabled but API key is missing');
      return false;
    }
    
    return true;
  }
  
  /**
   * Validate a phone number using Numverify API
   * @param {string} phone - Phone number to validate
   * @param {string} country - Optional country code
   * @returns {Promise<Object>} Validation result
   */
  async validatePhone(phone, country = null) {
    if (!this.isEnabled()) {
      this.logger.debug('Numverify validation skipped - service disabled');
      return null;
    }
    
    try {
      this.logger.info('Calling Numverify API', {
        phone: phone.substring(0, 6) + '***',
        country
      });
      
      const params = {
        access_key: this.apiKey,
        number: phone,
        format: 1 // JSON format
      };
      
      // Add country code if provided
      if (country) {
        params.country_code = country;
      }
      
      const response = await axios.get(`${this.baseUrl}/validate`, {
        params,
        timeout: this.timeout
      });
      
      const data = response.data;
      
      // Check for API errors
      if (!data.valid && data.error) {
        this.logger.error('Numverify API error', {
          code: data.error.code,
          type: data.error.type,
          info: data.error.info
        });
        
        // Handle specific error codes
        if (data.error.code === 101) {
          throw new ExternalServiceError('Invalid Numverify API key');
        } else if (data.error.code === 104) {
          throw new ExternalServiceError('Numverify monthly limit reached');
        }
        
        throw new ExternalServiceError(`Numverify error: ${data.error.info}`);
      }
      
      // Log successful response
      this.logger.info('Numverify validation successful', {
        valid: data.valid,
        country: data.country_code,
        carrier: data.carrier,
        lineType: data.line_type
      });
      
      // Transform response to our format
      return {
        valid: data.valid,
        number: data.number,
        localFormat: data.local_format,
        internationalFormat: data.international_format,
        countryPrefix: data.country_prefix,
        countryCode: data.country_code,
        countryName: data.country_name,
        location: data.location,
        carrier: data.carrier,
        lineType: data.line_type,
        
        // Additional Numverify fields
        isPortable: data.portable,
        isMobile: data.line_type === 'mobile',
        isFixedLine: data.line_type === 'fixed_line',
        isSpecialService: data.line_type === 'special_services',
        isPremiumRate: data.line_type === 'premium_rate',
        isTollFree: data.line_type === 'toll_free',
        isVoip: data.line_type === 'voip',
        
        // Metadata
        source: 'numverify',
        apiResponse: data
      };
      
    } catch (error) {
      // Handle different error types
      if (error.response) {
        // API returned an error response
        this.logger.error('Numverify API request failed', {
          status: error.response.status,
          statusText: error.response.statusText,
          data: error.response.data
        });
        
        if (error.response.status === 429) {
          throw new ExternalServiceError('Numverify rate limit exceeded');
        } else if (error.response.status === 401) {
          throw new ExternalServiceError('Numverify authentication failed');
        }
      } else if (error.request) {
        // Request was made but no response received
        this.logger.error('Numverify request timeout', { timeout: this.timeout });
        throw new ExternalServiceError('Numverify request timeout');
      }
      
      // Re-throw if it's already our error
      if (error instanceof ExternalServiceError) {
        throw error;
      }
      
      // Generic error
      this.logger.error('Numverify validation failed', error);
      throw new ExternalServiceError('Failed to validate phone with Numverify');
    }
  }
  
  /**
   * Get remaining API credits
   * @returns {Promise<number>} Number of remaining credits
   */
  async getCredits() {
    if (!this.isEnabled()) {
      return null;
    }
    
    try {
      const response = await axios.get(`${this.baseUrl}/validate`, {
        params: {
          access_key: this.apiKey,
          number: '14158586273' // Example number to check credits
        },
        timeout: this.timeout
      });
      
      // Numverify doesn't have a dedicated credits endpoint
      // We can check headers or response for rate limit info
      const remainingRequests = response.headers['x-ratelimit-remaining'];
      
      if (remainingRequests !== undefined) {
        return parseInt(remainingRequests, 10);
      }
      
      return null;
    } catch (error) {
      this.logger.error('Failed to get Numverify credits', error);
      return null;
    }
  }
}

// Create singleton instance
const numverifyService = new NumverifyService();

// Export service
export { numverifyService, NumverifyService };
export default numverifyService;