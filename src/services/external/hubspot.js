// src/services/external/hubspot.js
import CircuitBreaker from 'opossum';
import { createServiceLogger } from '../../core/logger.js';
import { 
  HubSpotError, 
  ValidationError, 
  ErrorRecovery,
  TimeoutError
} from '../../core/errors.js';
import { config } from '../../core/config.js';
import crypto from 'crypto';

// Create logger instance
const logger = createServiceLogger('hubspot-service');

/**
 * HubSpot service handles all interactions with HubSpot APIs
 * and webhook processing
 */
class HubSpotService {
  constructor() {
    this.logger = logger;
    
    // Initialize circuit breaker with Opossum
    this.circuitBreaker = new CircuitBreaker(this.executeRequest.bind(this), {
      name: 'HubSpot',
      timeout: 10000, // Time in ms before a request is considered failed
      errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
      resetTimeout: 60000, // Wait time before trying to close the circuit
      volumeThreshold: 5, // Minimum number of requests needed before tripping circuit
      rollingCountTimeout: 10000, // Time window for error rate calculation
      rollingCountBuckets: 10 // Number of buckets for stats tracking
    });
    
    // Add event listeners
    this.setupCircuitBreakerEvents();
    
    // Cache for client HubSpot configurations
    this.clientConfigCache = new Map();
    
    // Default timeout settings
    this.timeouts = {
      contacts: 5000,
      form: 6000,
      retry: 8000
    };
  }
  
  // Setup circuit breaker event handlers
  setupCircuitBreakerEvents() {
    this.circuitBreaker.on('open', () => {
      this.logger.warn('HubSpot circuit breaker opened');
    });
    
    this.circuitBreaker.on('halfOpen', () => {
      this.logger.info('HubSpot circuit breaker half-open, testing service');
    });
    
    this.circuitBreaker.on('close', () => {
      this.logger.info('HubSpot circuit breaker closed, service recovered');
    });
    
    this.circuitBreaker.on('fallback', (result) => {
      this.logger.warn('HubSpot circuit breaker fallback executed');
    });
    
    this.circuitBreaker.on('timeout', () => {
      this.logger.warn('HubSpot request timed out');
    });
    
    this.circuitBreaker.on('reject', () => {
      this.logger.warn('HubSpot request rejected (circuit open)');
    });
  }
  
  // This is the function that will be wrapped by the circuit breaker
  async executeRequest(requestData) {
    const { 
      url, 
      options, 
      operation,
      timeout
    } = requestData;
    
    try {
      return await ErrorRecovery.withRetry(
        async (attempt) => {
          this.logger.debug(`Calling HubSpot API: ${operation}`, { attempt });
          
          // Execute API call with timeout
          const response = await ErrorRecovery.withTimeout(
            fetch(url, options),
            timeout,
            `HubSpot ${operation}`
          );
          
          if (!response.ok) {
            let errorData;
            try {
              errorData = await response.json();
            } catch (e) {
              errorData = { message: await response.text() || response.statusText };
            }
            
            const errorMessage = errorData.message || `${response.status} ${response.statusText}`;
            
            throw new HubSpotError(
              `API error: ${errorMessage}`,
              response.status
            );
          }
          
          return await response.json();
        },
        3, // Max retries
        500, // Initial delay
        (error) => {
          // Only retry on network errors or 5xx errors
          return (
            error.message?.includes('network') ||
            error.statusCode >= 500 ||
            error instanceof TimeoutError
          );
        }
      );
    } catch (error) {
      this.logger.error(`HubSpot API call failed: ${operation}`, error);
      throw error;
    }
  }
  
  /**
   * Fetch a contact from HubSpot by ID
   * 
   * @param {string} contactId - HubSpot contact ID
   * @param {string} apiKey - HubSpot API key
   * @param {Object} options - Additional options
   * @returns {Promise<Object>} Contact data
   */
  async fetchContact(contactId, apiKey, options = {}) {
    if (!apiKey) {
      throw new HubSpotError('No HubSpot API key provided', 400);
    }
    
    if (!contactId) {
      throw new ValidationError('Contact ID is required');
    }
    
    const { timeout = this.timeouts.contacts, properties = [] } = options;
    
    // Add default properties if none provided
    let propertiesToFetch = properties.length > 0 ? properties : [
      'email',
      'firstname',
      'lastname',
      'phone',
      'address',
      'city',
      'state',
      'zip',
      'country'
    ];
    
    // Build URL
    const url = `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}?properties=${propertiesToFetch.join(',')}`;
    
    // Configure request options
    const requestOptions = {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Accept': 'application/json'
      }
    };
    
    // Execute with circuit breaker
    try {
      const requestData = {
        url,
        options: requestOptions,
        operation: `fetch contact ${contactId}`,
        timeout
      };
      
      const data = await this.circuitBreaker.fire(requestData);
      
      return {
        id: data.id,
        properties: data.properties,
        createdAt: data.createdAt,
        updatedAt: data.updatedAt
      };
    } catch (error) {
      if (error.name === 'CircuitBreaker:OpenError') {
        throw new HubSpotError('HubSpot service is currently unavailable (circuit open)', 503);
      }
      
      if (error.name === 'CircuitBreaker:TimeoutError') {
        throw new TimeoutError('HubSpot', timeout);
      }
      
      if (error instanceof HubSpotError || error instanceof TimeoutError) {
        throw error;
      }
      
      throw new HubSpotError(`Failed to fetch contact: ${error.message}`);
    }
  }

  /**
   * Get contact from HubSpot (alias for fetchContact for backward compatibility)
   * 
   * @param {string} contactId - HubSpot contact ID
   * @param {string} apiKey - HubSpot API key
   * @param {Object} options - Additional options
   * @returns {Promise<Object>} Contact data
   */
  async getContact(contactId, apiKey, options = {}) {
    return this.fetchContact(contactId, apiKey, options);
  }
  
  /**
   * Submit form data to HubSpot with improved flexibility
   * 
   * @param {Object} formData - Form data to submit
   * @param {string} portalId - HubSpot portal ID
   * @param {string} formGuid - HubSpot form GUID
   * @param {Object} options - Additional options including objectId for contact association
   * @returns {Promise<Object>} Submission response
   */
  async submitForm(formData, portalId, formGuid, options = {}) {
    if (!portalId || !formGuid) {
      throw new ValidationError('Portal ID and Form GUID are required');
    }
    
    // No longer require email - form data can be empty or contain any fields
    if (!formData || typeof formData !== 'object') {
      this.logger.warn('Form data is empty or invalid, proceeding with empty fields');
      formData = {};
    }
    
    const { 
      timeout = this.timeouts.form,
      objectId = null, // Contact ID for association
      skipValidation = false, // Skip field validation
      legalConsentOptions = null, // GDPR consent options
      hutk = null // HubSpot tracking cookie
    } = options;
    
    // Build URL
    const url = `https://api.hsforms.com/submissions/v3/integration/submit/${portalId}/${formGuid}`;
    
    // Check if formData already has the fields array structure
    let fields = [];
    
    if (formData.fields && Array.isArray(formData.fields)) {
      // Already in correct format
      fields = formData.fields;
    } else {
      // Convert flat object to fields array
      Object.entries(formData).forEach(([name, value]) => {
        try {
          // Skip null, undefined, or function values
          if (value === null || value === undefined || typeof value === 'function') {
            this.logger.debug(`Skipping field ${name} with invalid value type`);
            return;
          }
          
          // Convert arrays and objects to strings
          let fieldValue = value;
          if (Array.isArray(value)) {
            fieldValue = value.join(', ');
          } else if (typeof value === 'object') {
            fieldValue = JSON.stringify(value);
          } else {
            fieldValue = String(value);
          }
          
          // Fix epoch timestamp if it's the date_last_um_check_epoch field
          if (name === 'date_last_um_check_epoch') {
            // Ensure it's a Unix timestamp in seconds, not milliseconds
            const timestamp = parseInt(fieldValue);
            if (timestamp > 9999999999) { // If it's in milliseconds
              fieldValue = Math.floor(timestamp / 1000).toString();
            }
          }
          
          fields.push({
            name,
            value: fieldValue
          });
        } catch (error) {
          this.logger.warn(`Error processing field ${name}, skipping`, { error: error.message });
        }
      });
    }
    
    // Build the payload
    const payload = {
      fields,
      context: {
        pageUri: options.pageUri || 'https://unmessy.api/form',
        pageName: options.pageName || 'Unmessy API Form Submission'
      }
    };
    
    // Add HubSpot tracking cookie if provided
    if (hutk) {
      payload.context.hutk = hutk;
    }
    
    // Add IP address if provided
    if (options.ipAddress) {
      payload.context.ipAddress = options.ipAddress;
    }
    
    // Add legal consent options if provided
    if (legalConsentOptions) {
      payload.legalConsentOptions = legalConsentOptions;
    }
    
    // Add contact association if objectId is provided
    if (objectId) {
      // For v3 API, we can associate the submission with an existing contact
      payload.context.objectId = objectId;
      
      // Log the association attempt
      this.logger.info('Associating form submission with contact', {
        formGuid,
        objectId,
        fieldCount: fields.length
      });
    }
    
    // Configure request options
    const requestOptions = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify(payload)
    };
    
    // Log the payload for debugging
    this.logger.debug('Submitting form data to HubSpot', {
      portalId,
      formGuid,
      fieldCount: fields.length,
      fieldNames: fields.map(f => f.name),
      hasObjectId: !!objectId
    });
    
    // Execute with circuit breaker
    try {
      const requestData = {
        url,
        options: requestOptions,
        operation: `submit form ${formGuid}`,
        timeout
      };
      
      const data = await this.circuitBreaker.fire(requestData);
      
      // Log successful submission
      this.logger.info('Form submitted successfully', {
        formGuid,
        portalId,
        objectId: objectId || 'none',
        fieldCount: fields.length
      });
      
      return {
        success: true,
        inlineMessage: data.inlineMessage,
        redirectUrl: data.redirectUri,
        portalId,
        formGuid,
        objectId: objectId || null,
        submittedFields: fields.map(f => f.name)
      };
    } catch (error) {
      // Enhanced error handling for field mismatches
      if (error instanceof HubSpotError && error.statusCode === 400) {
        // Try to parse the error for field-specific issues
        const errorMessage = error.message.toLowerCase();
        
        this.logger.error('HubSpot form submission failed with 400 error', {
          formGuid,
          error: error.message,
          submittedFields: fields.map(f => ({ name: f.name, value: f.value })),
          payload: JSON.stringify(payload)
        });
        
        if (errorMessage.includes('field') || errorMessage.includes('property')) {
          this.logger.warn('Form field mismatch detected', {
            formGuid,
            error: error.message,
            submittedFields: fields.map(f => f.name)
          });
          
          // If skipValidation is true, return a partial success
          if (skipValidation) {
            return {
              success: false,
              partial: true,
              error: 'Field validation failed but skipValidation was set',
              originalError: error.message,
              portalId,
              formGuid,
              objectId: objectId || null,
              submittedFields: fields.map(f => f.name)
            };
          }
        }
      }
      
      if (error.name === 'CircuitBreaker:OpenError') {
        throw new HubSpotError('HubSpot service is currently unavailable (circuit open)', 503);
      }
      
      if (error.name === 'CircuitBreaker:TimeoutError') {
        throw new TimeoutError('HubSpot', timeout);
      }
      
      if (error instanceof HubSpotError || error instanceof TimeoutError) {
        throw error;
      }
      
      throw new HubSpotError(`Form submission failed: ${error.message}`);
    }
  }
  
  /**
   * Get form fields from HubSpot to validate against
   * 
   * @param {string} formGuid - HubSpot form GUID
   * @param {string} apiKey - HubSpot API key
   * @returns {Promise<Array>} Form fields
   */
  async getFormFields(formGuid, apiKey) {
    if (!apiKey) {
      throw new HubSpotError('No HubSpot API key provided', 400);
    }
    
    if (!formGuid) {
      throw new ValidationError('Form GUID is required');
    }
    
    // Build URL - using v3 API
    const url = `https://api.hubapi.com/marketing/v3/forms/${formGuid}`;
    
    // Configure request options
    const requestOptions = {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Accept': 'application/json'
      }
    };
    
    try {
      const requestData = {
        url,
        options: requestOptions,
        operation: `get form fields ${formGuid}`,
        timeout: this.timeouts.contacts
      };
      
      const data = await this.circuitBreaker.fire(requestData);
      
      // Extract field information
      const fields = data.formFieldGroups?.flatMap(group => 
        group.fields?.map(field => ({
          name: field.name,
          label: field.label,
          type: field.fieldType,
          required: field.required,
          options: field.options || []
        }))
      ) || [];
      
      return fields;
    } catch (error) {
      this.logger.error('Failed to get form fields', { formGuid, error: error.message });
      
      // Return empty array on error to allow form submission to proceed
      return [];
    }
  }
  
  /**
   * Verify webhook signature with version support
   * 
   * @param {string} requestBody - Raw request body
   * @param {string} signature - HubSpot signature
   * @param {string} clientSecret - Client's webhook secret
   * @param {string} version - Signature version (v1, v2, v3)
   * @returns {boolean} Whether signature is valid
   */
  verifyWebhookSignature(requestBody, signature, clientSecret, version = 'v1') {
    if (!requestBody || !signature || !clientSecret) {
      this.logger.warn('Missing required parameters for signature verification');
      return false;
    }
    
    try {
      let computedSignature;
      
      // Handle different signature versions
      switch (version) {
        case 'v1':
          // V1: SHA256 hash of client secret + request body
          computedSignature = crypto
            .createHash('sha256')
            .update(clientSecret + requestBody)
            .digest('hex');
          break;
          
        case 'v2':
          // V2: HMAC-SHA256 with client secret as key
          computedSignature = crypto
            .createHmac('sha256', clientSecret)
            .update(requestBody)
            .digest('hex');
          break;
          
        case 'v3':
          // V3: HMAC-SHA256 with UTF-8 encoding explicitly
          computedSignature = crypto
            .createHmac('sha256', clientSecret)
            .update(requestBody, 'utf8')
            .digest('hex');
          break;
          
        default:
          // Default to v2 for unknown versions
          computedSignature = crypto
            .createHmac('sha256', clientSecret)
            .update(requestBody)
            .digest('hex');
      }
      
      // For v1, HubSpot sends the signature directly
      // For v2/v3, it might include a prefix like "sha256="
      let hubspotSignature = signature;
      if (signature.includes('=')) {
        hubspotSignature = signature.split('=')[1];
      }
      
      // Compare signatures (both should be hex strings now)
      // Using string comparison instead of timingSafeEqual to avoid length mismatch issues
      return computedSignature === hubspotSignature;
      
    } catch (error) {
      this.logger.error('Webhook signature verification failed', error);
      return false;
    }
  }
  
  /**
   * Get client HubSpot configuration
   * 
   * @param {string} clientId - Client ID
   * @param {Object} db - Database connection
   * @returns {Promise<Object>} HubSpot config
   */
  async getClientHubSpotConfig(clientId, db) {
    // Check cache first
    if (this.clientConfigCache.has(clientId)) {
      return this.clientConfigCache.get(clientId);
    }
    
    try {
      const { data, error } = await db.executeWithRetry(
        async (supabase) => {
          const { data, error } = await supabase
            .from('clients')
            .select('hubspot_private_key, hubspot_portal_id, hubspot_form_guid, hubspot_webhook_secret, hubspot_enabled')
            .eq('client_id', clientId)
            .single();
          
          if (error) throw error;
          
          return { data, error: null };
        }
      );
      
      if (error) {
        throw error;
      }
      
      const config = {
        apiKey: data.hubspot_private_key,
        portalId: data.hubspot_portal_id,
        formGuid: data.hubspot_form_guid,
        webhookSecret: data.hubspot_webhook_secret,
        enabled: data.hubspot_enabled
      };
      
      // Cache the configuration
      this.clientConfigCache.set(clientId, config);
      
      return config;
    } catch (error) {
      this.logger.error('Failed to get client HubSpot config', error, { clientId });
      return null;
    }
  }
  
  /**
   * Find client by HubSpot portal ID
   * 
   * @param {string} portalId - HubSpot portal ID
   * @param {Object} db - Database connection
   * @returns {Promise<Object>} Client data
   */
  async findClientByPortalId(portalId, db) {
    try {
      const { data, error } = await db.executeWithRetry(
        async (supabase) => {
          const { data, error } = await supabase
            .from('clients')
            .select('client_id, name')
            .eq('hubspot_portal_id', portalId.toString())
            .eq('hubspot_enabled', true)
            .single();
          
          if (error) throw error;
          
          return { data, error: null };
        }
      );
      
      if (error) {
        this.logger.debug('No client found for portal ID', { portalId });
        return null;
      }
      
      return data;
    } catch (error) {
      this.logger.error('Error finding client by portal ID', error, { portalId });
      return null;
    }
  }
  
  /**
   * Process HubSpot webhook event
   * 
   * @param {Object} event - Webhook event data
   * @param {Object} db - Database connection
   * @returns {Promise<Object>} Enriched event
   */
  async processWebhookEvent(event, db) {
    // Validate event structure
    if (!event.eventId || !event.subscriptionType || !event.objectId) {
      throw new ValidationError('Invalid event structure');
    }
    
    this.logger.debug('Processing webhook event', {
      eventId: event.eventId,
      type: event.subscriptionType
    });
    
    // Only process creation and property changes
    if (!['contact.creation', 'contact.propertyChange'].includes(event.subscriptionType)) {
      this.logger.debug('Ignoring non-contact event', {
        eventId: event.eventId,
        type: event.subscriptionType
      });
      return null;
    }
    
    // Find the client for this portal
    const client = await this.findClientByPortalId(event.portalId, db);
    
    if (!client) {
      this.logger.warn('No client found for portal ID', {
        eventId: event.eventId,
        portalId: event.portalId
      });
      return null;
    }
    
    // Get client HubSpot config
    const clientConfig = await this.getClientHubSpotConfig(client.client_id, db);
    
    if (!clientConfig || !clientConfig.enabled || !clientConfig.apiKey) {
      this.logger.warn('Client HubSpot integration not enabled', {
        eventId: event.eventId,
        clientId: client.client_id
      });
      return null;
    }
    
    // Fetch contact details from HubSpot
    const contactDetails = await this.fetchContact(
      event.objectId,
      clientConfig.apiKey
    );
    
    // Add client info and contact details to event
    const enrichedEvent = {
      ...event,
      clientId: client.client_id,
      clientName: client.name,
      contactEmail: contactDetails.properties.email,
      contactFirstname: contactDetails.properties.firstname,
      contactLastname: contactDetails.properties.lastname,
      contactPhone: contactDetails.properties.phone,
      contactData: contactDetails
    };
    
    return enrichedEvent;
  }
  
  // Get circuit breaker state
  getCircuitBreakerState() {
    return {
      state: this.circuitBreaker.status,
      stats: {
        successes: this.circuitBreaker.stats.successes,
        failures: this.circuitBreaker.stats.failures,
        rejects: this.circuitBreaker.stats.rejects,
        timeouts: this.circuitBreaker.stats.timeouts
      }
    };
  }
  
  // Health check
  async healthCheck() {
    try {
      // Basic health check - no actual API call
      return {
        status: 'healthy',
        circuitBreaker: this.circuitBreaker.status,
        clientConfigCacheSize: this.clientConfigCache.size
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        circuitBreaker: this.circuitBreaker.status,
        error: error.message
      };
    }
  }
  
  /**
   * Clear client config cache
   * Useful when client settings are updated
   */
  clearConfigCache(clientId = null) {
    if (clientId) {
      this.clientConfigCache.delete(clientId);
      this.logger.info('Cleared HubSpot config cache for client', { clientId });
    } else {
      this.clientConfigCache.clear();
      this.logger.info('Cleared entire HubSpot config cache');
    }
  }
}

// Create singleton instance
const hubspotService = new HubSpotService();

// Export both the instance and the class
export { hubspotService, HubSpotService };
export default hubspotService;