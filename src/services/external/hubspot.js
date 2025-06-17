// src/services/external/hubspot.js
import { createServiceLogger } from '../../core/logger.js';
import { 
  HubSpotError, 
  ValidationError, 
  ErrorRecovery,
  CircuitBreaker
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
    
    // Initialize circuit breaker for HubSpot API calls
    this.circuitBreaker = new CircuitBreaker({
      name: 'HubSpot',
      failureThreshold: 5,
      resetTimeout: 60000,
      monitoringPeriod: 10000
    });
    
    // Cache for client HubSpot configurations
    this.clientConfigCache = new Map();
    
    // Default timeout settings
    this.timeouts = {
      contacts: 5000,
      form: 6000,
      retry: 8000
    };
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
      'email', 'firstname', 'lastname', 'phone',
      'um_email', 'um_first_name', 'um_last_name',
      'um_email_status', 'um_bounce_status', 'um_name_status',
      'um_phone', 'um_phone_status', 'um_phone_is_mobile',
      'um_house_number', 'um_street_name', 'um_city',
      'um_state_province', 'um_postal_code', 'um_country',
      'um_address_status'
    ];
    
    // Join properties for URL
    const propertiesParam = propertiesToFetch.join(',');
    
    return this.circuitBreaker.execute(async () => {
      try {
        this.logger.debug('Fetching contact from HubSpot', { contactId });
        
        const response = await ErrorRecovery.withTimeout(
          fetch(
            `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}?properties=${propertiesParam}`,
            {
              headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
              }
            }
          ),
          timeout,
          'HubSpot contact fetch'
        );
        
        if (!response.ok) {
          const errorText = await response.text();
          throw new HubSpotError(
            `API error (${response.status}): ${errorText}`,
            response.status
          );
        }
        
        return response.json();
      } catch (error) {
        if (error instanceof HubSpotError) {
          throw error;
        }
        
        this.logger.error('Error fetching contact', error, { contactId });
        throw new HubSpotError(
          `Failed to fetch contact: ${error.message}`,
          error.statusCode || 500,
          error
        );
      }
    });
  }
  
  /**
   * Submit form data to HubSpot to update a contact
   * 
   * @param {Object} formData - Form data to submit
   * @param {Object} hubspotConfig - HubSpot configuration
   * @returns {Promise<Object>} Submission result
   */
  async submitForm(formData, hubspotConfig) {
    if (!hubspotConfig || !hubspotConfig.apiKey || !hubspotConfig.portalId || !hubspotConfig.formGuid) {
      throw new ValidationError('Invalid HubSpot configuration');
    }
    
    return this.circuitBreaker.execute(async () => {
      try {
        this.logger.debug('Submitting form to HubSpot', {
          portalId: hubspotConfig.portalId,
          formGuid: hubspotConfig.formGuid
        });
        
        const url = `https://api.hsforms.com/submissions/v3/integration/submit/${hubspotConfig.portalId}/${hubspotConfig.formGuid}`;
        
        const payload = {
          fields: this.formatFormFields(formData),
          context: {
            hutk: formData.hutk || '',
            pageUri: 'https://api.unmessy.com',
            pageName: 'Unmessy API Form Submission'
          }
        };
        
        const response = await ErrorRecovery.withTimeout(
          fetch(url, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
          }),
          this.timeouts.form,
          'HubSpot form submission'
        );
        
        if (!response.ok) {
          const errorText = await response.text();
          throw new HubSpotError(
            `Form submission error (${response.status}): ${errorText}`,
            response.status
          );
        }
        
        return response.json();
      } catch (error) {
        if (error instanceof HubSpotError) {
          throw error;
        }
        
        this.logger.error('Error submitting form', error);
        throw new HubSpotError(
          `Failed to submit form: ${error.message}`,
          error.statusCode || 500,
          error
        );
      }
    });
  }
  
  /**
   * Format data for HubSpot form submission
   * 
   * @param {Object} data - Data to format
   * @returns {Array} Formatted fields
   */
  formatFormFields(data) {
    const fields = [];
    
    // Process each field
    for (const [key, value] of Object.entries(data)) {
      // Skip null/undefined values and special fields
      if (value === null || value === undefined || key === 'hutk') {
        continue;
      }
      
      fields.push({
        name: key,
        value: String(value)
      });
    }
    
    return fields;
  }
  
  /**
   * Verify HubSpot webhook signature
   * 
   * @param {string} body - Request body
   * @param {string} signature - Signature from headers
   * @param {string} secret - Webhook secret
   * @param {string} version - Signature version
   * @returns {boolean} Whether signature is valid
   */
  verifySignature(body, signature, secret, version = 'v1') {
    try {
      if (!signature || !secret) {
        return false;
      }
      
      let computedSignature;
      
      if (version === 'v1') {
        const hmac = crypto.createHmac('sha256', secret);
        hmac.update(body);
        computedSignature = hmac.digest('hex');
      } else {
        this.logger.error('Unsupported signature version', { version });
        return false;
      }
      
      const expectedSignature = signature.startsWith('sha256=') ? 
        signature.substring(7) : signature;
      
      return crypto.timingSafeEqual(
        Buffer.from(computedSignature, 'hex'),
        Buffer.from(expectedSignature, 'hex')
      );
    } catch (error) {
      this.logger.error('Signature verification error', error);
      return false;
    }
  }
  
  /**
   * Get HubSpot configuration for a client
   * 
   * @param {string|number} clientId - Client ID
   * @param {Object} db - Database connection
   * @returns {Promise<Object>} HubSpot configuration
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
            .eq('client_id', parseInt(clientId, 10))
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
    if (!['contact.propertyChange', 'contact.creation'].includes(event.subscriptionType)) {
      this.logger.debug('Skipping unsupported event type', {
        eventId: event.eventId,
        type: event.subscriptionType
      });
      return null;
    }
    
    // Determine client from portal ID or use default
    let clientId = config.clients.defaultClientId;
    let client = null;
    
    if (event.portalId) {
      client = await this.findClientByPortalId(event.portalId, db);
      if (client) {
        clientId = client.client_id.toString();
      }
    }
    
    // Get client HubSpot configuration
    const hubspotConfig = await this.getClientHubSpotConfig(clientId, db);
    
    if (!hubspotConfig || !hubspotConfig.enabled) {
      this.logger.debug('HubSpot not enabled for client', {
        clientId,
        eventId: event.eventId
      });
      return null;
    }
    
    // Fetch contact data using client-specific API key
    const contact = await this.fetchContact(event.objectId, hubspotConfig.apiKey);
    
    if (!contact) {
      this.logger.error('Failed to fetch contact', {
        eventId: event.eventId,
        contactId: event.objectId,
        clientId
      });
      return null;
    }
    
    // Create enriched event
    const enrichedEvent = {
      event_id: event.eventId,
      subscription_type: event.subscriptionType,
      object_id: event.objectId,
      portal_id: event.portalId || null,
      property_name: event.propertyName || null,
      property_value: event.propertyValue ? String(event.propertyValue).substring(0, 1000) : null,
      contact_email: contact.properties.email || null,
      contact_firstname: contact.properties.firstname || null,
      contact_lastname: contact.properties.lastname || null,
      contact_phone: contact.properties.phone || null,
      occurred_at: event.occurredAt ? new Date(event.occurredAt).toISOString() : new Date().toISOString(),
      event_data: event,
      contact_data: contact,
      status: 'pending',
      client_id: clientId,
      attempts: 0,
      created_at: new Date().toISOString(),
      
      // Determine which validations are needed
      needs_email_validation: this.needsEmailValidation(event, contact),
      needs_name_validation: this.needsNameValidation(event, contact),
      needs_phone_validation: this.needsPhoneValidation(event, contact),
      needs_address_validation: this.needsAddressValidation(event, contact)
    };
    
    return enrichedEvent;
  }
  
  /**
   * Determine if email validation is needed
   */
  needsEmailValidation(event, contact) {
    // Always validate on creation
    if (event.subscriptionType === 'contact.creation') {
      return contact.properties.email ? true : false;
    }
    
    // Validate on email property change
    if (event.propertyName === 'email') {
      return true;
    }
    
    return false;
  }
  
  /**
   * Determine if name validation is needed
   */
  needsNameValidation(event, contact) {
    // Always validate on creation
    if (event.subscriptionType === 'contact.creation') {
      return contact.properties.firstname || contact.properties.lastname ? true : false;
    }
    
    // Validate on name property changes
    if (event.propertyName === 'firstname' || event.propertyName === 'lastname') {
      return true;
    }
    
    return false;
  }
  
  /**
   * Determine if phone validation is needed
   */
  needsPhoneValidation(event, contact) {
    // Always validate on creation
    if (event.subscriptionType === 'contact.creation') {
      return contact.properties.phone ? true : false;
    }
    
    // Validate on phone property change
    if (event.propertyName === 'phone') {
      return true;
    }
    
    return false;
  }
  
  /**
   * Determine if address validation is needed
   */
  needsAddressValidation(event, contact) {
    // Check for address-related properties
    const addressProperties = [
      'address', 'city', 'state', 'zip', 'country',
      'address2', 'postal_code', 'state_province'
    ];
    
    // Always validate on creation if address properties exist
    if (event.subscriptionType === 'contact.creation') {
      return addressProperties.some(prop => contact.properties[prop]);
    }
    
    // Validate on address property changes
    if (addressProperties.includes(event.propertyName)) {
      return true;
    }
    
    return false;
  }
  
  /**
   * Submit validation results back to HubSpot
   * 
   * @param {Object} contactData - Contact data
   * @param {Object} validationResults - Validation results
   * @param {Object} hubspotConfig - HubSpot configuration
   * @returns {Promise<Object>} Submission result
   */
  async submitValidationResults(contactData, validationResults, hubspotConfig) {
    // Prepare form data with contact email
    const formData = {
      email: contactData.properties.email,
      date_last_um_check_epoch: validationResults.date_last_um_check_epoch || Date.now(),
      um_check_id: validationResults.um_check_id || this.generateUmCheckId()
    };
    
    // Add email validation results
    if (validationResults.email) {
      formData.um_email = validationResults.email.um_email;
      formData.um_email_status = validationResults.email.um_email_status;
      formData.um_bounce_status = validationResults.email.um_bounce_status;
    }
    
    // Add name validation results
    if (validationResults.name) {
      formData.um_first_name = validationResults.name.um_first_name;
      formData.um_last_name = validationResults.name.um_last_name;
      formData.um_name = validationResults.name.um_name;
      formData.um_name_status = validationResults.name.um_name_status;
      formData.um_name_format = validationResults.name.um_name_format;
    }
    
    // Add phone validation results
    if (validationResults.phone) {
      formData.um_phone = validationResults.phone.um_phone;
      formData.um_phone_status = validationResults.phone.um_phone_status;
      formData.um_phone_format = validationResults.phone.um_phone_format;
      formData.um_phone_country_code = validationResults.phone.um_phone_country_code;
      formData.um_phone_is_mobile = validationResults.phone.um_phone_is_mobile;
    }
    
    // Add address validation results
    if (validationResults.address) {
      formData.um_house_number = validationResults.address.um_house_number;
      formData.um_street_name = validationResults.address.um_street_name;
      formData.um_street_type = validationResults.address.um_street_type;
      formData.um_street_direction = validationResults.address.um_street_direction;
      formData.um_unit_type = validationResults.address.um_unit_type;
      formData.um_unit_number = validationResults.address.um_unit_number;
      formData.um_address_line_1 = validationResults.address.um_address_line_1;
      formData.um_address_line_2 = validationResults.address.um_address_line_2;
      formData.um_city = validationResults.address.um_city;
      formData.um_state_province = validationResults.address.um_state_province;
      formData.um_country = validationResults.address.um_country;
      formData.um_country_code = validationResults.address.um_country_code;
      formData.um_postal_code = validationResults.address.um_postal_code;
      formData.um_address_status = validationResults.address.um_address_status;
    }
    
    // Submit to HubSpot
    return this.submitForm(formData, hubspotConfig);
  }
  
  /**
   * Generate um_check_id for tracking
   */
  generateUmCheckId(clientId = '0001') {
    const epochTime = Date.now();
    const lastSixDigits = String(epochTime).slice(-6);
    const clientIdStr = clientId;
    const firstThreeDigits = String(epochTime).slice(0, 3);
    const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
    const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
    const unmessyVersion = config.unmessy.version;
    return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${unmessyVersion}`);
  }
  
  /**
   * Queue webhook event for processing
   * 
   * @param {Object} enrichedEvent - Enriched webhook event
   * @param {Object} db - Database connection
   * @returns {Promise<Object>} Queue result
   */
  async queueEvent(enrichedEvent, db) {
    try {
      const { data, error } = await db.executeWithRetry(async (supabase) => {
        const { data, error } = await supabase
          .from('hubspot_webhook_queue')
          .upsert(enrichedEvent, { 
            onConflict: 'event_id',
            ignoreDuplicates: false 
          });
          
        if (error) throw error;
        
        return { data, error: null };
      });
      
      if (error) {
        throw error;
      }
      
      this.logger.info('Event queued successfully', {
        eventId: enrichedEvent.event_id,
        clientId: enrichedEvent.client_id
      });
      
      return { success: true, data };
    } catch (error) {
      this.logger.error('Failed to queue event', error, {
        eventId: enrichedEvent.event_id
      });
      
      throw new HubSpotError(
        `Failed to queue event: ${error.message}`,
        500,
        error
      );
    }
  }
  
  /**
   * Process queued webhook events
   * 
   * @param {Object} validationService - Validation service
   * @param {Object} db - Database connection
   * @param {number} batchSize - Number of events to process
   * @returns {Promise<Object>} Processing results
   */
  async processQueuedEvents(validationService, db, batchSize = 10) {
    this.logger.info('Processing queued events', { batchSize });
    
    try {
      // Fetch pending events
      const { data: events, error } = await db.executeWithRetry(async (supabase) => {
        const { data, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('*')
          .eq('status', 'pending')
          .lt('attempts', 3)
          .order('created_at', { ascending: true })
          .limit(batchSize);
          
        if (error) throw error;
        
        return { data, error: null };
      });
      
      if (error) {
        throw error;
      }
      
      if (!events || events.length === 0) {
        this.logger.info('No pending events found');
        return { processed: 0, failed: 0, remaining: 0 };
      }
      
      // Process each event
      const results = {
        processed: 0,
        failed: 0,
        errors: []
      };
      
      for (const event of events) {
        try {
          // Mark as processing
          await db.executeWithRetry(async (supabase) => {
            await supabase
              .from('hubspot_webhook_queue')
              .update({
                status: 'processing',
                processing_started_at: new Date().toISOString(),
                attempts: event.attempts + 1
              })
              .eq('event_id', event.event_id);
          });
          
          // Get HubSpot config
          const hubspotConfig = await this.getClientHubSpotConfig(event.client_id, db);
          
          if (!hubspotConfig || !hubspotConfig.enabled) {
            throw new ValidationError('HubSpot not enabled for client');
          }
          
          // Process validations
          const validationResults = {};
          
          // Email validation
          if (event.needs_email_validation && event.contact_data.properties.email) {
            validationResults.email = await validationService.validateEmail(
              event.contact_data.properties.email,
              { clientId: event.client_id }
            );
          }
          
          // Name validation
          if (event.needs_name_validation) {
            if (event.contact_data.properties.firstname || event.contact_data.properties.lastname) {
              validationResults.name = await validationService.validateSeparateNames(
                event.contact_data.properties.firstname,
                event.contact_data.properties.lastname,
                { clientId: event.client_id }
              );
            }
          }
          
          // Phone validation
          if (event.needs_phone_validation && event.contact_data.properties.phone) {
            validationResults.phone = await validationService.validatePhone(
              event.contact_data.properties.phone,
              { clientId: event.client_id }
            );
          }
          
          // Address validation
          if (event.needs_address_validation) {
            // Extract address fields from contact properties
            const addressInput = {
              address_line_1: event.contact_data.properties.address,
              address_line_2: event.contact_data.properties.address2,
              city: event.contact_data.properties.city,
              state_province: event.contact_data.properties.state,
              postal_code: event.contact_data.properties.zip || event.contact_data.properties.postal_code,
              country: event.contact_data.properties.country
            };
            
            if (Object.values(addressInput).some(v => v)) {
              validationResults.address = await validationService.validateAddress(
                addressInput,
                { clientId: event.client_id }
              );
            }
          }
          
          // Submit validation results to HubSpot
          const submission = await this.submitValidationResults(
            event.contact_data,
            validationResults,
            hubspotConfig
          );
          
          // Mark as completed
          await db.executeWithRetry(async (supabase) => {
            await supabase
              .from('hubspot_webhook_queue')
              .update({
                status: 'completed',
                processing_completed_at: new Date().toISOString(),
                validation_results: validationResults,
                form_submission_response: submission
              })
              .eq('event_id', event.event_id);
          });
          
          results.processed++;
          
          this.logger.info('Event processed successfully', {
            eventId: event.event_id,
            clientId: event.client_id
          });
        } catch (error) {
          results.failed++;
          results.errors.push({
            eventId: event.event_id,
            error: error.message
          });
          
          // Mark as failed or retry
          const isFinalAttempt = event.attempts >= 2; // 0-indexed, so 2 = 3rd attempt
          const status = isFinalAttempt ? 'failed' : 'pending';
          
          await db.executeWithRetry(async (supabase) => {
            await supabase
              .from('hubspot_webhook_queue')
              .update({
                status,
                error_message: error.message,
                error_details: {
                  message: error.message,
                  stack: error.stack,
                  name: error.name
                },
                next_retry_at: isFinalAttempt ? null : new Date(Date.now() + 300000).toISOString() // 5 min retry
              })
              .eq('event_id', event.event_id);
          });
          
          this.logger.error('Failed to process event', error, {
            eventId: event.event_id,
            clientId: event.client_id,
            attempt: event.attempts + 1,
            isFinalAttempt
          });
        }
      }
      
      // Get remaining events count
      const { count } = await db.executeWithRetry(async (supabase) => {
        const { count, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('*', { count: 'exact', head: true })
          .eq('status', 'pending')
          .lt('attempts', 3);
          
        if (error) throw error;
        
        return { count };
      });
      
      results.remaining = count || 0;
      
      return results;
    } catch (error) {
      this.logger.error('Error processing queue', error);
      throw error;
    }
  }
}

// Create singleton instance
const hubSpotService = new HubSpotService();

// Export service
export { hubSpotService, HubSpotService };