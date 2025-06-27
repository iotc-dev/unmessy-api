// src/services/queue-service.js
import db from '../core/db.js';
import { config } from '../core/config.js';
import { createServiceLogger } from '../core/logger.js';
import { 
  QueueError,
  TimeoutError,
  ErrorRecovery 
} from '../core/errors.js';
import validationService from './validation-service.js';
import clientService from './client-service.js';
import { hubspotService } from './external/hubspot.js';
import { triggerAlert, ALERT_TYPES } from '../monitoring/alerts.js';

const logger = createServiceLogger('queue-service');

class QueueService {
  constructor() {
    this.logger = logger;
    
    // Processing state
    this.isProcessing = false;
    this.processingStartTime = null;
    this.abortController = null;
    
    // Metrics
    this.metrics = {
      totalProcessed: 0,
      totalFailed: 0,
      averageProcessingTime: 0,
      lastProcessedAt: null
    };
    
    // Parallel processing configuration
    this.maxConcurrency = config.queue.maxConcurrency || 5;
  }
  
  // Enqueue multiple webhook events
  async enqueueWebhookEvents(events) {
    if (!Array.isArray(events) || events.length === 0) {
      return { success: true, count: 0 };
    }
    
    try {
      const results = [];
      
      for (const event of events) {
        try {
          const result = await this.enqueueWebhookEvent(event);
          results.push(result);
        } catch (error) {
          this.logger.error('Failed to enqueue event', error, {
            eventId: event.event_id
          });
        }
      }
      
      return {
        success: true,
        count: results.length,
        results
      };
    } catch (error) {
      this.logger.error('Failed to enqueue webhook events', error);
      throw new QueueError('Failed to enqueue events', error);
    }
  }
  
  // Enqueue a single webhook event
  async enqueueWebhookEvent(eventData) {
    try {
      const queueItem = {
        event_id: eventData.event_id,
        subscription_type: eventData.subscription_type,
        event_type: eventData.subscription_type,
        object_id: eventData.object_id,
        portal_id: eventData.portal_id,
        occurred_at: eventData.occurred_at,
        property_name: eventData.property_name,
        property_value: eventData.property_value,
        previous_value: eventData.previous_value,
        contact_email: eventData.contact_email,
        contact_firstname: eventData.contact_firstname,
        contact_lastname: eventData.contact_lastname,
        contact_phone: eventData.contact_phone,
        um_house_number: eventData.um_house_number,
        um_street_name: eventData.um_street_name,
        um_street_type: eventData.um_street_type,
        um_street_direction: eventData.um_street_direction,
        um_unit_type: eventData.um_unit_type,
        um_unit_number: eventData.um_unit_number,
        um_city: eventData.um_city,
        um_state_province: eventData.um_state_province,
        um_country: eventData.um_country,
        um_country_code: eventData.um_country_code,
        um_postal_code: eventData.um_postal_code,
        um_address_status: eventData.um_address_status,
        status: 'pending',
        client_id: eventData.client_id,
        attempts: 0,
        max_attempts: config.queue.maxRetries || 3,
        needs_email_validation: eventData.needs_email_validation || false,
        needs_name_validation: eventData.needs_name_validation || false,
        needs_phone_validation: eventData.needs_phone_validation || false,
        needs_address_validation: eventData.needs_address_validation || false,
        event_data: eventData.event_data,
        contact_data: eventData.contact_data
        // Note: created_at is handled by database DEFAULT
      };
      
      const { rows } = await db.insert('hubspot_webhook_queue', queueItem, {
        returning: ['id', 'event_id']
      });
      
      const result = rows[0];
      
      this.logger.info('Webhook event enqueued', {
        eventId: eventData.event_id,
        type: eventData.subscription_type,
        clientId: eventData.client_id,
        queueId: result.id
      });
      
      return result;
    } catch (error) {
      // Check for duplicate event
      if (error.code === '23505') { // PostgreSQL unique violation
        this.logger.debug('Event already queued', {
          eventId: eventData.event_id
        });
        return { id: null, event_id: eventData.event_id, duplicate: true };
      }
      
      this.logger.error('Failed to enqueue webhook event', error, {
        eventId: eventData.event_id
      });
      throw new QueueError('Failed to enqueue event', error);
    }
  }
  
  // Process pending queue items (called by cron)
  async processPendingItems(options = {}) {
    const {
      batchSize = config.queue.batchSize,
      maxRuntime = config.queue.maxRuntime || 270000, // 4.5 minutes default
      concurrency = this.maxConcurrency
    } = options;
    
    // Prevent concurrent processing
    if (this.isProcessing) {
      this.logger.warn('Queue processing already in progress');
      return {
        status: 'already_processing',
        message: 'Another process is already running'
      };
    }
    
    this.isProcessing = true;
    this.processingStartTime = Date.now();
    this.abortController = new AbortController();
    
    const stats = {
      processed: 0,
      failed: 0,
      skipped: 0,
      errors: [],
      runtime: 0
    };
    
    try {
      // Fetch pending items using Supabase query builder
      const items = await this.getPendingQueueItems(batchSize);
      
      if (items.length === 0) {
        this.logger.info('No pending queue items');
        return {
          status: 'empty',
          message: 'No pending items to process',
          ...stats
        };
      }
      
      this.logger.info('Processing queue batch', {
        batchSize: items.length,
        maxRuntime,
        concurrency
      });
      
      // Process items in parallel batches
      const results = await this.processItemsInParallel(items, {
        concurrency,
        maxRuntime,
        signal: this.abortController.signal
      });
      
      // Aggregate results
      for (const result of results) {
        if (result.status === 'fulfilled') {
          if (result.value.success) {
            stats.processed++;
          } else {
            stats.failed++;
            stats.errors.push({
              itemId: result.value.itemId,
              error: result.value.error
            });
          }
        } else {
          stats.failed++;
          stats.errors.push({
            error: result.reason?.message || 'Unknown error'
          });
        }
      }
      
      stats.runtime = Date.now() - this.processingStartTime;
      
      this.logger.info('Queue batch processing completed', stats);
      
      // Update metrics
      this.updateMetrics(stats);
      
      return {
        status: 'completed',
        ...stats
      };
      
    } catch (error) {
      this.logger.error('Queue processing failed', error);
      stats.runtime = Date.now() - this.processingStartTime;
      
      return {
        status: 'error',
        error: error.message,
        ...stats
      };
    } finally {
      this.isProcessing = false;
      this.processingStartTime = null;
      this.abortController = null;
    }
  }
  
  // Get pending queue items using Supabase query builder
  async getPendingQueueItems(limit) {
    try {
      // First, let's debug what's in the queue
      const debugResult = await db.select('hubspot_webhook_queue', {}, {
        columns: 'id, event_id, status, attempts, max_attempts, processing_completed_at, next_retry_at',
        order: { column: 'id', ascending: true }
      });
      
      this.logger.warn('DEBUG: All queue items:', {
        items: debugResult.rows
      });
      
      // Now get pending items using Supabase query builder
      const result = await db.executeWithRetry(async (supabase) => {
        // First get all pending items
        const { data, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('*')
          .eq('status', 'pending')
          .order('created_at', { ascending: true })
          .limit(limit * 2); // Get extra to filter in memory
        
        if (error) throw error;
        
        // Filter in JavaScript to handle complex conditions
        const now = new Date().toISOString();
        const filteredData = (data || []).filter(item => {
          // Check if attempts < max_attempts
          if (item.attempts >= item.max_attempts) {
            return false;
          }
          
          // Check if next_retry_at is null or in the past
          if (item.next_retry_at === null || item.next_retry_at <= now) {
            return true;
          }
          
          return false;
        });
        
        // Return only the requested limit
        return filteredData.slice(0, limit);
      });
      
      this.logger.warn('DEBUG: Pending items found:', {
        count: result.length,
        items: result.map(r => ({
          id: r.id,
          status: r.status,
          attempts: r.attempts,
          completed_at: r.processing_completed_at
        }))
      });
      
      return result;
    } catch (error) {
      this.logger.error('Failed to get pending queue items', error);
      throw new QueueError('Failed to fetch queue items', error);
    }
  }
  
  // Process items in parallel with concurrency control
  async processItemsInParallel(items, options) {
    const { concurrency, maxRuntime, signal } = options;
    const startTime = Date.now();
    
    // Create a queue of items to process
    const queue = [...items];
    const inProgress = new Map();
    const results = [];
    
    // Process items with concurrency limit
    while (queue.length > 0 || inProgress.size > 0) {
      // Check if we're running out of time
      const elapsed = Date.now() - startTime;
      if (elapsed > maxRuntime - 2000) { // Leave 2s buffer
        this.logger.warn('Approaching max runtime, stopping processing', {
          elapsed,
          maxRuntime,
          remaining: queue.length,
          inProgress: inProgress.size
        });
        break;
      }
      
      // Start new items up to concurrency limit
      while (queue.length > 0 && inProgress.size < concurrency) {
        const item = queue.shift();
        
        // Create cancellable promise
        const processingPromise = this.processItemWithTimeout(item, {
          timeout: Math.min(10000, maxRuntime - elapsed - 1000), // Item timeout
          signal
        });
        
        // Track the promise
        inProgress.set(item.id, processingPromise);
        
        // Handle completion
        processingPromise
          .then(result => {
            results.push({ status: 'fulfilled', value: result });
            inProgress.delete(item.id);
          })
          .catch(error => {
            results.push({ 
              status: 'rejected', 
              reason: error,
              itemId: item.id 
            });
            inProgress.delete(item.id);
          });
      }
      
      // Wait for at least one to complete if we're at capacity
      if (inProgress.size >= concurrency || (queue.length === 0 && inProgress.size > 0)) {
        await Promise.race(Array.from(inProgress.values()));
      }
    }
    
    // Wait for remaining items
    if (inProgress.size > 0) {
      const remainingResults = await Promise.allSettled(Array.from(inProgress.values()));
      results.push(...remainingResults);
    }
    
    return results;
  }
  
  // Process a single queue item with timeout
  async processItemWithTimeout(item, options) {
    const { timeout, signal } = options;
    
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new TimeoutError('Item processing', timeout)), timeout);
    });
    
    const abortPromise = new Promise((_, reject) => {
      signal?.addEventListener('abort', () => {
        reject(new Error('Processing aborted'));
      });
    });
    
    try {
      const result = await Promise.race([
        this.processQueueItem(item),
        timeoutPromise,
        abortPromise
      ]);
      
      return result;
    } catch (error) {
      // Update item as failed
      await this.markItemFailed(item, error);
      throw error;
    }
  }
  
  // Process a single queue item
  async processQueueItem(item) {
    const startTime = Date.now();
    
    try {
      this.logger.debug('Processing queue item', {
        id: item.id,
        eventId: item.event_id,
        type: item.subscription_type,
        attempts: item.attempts
      });
      
      // Mark as processing
      await this.updateQueueItemStatus(item.id, 'processing', {
        processing_started_at: new Date().toISOString()
      });
      
      // Get contact data (if not already fetched)
      let contactData = item.contact_data;
      if (!contactData && item.needs_email_validation) {
        contactData = await this.fetchContactData(item);
      }
      
      // Perform validations based on event type
      const validationResults = await this.performValidations(item, contactData);
      
      // Submit to HubSpot form
      const formResult = await this.submitToHubSpot(item, contactData, validationResults);
      
      // Mark as completed
      await this.updateQueueItemStatus(item.id, 'completed', {
        processing_completed_at: new Date().toISOString(),
        validation_results: validationResults,
        form_submission_response: formResult
      });
      
      const processingTime = Date.now() - startTime;
      
      this.logger.info('Queue item processed successfully', {
        id: item.id,
        eventId: item.event_id,
        processingTime
      });
      
      return {
        success: true,
        itemId: item.id,
        processingTime,
        validations: validationResults
      };
      
    } catch (error) {
      const processingTime = Date.now() - startTime;
      
      this.logger.error('Failed to process queue item', error, {
        id: item.id,
        eventId: item.event_id,
        attempts: item.attempts,
        processingTime
      });
      
      return {
        success: false,
        itemId: item.id,
        error: error.message,
        processingTime
      };
    }
  }
  
  // Fetch contact data from HubSpot
  async fetchContactData(item) {
    try {
      if (!item.object_id) {
        throw new Error('No contact ID provided');
      }
      
      // Get client's HubSpot config
      const hubspotConfig = await clientService.getClientHubSpotConfig(item.client_id);
      if (!hubspotConfig?.enabled) {
        throw new Error('HubSpot not enabled for client');
      }
      
      // Fetch contact from HubSpot
      const contact = await hubspotService.fetchContact(
        item.object_id,
        hubspotConfig.apiKey,
        {
          properties: [
            'email', 'firstname', 'lastname', 'phone',
            'address', 'city', 'state', 'zip', 'country',
            'um_email', 'um_first_name', 'um_last_name',
            'um_email_status', 'um_bounce_status', 'um_name_status',
            'um_phone1', 'um_phone2', 'um_phone1_status', 'um_phone2_status'
          ]
        }
      );
      
      // Store in queue item for future use
      await this.updateQueueItemData(item.id, {
        contact_data: contact
      });
      
      return contact;
    } catch (error) {
      this.logger.error('Failed to fetch contact data', error, {
        itemId: item.id,
        contactId: item.object_id
      });
      throw error;
    }
  }
  
  // Perform validations based on event type
  async performValidations(item, contactData) {
    const results = {};
    
    // Determine what to validate
    const shouldValidateEmail = 
      item.needs_email_validation && contactData?.properties?.email;
      
    const shouldValidateName = 
      item.needs_name_validation && 
      (contactData?.properties?.firstname || contactData?.properties?.lastname);
      
    const shouldValidatePhone = 
      item.needs_phone_validation && contactData?.properties?.phone;
      
    const shouldValidateAddress = 
      item.needs_address_validation && 
      (contactData?.properties?.address || contactData?.properties?.city || 
       contactData?.properties?.state || contactData?.properties?.zip);
    
    // Run validations in parallel
    const validationPromises = [];
    
    if (shouldValidateEmail) {
      validationPromises.push(
        validationService.validateEmail(contactData.properties.email, {
          clientId: item.client_id,
          skipExternal: false // Use external for background processing
        }).then(result => {
          results.email = result;
        }).catch(error => {
          this.logger.error('Email validation failed', error);
          results.email = { error: error.message };
        })
      );
    }
    
    if (shouldValidateName) {
      validationPromises.push(
        validationService.validateName(null, {
          clientId: item.client_id,
          firstName: contactData.properties.firstname,
          lastName: contactData.properties.lastname
        }).then(result => {
          results.name = result;
        }).catch(error => {
          this.logger.error('Name validation failed', error);
          results.name = { error: error.message };
        })
      );
    }
    
    if (shouldValidatePhone) {
      validationPromises.push(
        validationService.validatePhone(contactData.properties.phone, {
          clientId: item.client_id
        }).then(result => {
          results.phone = result;
        }).catch(error => {
          this.logger.error('Phone validation failed', error);
          results.phone = { error: error.message };
        })
      );
    }
    
    if (shouldValidateAddress) {
      const addressData = {
        line1: contactData.properties.address,
        city: contactData.properties.city,
        state: contactData.properties.state,
        postalCode: contactData.properties.zip,
        country: contactData.properties.country
      };
      
      validationPromises.push(
        validationService.validateAddress(addressData, {
          clientId: item.client_id
        }).then(result => {
          results.address = result;
        }).catch(error => {
          this.logger.error('Address validation failed', error);
          results.address = { error: error.message };
        })
      );
    }
    
    // Wait for all validations to complete
    await Promise.all(validationPromises);
    
    return results;
  }
  
  // Submit validation results to HubSpot
  // Submit validation results to HubSpot
async submitToHubSpot(item, contactData, validationResults) {
  try {
    // Get client's HubSpot config
    const hubspotConfig = await clientService.getClientHubSpotConfig(item.client_id);
    
    if (!hubspotConfig?.enabled || !hubspotConfig.portalId || !hubspotConfig.formGuid) {
      throw new Error('HubSpot form submission not configured');
    }
    
    // Build form fields with proper structure
    const formData = this.buildFormData(item, contactData, validationResults, item.client_id);
    
    // Debug log the form data being submitted - show actual structure
    this.logger.info('Submitting form data to HubSpot', {
      formGuid: hubspotConfig.formGuid,
      contactId: item.object_id,
      fieldCount: formData.fields.length,
      fieldNames: formData.fields.map(f => f.name),
      // Log a sample of the fields structure for debugging
      sampleFields: formData.fields.slice(0, 3).map(f => ({ name: f.name, value: f.value }))
    });
    
    // Submit to HubSpot with contact association
    const result = await hubspotService.submitForm(
      formData, // Pass the whole formData object with { fields: [...] } structure
      hubspotConfig.portalId,
      hubspotConfig.formGuid,
      {
        objectId: item.object_id, // Associate with the contact
        skipValidation: true, // Handle field mismatches gracefully
        pageUri: 'https://unmessy-api.vercel.app/queue-processor',
        pageName: 'Unmessy Queue Processor'
      }
    );
    
    // Update rate limits
    await this.updateRateLimits(item.client_id, validationResults);
    
    return result;
  } catch (error) {
    // If form fields don't match, log but don't fail
    if (error.message?.includes('field') || error.message?.includes('property')) {
      this.logger.warn('Form field mismatch, some fields may not have been submitted', {
        itemId: item.id,
        contactId: item.object_id,
        error: error.message
      });
      return { success: true, warning: 'partial_submission' };
    }
    
    this.logger.error('Failed to submit to HubSpot', error, {
      itemId: item.id,
      contactId: item.object_id
    });
    throw error;
  }
}
  
  // Build form data for HubSpot submission with proper fields array structure
  buildFormData(item, contactData, validationResults, clientId) {
    const fields = [];
    
    // Always include UM check ID and epoch
    const epochMs = Date.now();
    const umCheckId = this.generateUmCheckId(clientId, epochMs);
    
    // Fix epoch to be in seconds, not milliseconds
    fields.push({
      name: 'date_last_um_check_epoch',
      value: Math.floor(epochMs / 1000).toString()
    });
    
    fields.push({
      name: 'um_check_id',
      value: String(umCheckId)
    });
    
    // Add original contact fields (firstname, lastname, email)
    if (contactData?.properties?.email) {
      fields.push({
        name: 'email',
        value: contactData.properties.email
      });
    }
    
    if (item.contact_firstname || contactData?.properties?.firstname) {
      fields.push({
        name: 'firstname',
        value: item.contact_firstname || contactData.properties.firstname || ''
      });
    }
    
    if (item.contact_lastname || contactData?.properties?.lastname) {
      fields.push({
        name: 'lastname',
        value: item.contact_lastname || contactData.properties.lastname || ''
      });
    }
    
    // Add email validation results (um_ fields)
    if (validationResults.email && !validationResults.email.error) {
      const emailResult = validationResults.email;
      fields.push({
        name: 'um_email',
        value: emailResult.currentEmail || emailResult.um_email || contactData?.properties?.email || ''
      });
      fields.push({
        name: 'um_email_status',
        value: emailResult.um_email_status || 'Unchanged'
      });
      fields.push({
        name: 'um_bounce_status',
        value: emailResult.um_bounce_status || 'Unknown'
      });
    }
    
    // Add name validation results (um_ fields)
    if (validationResults.name && !validationResults.name.error) {
      const nameResult = validationResults.name;
      fields.push({
        name: 'um_first_name',
        value: nameResult.firstName || ''
      });
      fields.push({
        name: 'um_last_name',
        value: nameResult.lastName || ''
      });
      fields.push({
        name: 'um_name_status',
        value: nameResult.wasCorrected ? 'Changed' : 'Unchanged'
      });
      fields.push({
        name: 'um_name_format',
        value: nameResult.formatValid ? 'Valid' : 'Invalid'
      });
      
      if (nameResult.middleName) {
        fields.push({
          name: 'um_middle_name',
          value: nameResult.middleName
        });
      }
      
      if (nameResult.honorific) {
        fields.push({
          name: 'um_honorific',
          value: nameResult.honorific
        });
      }
      
      if (nameResult.suffix) {
        fields.push({
          name: 'um_suffix',
          value: nameResult.suffix
        });
      }
      
      if (nameResult.firstName || nameResult.lastName) {
        fields.push({
          name: 'um_name',
          value: `${nameResult.firstName || ''} ${nameResult.lastName || ''}`.trim()
        });
      }
    }
    
    // Add phone validation results (um_ fields)
    if (validationResults.phone && !validationResults.phone.error) {
      const phoneResult = validationResults.phone;
      
      // Determine whether to use um_phone1 or um_phone2
      const existingPhone1 = contactData?.properties?.um_phone1;
      const usePhone2 = existingPhone1 && existingPhone1.trim() !== '';
      
      if (usePhone2) {
        // Use um_phone2 fields if um_phone1 already has a value
        fields.push({
          name: 'um_phone2',
          value: phoneResult.formatted || ''
        });
        fields.push({
          name: 'um_phone2_status',
          value: phoneResult.isValid ? 'Valid' : 'Invalid'
        });
        fields.push({
          name: 'um_phone_2_format',
          value: phoneResult.type || 'unknown'
        });
        fields.push({
          name: 'um_phone2_country_code',
          value: phoneResult.countryCode || ''
        });
        fields.push({
          name: 'um_phone2_is_mobile',
          value: phoneResult.isMobile ? 'Yes' : 'No'
        });
        fields.push({
          name: 'um_phone2_country',
          value: phoneResult.country || ''
        });
      } else {
        // Default to um_phone1 fields
        fields.push({
          name: 'um_phone1',
          value: phoneResult.formatted || ''
        });
        fields.push({
          name: 'um_phone1_status',
          value: phoneResult.isValid ? 'Valid' : 'Invalid'
        });
        fields.push({
          name: 'um_phone1_format',
          value: phoneResult.type || 'unknown'
        });
        fields.push({
          name: 'um_phone1_country_code',
          value: phoneResult.countryCode || ''
        });
        fields.push({
          name: 'um_phone1_is_mobile',
          value: phoneResult.isMobile ? 'Yes' : 'No'
        });
        fields.push({
          name: 'um_phone1_country',
          value: phoneResult.country || ''
        });
      }
      
      this.logger.info('Phone validation mapped to field', {
        usePhone2,
        existingPhone1: existingPhone1 || 'empty',
        newPhone: phoneResult.formatted
      });
    }
    
    // Add address validation results
    if (validationResults.address && !validationResults.address.error) {
      const addressResult = validationResults.address;
      
      const addressFields = [
        { name: 'um_house_number', value: addressResult.houseNumber || '' },
        { name: 'um_street_name', value: addressResult.streetName || '' },
        { name: 'um_street_type', value: addressResult.streetType || '' },
        { name: 'um_street_direction', value: addressResult.streetDirection || '' },
        { name: 'um_unit_type', value: addressResult.unitType || '' },
        { name: 'um_unit_number', value: addressResult.unitNumber || '' },
        { name: 'um_city', value: addressResult.city || '' },
        { name: 'um_state_province', value: addressResult.state || '' },
        { name: 'um_postal_code', value: addressResult.postalCode || '' },
        { name: 'um_country', value: addressResult.country || '' },
        { name: 'um_country_code', value: addressResult.countryCode || '' },
        { name: 'um_address_status', value: addressResult.isValid ? 'Valid' : 'Invalid' },
        { name: 'um_formatted_address', value: addressResult.formatted || '' }
      ];
      
      addressFields.forEach(field => {
        if (field.value) {
          fields.push(field);
        }
      });
      
      if (addressResult.latitude && addressResult.longitude) {
        fields.push({
          name: 'um_latitude',
          value: String(addressResult.latitude)
        });
        fields.push({
          name: 'um_longitude',
          value: String(addressResult.longitude)
        });
      }
    }
    
    // Return in the format expected by HubSpot service
    return {
      fields: fields.filter(field => field.value !== null && field.value !== undefined && field.value !== '')
    };
  }
  
  // Generate UM check ID
  generateUmCheckId(clientId, epochMs) {
    const lastSixDigits = String(epochMs).slice(-6);
    const clientIdStr = clientId || config.clients.defaultClientId || '0001';
    const firstThreeDigits = String(epochMs).slice(0, 3);
    const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
    const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
    return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${config.unmessy.version.replace(/\./g, '')}`);
  }
  
  // Update rate limits after successful validation
  async updateRateLimits(clientId, validationResults) {
    const promises = [];
    
    if (validationResults.email && !validationResults.email.error) {
      promises.push(
        clientService.incrementUsage(clientId, 'email')
          .catch(err => this.logger.error('Failed to update email rate limit', err))
      );
    }
    
    if (validationResults.name && !validationResults.name.error) {
      promises.push(
        clientService.incrementUsage(clientId, 'name')
          .catch(err => this.logger.error('Failed to update name rate limit', err))
      );
    }
    
    if (validationResults.phone && !validationResults.phone.error) {
      promises.push(
        clientService.incrementUsage(clientId, 'phone')
          .catch(err => this.logger.error('Failed to update phone rate limit', err))
      );
    }
    
    if (validationResults.address && !validationResults.address.error) {
      promises.push(
        clientService.incrementUsage(clientId, 'address')
          .catch(err => this.logger.error('Failed to update address rate limit', err))
      );
    }
    
    await Promise.all(promises);
  }
  
  // Update queue item status
  async updateQueueItemStatus(itemId, status, additionalData = {}) {
    try {
      this.logger.info('Updating queue item status', {
        itemId,
        oldStatus: 'unknown',
        newStatus: status,
        hasAdditionalData: Object.keys(additionalData).length > 0
      });
      
      const updates = {
        status,
        ...additionalData
      };
      
      const result = await db.update(
        'hubspot_webhook_queue',
        updates,
        { id: itemId },
        { returning: true }
      );
      
      // Log the result
      if (result.rows && result.rows.length > 0) {
        this.logger.info('Queue item status updated successfully', {
          itemId,
          newStatus: result.rows[0].status,
          updateConfirmed: result.rows[0].status === status
        });
      } else {
        this.logger.error('Queue item update returned no rows', {
          itemId,
          attemptedStatus: status
        });
      }
      
      return result;
    } catch (error) {
      this.logger.error('Failed to update queue item status', error, {
        itemId,
        status,
        errorMessage: error.message,
        errorCode: error.code
      });
      throw error;
    }
  }
  
  // Update queue item data
  async updateQueueItemData(itemId, data) {
    try {
      await db.update(
        'hubspot_webhook_queue',
        data,
        { id: itemId }
      );
    } catch (error) {
      this.logger.error('Failed to update queue item data', error, {
        itemId
      });
      throw error;
    }
  }
  
  // Mark item as failed
  async markItemFailed(item, error) {
    const shouldRetry = item.attempts < item.max_attempts;
    const nextStatus = shouldRetry ? 'pending' : 'failed';
    
    await this.updateQueueItemStatus(item.id, nextStatus, {
      attempts: item.attempts + 1,
      error_message: error.message,
      error_details: {
        name: error.name,
        stack: error.stack,
        timestamp: new Date().toISOString()
      },
      next_retry_at: shouldRetry ? 
        new Date(Date.now() + this.calculateBackoff(item.attempts)).toISOString() : 
        null
    });
  }
  
  // Calculate exponential backoff for retries
  calculateBackoff(attempts) {
    const baseDelay = 60000; // 1 minute
    const maxDelay = 3600000; // 1 hour
    const delay = Math.min(baseDelay * Math.pow(2, attempts), maxDelay);
    return delay;
  }
  
  // Update metrics
  updateMetrics(stats) {
    this.metrics.totalProcessed += stats.processed;
    this.metrics.totalFailed += stats.failed;
    this.metrics.lastProcessedAt = new Date();
    
    // Update average processing time
    const totalItems = stats.processed + stats.failed;
    if (totalItems > 0) {
      const avgTime = stats.runtime / totalItems;
      if (this.metrics.averageProcessingTime === 0) {
        this.metrics.averageProcessingTime = avgTime;
      } else {
        // Exponential moving average
        this.metrics.averageProcessingTime = 
          (this.metrics.averageProcessingTime * 0.7) + (avgTime * 0.3);
      }
    }
  }
  
  // Get queue statistics using Supabase query builder
  async getQueueStats() {
    try {
      // Get counts by status
      const result = await db.executeWithRetry(async (supabase) => {
        const { data, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('status', { count: 'exact' });
        
        if (error) throw error;
        
        // Group by status manually since Supabase doesn't support GROUP BY in select
        const statusCounts = {};
        for (const row of (data || [])) {
          statusCounts[row.status] = (statusCounts[row.status] || 0) + 1;
        }
        
        return statusCounts;
      });
      
      const stats = {
        pending: result.pending || 0,
        processing: result.processing || 0,
        completed: result.completed || 0,
        failed: result.failed || 0,
        total: 0
      };
      
      // Calculate total
      stats.total = Object.values(stats).reduce((sum, count) => sum + count, 0);
      
      return {
        ...stats,
        metrics: this.metrics,
        isProcessing: this.isProcessing,
        processingStartTime: this.processingStartTime
      };
    } catch (error) {
      this.logger.error('Failed to get queue stats', error);
      throw new QueueError('Failed to retrieve queue statistics', error);
    }
  }
  
  /**
   * Check queue status with alerting
   */
  async checkQueueStatus() {
    try {
      this.logger.debug('Checking queue status');
      
      // Get all queue items to analyze
      const result = await db.select('hubspot_webhook_queue', {}, {
        columns: 'status, created_at'
      });
      
      // Process results
      const stats = {
        pending: 0,
        processing: 0,
        completed: 0,
        failed: 0,
        oldest: 0,
        timestamp: new Date().toISOString()
      };
      
      const now = Date.now();
      result.rows.forEach(row => {
        stats[row.status.toLowerCase()] = (stats[row.status.toLowerCase()] || 0) + 1;
        
        const ageSeconds = (now - new Date(row.created_at).getTime()) / 1000;
        if (ageSeconds > stats.oldest) {
          stats.oldest = ageSeconds;
        }
      });
      
      // Check for backed up queue
      const pendingThreshold = config.queue?.pendingThreshold || 100;
      if (stats.pending > pendingThreshold) {
        triggerAlert(ALERT_TYPES.APPLICATION.QUEUE_BACKED_UP, {
          pendingCount: stats.pending,
          threshold: pendingThreshold
        });
      }
      
      // Check for stalled items
      const stalledThresholdHours = config.queue?.stalledThresholdHours || 1;
      const stalledThresholdSeconds = stalledThresholdHours * 3600;
      if (stats.processing > 0 && stats.oldest > stalledThresholdSeconds) {
        triggerAlert(ALERT_TYPES.APPLICATION.QUEUE_PROCESSING_ERROR, {
          processingCount: stats.processing,
          oldestHours: Math.round(stats.oldest / 3600 * 10) / 10
        });
      }
      
      // Check for failed items
      if (stats.failed > 0) {
        this.logger.warn(`Queue has ${stats.failed} failed items`);
      }
      
      this.logger.info('Queue status check completed', stats);
      return stats;
      
    } catch (error) {
      this.logger.error('Error checking queue status', error);
      throw error;
    }
  }

  /**
   * Reset stalled queue items using Supabase query builder
   */
  async resetStalledItems() {
    try {
      this.logger.info('Resetting stalled queue items');
      
      const stalledThresholdMinutes = config.queue?.stalledThresholdMinutes || 30;
      const cutoffTime = new Date(Date.now() - stalledThresholdMinutes * 60 * 1000).toISOString();
      
      // Find stalled items
      const stalledItems = await db.executeWithRetry(async (supabase) => {
        const { data, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('id, attempts, max_attempts')
          .eq('status', 'processing')
          .lt('processing_started_at', cutoffTime);
        
        if (error) throw error;
        
        // Filter items where attempts < max_attempts
        return (data || []).filter(item => item.attempts < item.max_attempts);
      });
      
      // Reset each stalled item
      let resetCount = 0;
      for (const item of stalledItems) {
        try {
          await db.update(
            'hubspot_webhook_queue',
            {
              status: 'pending',
              processing_started_at: null,
              next_retry_at: new Date().toISOString(),
              attempts: item.attempts + 1
            },
            { id: item.id }
          );
          resetCount++;
        } catch (error) {
          this.logger.error('Failed to reset stalled item', { itemId: item.id, error });
        }
      }
      
      this.logger.info(`Reset ${resetCount} stalled queue items`);
      
      return {
        resetCount
      };
    } catch (error) {
      this.logger.error('Error resetting stalled queue items', error);
      throw error;
    }
  }

  /**
   * Clean up old completed queue items using Supabase query builder
   */
  async cleanupCompletedItems() {
    try {
      this.logger.info('Cleaning up old completed queue items');
      
      const retentionDays = config.queue?.completedRetentionDays || 30;
      const cutoffDate = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();
      
      // Find old completed items
      const oldItems = await db.executeWithRetry(async (supabase) => {
        const { data, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('id')
          .eq('status', 'completed')
          .lt('processing_completed_at', cutoffDate);
        
        if (error) throw error;
        return data || [];
      });
      
      // Delete each old item
      let deletedCount = 0;
      for (const item of oldItems) {
        try {
          await db.executeWithRetry(async (supabase) => {
            const { error } = await supabase
              .from('hubspot_webhook_queue')
              .delete()
              .eq('id', item.id);
            
            if (error) throw error;
          });
          deletedCount++;
        } catch (error) {
          this.logger.error('Failed to delete old item', { itemId: item.id, error });
        }
      }
      
      this.logger.info(`Cleaned up ${deletedCount} old completed queue items`);
      
      return {
        deletedCount
      };
    } catch (error) {
      this.logger.error('Error cleaning up completed queue items', error);
      throw error;
    }
  }
  
  // Get queue status (alias for getQueueStats)
  async getQueueStatus() {
    return this.getQueueStats();
  }
  
  // Process queue batch (for compatibility)
  async processQueue(limit) {
    return this.processPendingItems({ batchSize: limit });
  }
  
  // Process a batch of queue items
  async processQueueBatch(limit = 10) {
    return this.processPendingItems({ batchSize: limit });
  }
  
  // Get failed items with pagination using Supabase query builder
  async getFailedItems(page = 1, limit = 20) {
    try {
      const offset = (page - 1) * limit;
      
      // Get total count
      const countResult = await db.executeWithRetry(async (supabase) => {
        const { count, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('*', { count: 'exact', head: true })
          .eq('status', 'failed');
        
        if (error) throw error;
        return count || 0;
      });
      
      // Get paginated results
      const result = await db.select('hubspot_webhook_queue', 
        { status: 'failed' },
        {
          order: { column: 'created_at', ascending: false },
          limit,
          offset
        }
      );
      
      return {
        items: result.rows,
        pagination: {
          page,
          limit,
          total: countResult,
          pages: Math.ceil(countResult / limit)
        }
      };
    } catch (error) {
      this.logger.error('Failed to get failed items', error);
      throw new QueueError('Failed to retrieve failed items', error);
    }
  }
  
  // Retry a failed item
  async retryItem(itemId) {
    try {
      const { rows } = await db.update(
        'hubspot_webhook_queue',
        {
          status: 'pending',
          attempts: 0,
          next_retry_at: null,
          error_message: null,
          error_details: null
        },
        { id: itemId },
        { returning: true }
      );
      
      if (rows.length === 0) {
        return null;
      }
      
      this.logger.info('Queue item queued for retry', { itemId });
      
      return rows[0];
    } catch (error) {
      this.logger.error('Failed to retry queue item', error, { itemId });
      throw new QueueError('Failed to retry item', error);
    }
  }
  
  // Health check
  async healthCheck() {
    try {
      const stats = await this.getQueueStats();
      const isHealthy = !this.isProcessing || 
        (Date.now() - this.processingStartTime < config.queue.maxRuntime);
      
      return {
        status: isHealthy ? 'healthy' : 'unhealthy',
        isProcessing: this.isProcessing,
        queueDepth: stats.pending,
        metrics: this.metrics
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        error: error.message
      };
    }
  }
}

// Create singleton instance
const queueService = new QueueService();

// Export both the instance and the class
export { queueService as default, QueueService };s