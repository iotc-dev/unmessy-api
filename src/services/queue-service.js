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
    
    // Lock management
    this.lockId = null;
    this.lockTimeout = config.queue.lockTimeout || 300000; // 5 minutes default
    
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
        object_id: eventData.object_id,
        portal_id: eventData.portal_id,
        occurred_at: eventData.occurred_at,
        property_name: eventData.property_name,
        property_value: eventData.property_value,
        contact_email: eventData.contact_email,
        contact_firstname: eventData.contact_firstname,
        contact_lastname: eventData.contact_lastname,
        contact_phone: eventData.contact_phone,
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
      // Acquire lock
      const lockAcquired = await this.acquireLock();
      if (!lockAcquired) {
        return {
          status: 'locked',
          message: 'Could not acquire processing lock'
        };
      }
      
      // Fetch pending items
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
      
      // Release lock
      await this.releaseLock();
    }
  }
  
  // Get pending queue items
  async getPendingQueueItems(limit) {
    try {
      const { rows } = await db.query(
        `SELECT * FROM hubspot_webhook_queue
         WHERE status = 'pending'
         AND (next_retry_at IS NULL OR next_retry_at <= NOW())
         AND attempts < max_attempts
         ORDER BY created_at ASC
         LIMIT $1`,
        [limit]
      );
      
      return rows;
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
            'um_email', 'um_first_name', 'um_last_name',
            'um_email_status', 'um_bounce_status', 'um_name_status'
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
    
    // Wait for all validations to complete
    await Promise.all(validationPromises);
    
    return results;
  }
  
  // Submit validation results to HubSpot
  async submitToHubSpot(item, contactData, validationResults) {
    try {
      // Get client's HubSpot config
      const hubspotConfig = await clientService.getClientHubSpotConfig(item.client_id);
      
      if (!hubspotConfig?.enabled || !hubspotConfig.portalId || !hubspotConfig.formGuid) {
        throw new Error('HubSpot form submission not configured');
      }
      
      // Build form fields
      const fields = this.buildFormFields(contactData, validationResults, item.client_id);
      
      // Build form data object
      const formData = {};
      fields.forEach(field => {
        formData[field.name] = field.value;
      });
      
      // Submit to HubSpot
      const result = await hubspotService.submitForm(
        formData,
        hubspotConfig.portalId,
        hubspotConfig.formGuid,
        {
          pageUri: 'https://unmessy-api.vercel.app/queue-processor',
          pageName: 'Unmessy Queue Processor'
        }
      );
      
      // Update rate limits
      await this.updateRateLimits(item.client_id, validationResults);
      
      return result;
    } catch (error) {
      this.logger.error('Failed to submit to HubSpot', error, {
        itemId: item.id,
        contactId: item.object_id
      });
      throw error;
    }
  }
  
  // Build form fields for HubSpot submission
  buildFormFields(contactData, validationResults, clientId) {
    const fields = [];
    
    // Always include email for contact matching
    fields.push({
      name: 'email',
      value: contactData?.properties?.email || ''
    });
    
    // Always include timestamp and check ID
    const epochMs = Date.now();
    const umCheckId = this.generateUmCheckId(clientId, epochMs);
    
    fields.push(
      { name: 'date_last_um_check', value: new Date().toISOString() },
      { name: 'date_last_um_check_epoch', value: epochMs.toString() },
      { name: 'um_check_id', value: umCheckId.toString() }
    );
    
    // Add email validation results
    if (validationResults.email && !validationResults.email.error) {
      const emailResult = validationResults.email;
      fields.push(
        { name: 'um_email', value: emailResult.currentEmail || emailResult.um_email || contactData?.properties?.email || '' },
        { name: 'um_email_status', value: emailResult.um_email_status || 'Unchanged' },
        { name: 'um_bounce_status', value: emailResult.um_bounce_status || 'Unknown' }
      );
    }
    
    // Add name validation results
    if (validationResults.name && !validationResults.name.error) {
      const nameResult = validationResults.name;
      fields.push(
        { name: 'um_first_name', value: nameResult.firstName || contactData?.properties?.firstname || '' },
        { name: 'um_last_name', value: nameResult.lastName || contactData?.properties?.lastname || '' },
        { name: 'um_name', value: `${nameResult.firstName || ''} ${nameResult.lastName || ''}`.trim() },
        { name: 'um_name_status', value: nameResult.wasCorrected ? 'Changed' : 'Unchanged' },
        { name: 'um_name_format', value: nameResult.formatValid ? 'Valid' : 'Invalid' }
      );
    }
    
    return fields;
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
    
    await Promise.all(promises);
  }
  
  // Update queue item status
  async updateQueueItemStatus(itemId, status, additionalData = {}) {
    try {
      const updates = {
        status,
        ...additionalData
      };
      
      await db.update(
        'hubspot_webhook_queue',
        updates,
        { id: itemId }
      );
    } catch (error) {
      this.logger.error('Failed to update queue item status', error, {
        itemId,
        status
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
  
  // Lock management
  async acquireLock() {
    try {
      this.lockId = `queue_processor_${Date.now()}_${Math.random()}`;
      
      // Simple lock implementation using database
      const { rows } = await db.insert(
        'queue_locks',
        {
          lock_id: this.lockId,
          locked_at: new Date(),
          expires_at: new Date(Date.now() + this.lockTimeout)
        },
        { returning: true }
      ).catch(() => ({ rows: [] })); // Ignore if table doesn't exist
      
      const acquired = rows.length > 0;
      
      if (acquired) {
        this.logger.info('Queue processing lock acquired', { lockId: this.lockId });
      }
      
      return acquired;
    } catch (error) {
      this.logger.error('Failed to acquire lock', error);
      return false;
    }
  }
  
  async releaseLock() {
    if (!this.lockId) return;
    
    try {
      await db.query(
        'DELETE FROM queue_locks WHERE lock_id = $1',
        [this.lockId]
      ).catch(() => {}); // Ignore if table doesn't exist
      
      this.logger.info('Queue processing lock released', { lockId: this.lockId });
    } catch (error) {
      this.logger.error('Failed to release lock', error);
    } finally {
      this.lockId = null;
    }
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
  
  // Get queue statistics
  async getQueueStats() {
    try {
      const { rows } = await db.query(`
        SELECT 
          status, 
          COUNT(*) as count
        FROM hubspot_webhook_queue
        GROUP BY status
      `);
      
      const stats = {
        pending: 0,
        processing: 0,
        completed: 0,
        failed: 0,
        total: 0
      };
      
      rows.forEach(row => {
        const status = row.status.toLowerCase();
        const count = parseInt(row.count);
        stats[status] = count;
        stats.total += count;
      });
      
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
  
  // Get queue status (alias for getQueueStats)
  async getQueueStatus() {
    return this.getQueueStats();
  }
  
  // Process queue batch (for compatibility)
  async processQueue(limit) {
    return this.processPendingItems({ batchSize: limit });
  }
  
  // Get failed items with pagination
  async getFailedItems(page = 1, limit = 20) {
    try {
      const offset = (page - 1) * limit;
      
      // Get total count
      const countResult = await db.query(
        'SELECT COUNT(*) as total FROM hubspot_webhook_queue WHERE status = $1',
        ['failed']
      );
      const total = parseInt(countResult.rows[0].total);
      
      // Get paginated results
      const { rows } = await db.query(
        `SELECT * FROM hubspot_webhook_queue 
         WHERE status = $1 
         ORDER BY created_at DESC 
         LIMIT $2 OFFSET $3`,
        ['failed', limit, offset]
      );
      
      return {
        items: rows,
        pagination: {
          page,
          limit,
          total,
          pages: Math.ceil(total / limit)
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
        (Date.now() - this.processingStartTime < this.lockTimeout);
      
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
export { queueService as default, QueueService };