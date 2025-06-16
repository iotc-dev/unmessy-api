// src/services/queue-service.js
import { db } from '../core/db.js';
import { config } from '../core/config.js';
import { createServiceLogger } from '../core/logger.js';
import { 
  QueueError,
  TimeoutError,
  ErrorRecovery 
} from '../core/errors.js';
import { validationService } from './validation-service.js';
import { clientService } from './client-service.js';
import { hubSpotService } from './external/hubspot.js';

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
    this.lockTimeout = config.queue.lockTimeout;
    
    // Parallel processing configuration
    this.maxConcurrency = config.queue.maxConcurrency || 5;
  }
  
  // Enqueue a new webhook event
  async enqueueWebhookEvent(eventData) {
    try {
      const queueItem = {
        event_id: eventData.eventId,
        subscription_type: eventData.subscriptionType,
        object_type: eventData.objectType || 'CONTACT',
        object_id: eventData.objectId,
        property_name: eventData.propertyName,
        property_value: eventData.propertyValue,
        occurred_at: eventData.occurredAt,
        portal_id: eventData.portalId,
        client_id: eventData.clientId,
        app_id: eventData.appId,
        event_data: eventData,
        status: 'pending',
        attempts: 0,
        max_attempts: config.queue.maxRetries
      };
      
      const result = await db.enqueueWebhookEvent(queueItem);
      
      this.logger.info('Webhook event enqueued', {
        eventId: eventData.eventId,
        type: eventData.subscriptionType,
        clientId: eventData.clientId,
        queueId: result.id
      });
      
      return result;
    } catch (error) {
      this.logger.error('Failed to enqueue webhook event', error, {
        eventId: eventData.eventId
      });
      throw new QueueError('Failed to enqueue event', error);
    }
  }
  
  // Process pending queue items (called by cron)
  async processPendingItems(options = {}) {
    const {
      batchSize = config.queue.batchSize,
      maxRuntime = config.queue.maxRuntime,
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
      const items = await db.getPendingQueueItems(batchSize);
      
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
            error: result.reason.message
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
        
        // Cancel in-progress items
        for (const [itemId, promise] of inProgress) {
          promise.cancel?.();
        }
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
      await db.updateQueueItemStatus(item.id, 'processing', {
        processing_started_at: new Date().toISOString()
      });
      
      // Get contact data (if not already fetched)
      let contactData = item.contact_data;
      if (!contactData) {
        contactData = await this.fetchContactData(item);
      }
      
      // Perform validations based on event type
      const validationResults = await this.performValidations(item, contactData);
      
      // Submit to HubSpot form
      const formResult = await this.submitToHubSpot(item, contactData, validationResults);
      
      // Mark as completed
      await db.updateQueueItemStatus(item.id, 'completed', {
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
      
      // Update item status
      await this.markItemFailed(item, error);
      
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
      const hubspotConfig = await clientService.getHubSpotConfig(item.client_id);
      if (!hubspotConfig?.enabled) {
        throw new Error('HubSpot not enabled for client');
      }
      
      // Fetch contact from HubSpot
      const contact = await hubSpotService.getContact(item.object_id, {
        portalId: item.portal_id,
        properties: [
          'email', 'firstname', 'lastname', 'phone',
          'um_email', 'um_first_name', 'um_last_name',
          'um_email_status', 'um_bounce_status', 'um_name_status'
        ]
      });
      
      // Store in queue item for future use
      await db.updateQueueItemData(item.id, {
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
      item.subscription_type === 'contact.creation' ||
      (item.subscription_type === 'contact.propertyChange' && item.property_name === 'email');
      
    const shouldValidateName = 
      item.subscription_type === 'contact.creation' ||
      (item.subscription_type === 'contact.propertyChange' && 
       (item.property_name === 'firstname' || item.property_name === 'lastname'));
    
    // Run validations in parallel
    const validationPromises = [];
    
    if (shouldValidateEmail && contactData.email) {
      validationPromises.push(
        validationService.validateEmail(contactData.email, {
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
    
    if (shouldValidateName && (contactData.firstname || contactData.lastname)) {
      validationPromises.push(
        validationService.validateName(null, {
          clientId: item.client_id,
          firstName: contactData.firstname,
          lastName: contactData.lastname
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
      const hubspotConfig = await clientService.getHubSpotConfig(item.client_id);
      
      if (!hubspotConfig?.enabled || !hubspotConfig.portalId || !hubspotConfig.formGuid) {
        throw new Error('HubSpot form submission not configured');
      }
      
      // Build form fields
      const fields = this.buildFormFields(contactData, validationResults, item.client_id);
      
      // Submit to HubSpot
      const result = await hubSpotService.submitForm({
        portalId: hubspotConfig.portalId,
        formGuid: hubspotConfig.formGuid,
        fields,
        context: {
          hutk: contactData.hutk,
          ipAddress: item.ip_address
        }
      });
      
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
      value: contactData.email || ''
    });
    
    // Always include timestamp and check ID
    const epochMs = Date.now();
    const umCheckId = this.generateUmCheckId(clientId, epochMs);
    
    fields.push(
      { name: 'date_last_um_check_epoch', value: epochMs.toString() },
      { name: 'um_check_id', value: umCheckId.toString() }
    );
    
    // Add email validation results
    if (validationResults.email && !validationResults.email.error) {
      const emailResult = validationResults.email;
      fields.push(
        { name: 'um_email', value: emailResult.currentEmail || emailResult.um_email || contactData.email },
        { name: 'um_email_status', value: emailResult.um_email_status || 'Unchanged' },
        { name: 'um_bounce_status', value: emailResult.um_bounce_status || 'Unknown' }
      );
    }
    
    // Add name validation results
    if (validationResults.name && !validationResults.name.error) {
      const nameResult = validationResults.name;
      fields.push(
        { name: 'um_first_name', value: nameResult.firstName || contactData.firstname || '' },
        { name: 'um_last_name', value: nameResult.lastName || contactData.lastname || '' },
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
    const clientIdStr = clientId || config.clients.defaultClientId;
    const firstThreeDigits = String(epochMs).slice(0, 3);
    const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
    const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
    return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${config.unmessy.version}`);
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
  
  // Mark item as failed
  async markItemFailed(item, error) {
    const shouldRetry = item.attempts < item.max_attempts;
    const nextStatus = shouldRetry ? 'pending' : 'failed';
    
    await db.updateQueueItemStatus(item.id, nextStatus, {
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
      const acquired = await db.acquireQueueLock(this.lockId, this.lockTimeout);
      
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
      await db.releaseQueueLock(this.lockId);
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
      const stats = await db.getQueueStats();
      
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
export { queueService, QueueService };