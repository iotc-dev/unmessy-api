// src/services/workers/queue-processor.js
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import db from '../../core/db.js';
import validationService from '../../services/validation-service.js';
import hubspotService from '../../services/external/hubspot.js';
import { triggerAlert, ALERT_TYPES } from '../../monitoring/alerts.js';
import { ValidationError, ExternalServiceError } from '../../core/errors.js';

// Create logger instance
const logger = createServiceLogger('queue-processor');

/**
 * Process a batch of pending queue items
 * Called by the cron endpoint
 * 
 * @param {number} limit - Maximum number of items to process
 * @returns {Promise<Object>} Processing results
 */
export async function processQueueBatch(limit = 10) {
  const results = {
    processed: 0,
    successful: 0,
    failed: 0,
    errors: [],
    timestamp: new Date().toISOString()
  };
  
  try {
    logger.info(`Starting queue batch processing, limit: ${limit}`);
    
    // Get pending items
    const pendingItems = await getPendingItems(limit);
    
    if (pendingItems.length === 0) {
      logger.info('No pending items to process');
      return results;
    }
    
    logger.info(`Found ${pendingItems.length} pending items to process`);
    
    // Process each item
    for (const item of pendingItems) {
      try {
        // Mark item as processing
        await markItemAsProcessing(item.id);
        
        // Process the item
        await processQueueItem(item);
        
        // Mark as completed
        await markItemAsCompleted(item.id);
        
        results.successful++;
        results.processed++;
        
      } catch (error) {
        results.failed++;
        results.processed++;
        results.errors.push({
          itemId: item.id,
          error: error.message
        });
        
        // Log the error
        logger.error(`Error processing queue item ${item.id}`, error);
        
        // Mark as failed
        await markItemAsFailed(item.id, error.message, error);
      }
    }
    
    // Alert if there were failures
    if (results.failed > 0) {
      triggerAlert(ALERT_TYPES.APPLICATION.QUEUE_PROCESSING_ERROR, {
        processedCount: results.processed,
        failedCount: results.failed,
        timestamp: results.timestamp
      });
    }
    
    logger.info('Queue batch processing completed', results);
    
    return results;
    
  } catch (error) {
    logger.error('Error in queue batch processing', error);
    
    triggerAlert(ALERT_TYPES.APPLICATION.QUEUE_PROCESSING_ERROR, {
      error: error.message,
      timestamp: new Date().toISOString()
    });
    
    throw error;
  }
}

/**
 * Get pending queue items
 * 
 * @param {number} limit - Maximum number of items to retrieve
 * @returns {Promise<Array>} Pending queue items
 */
async function getPendingItems(limit) {
  try {
    const { rows } = await db.query(`
      SELECT * FROM hubspot_webhook_queue
      WHERE 
        status = 'pending'
        AND (next_retry_at IS NULL OR next_retry_at <= NOW())
        AND attempts < max_attempts
      ORDER BY created_at ASC
      LIMIT $1
    `, [limit]);
    
    return rows;
  } catch (error) {
    logger.error('Error getting pending queue items', error);
    throw error;
  }
}

/**
 * Mark queue item as processing
 * 
 * @param {number} itemId - Queue item ID
 * @returns {Promise<void>}
 */
async function markItemAsProcessing(itemId) {
  try {
    await db.query(`
      UPDATE hubspot_webhook_queue
      SET 
        status = 'processing',
        processing_started_at = NOW()
      WHERE id = $1
    `, [itemId]);
  } catch (error) {
    logger.error(`Error marking item ${itemId} as processing`, error);
    throw error;
  }
}

/**
 * Mark queue item as completed
 * 
 * @param {number} itemId - Queue item ID
 * @returns {Promise<void>}
 */
async function markItemAsCompleted(itemId) {
  try {
    await db.query(`
      UPDATE hubspot_webhook_queue
      SET 
        status = 'completed',
        processing_completed_at = NOW()
      WHERE id = $1
    `, [itemId]);
  } catch (error) {
    logger.error(`Error marking item ${itemId} as completed`, error);
    throw error;
  }
}

/**
 * Mark queue item as failed
 * 
 * @param {number} itemId - Queue item ID
 * @param {string} errorMessage - Error message
 * @param {Error} error - Error object
 * @returns {Promise<void>}
 */
async function markItemAsFailed(itemId, errorMessage, error) {
  try {
    // Calculate next retry time with exponential backoff
    const item = await getQueueItem(itemId);
    const attempts = (item?.attempts || 0) + 1;
    
    // Calculate backoff time (exponential with maximum)
    let backoffMinutes = Math.min(
      Math.pow(2, attempts) * 5, // 5, 10, 20, 40... minutes
      120 // Max 2 hours
    );
    
    const nextRetryAt = attempts < item.max_attempts ? 
      new Date(Date.now() + backoffMinutes * 60 * 1000) : null;
    
    const errorDetails = {
      message: errorMessage,
      stack: error.stack,
      time: new Date().toISOString()
    };
    
    await db.query(`
      UPDATE hubspot_webhook_queue
      SET 
        status = $1,
        attempts = attempts + 1,
        next_retry_at = $2,
        error_message = $3,
        error_details = $4
      WHERE id = $5
    `, [
      attempts < item.max_attempts ? 'pending' : 'failed',
      nextRetryAt,
      errorMessage.substring(0, 255),
      JSON.stringify(errorDetails),
      itemId
    ]);
  } catch (error) {
    logger.error(`Error marking item ${itemId} as failed`, error);
    // We don't want to throw here as this is already error handling
    // Just log the error
  }
}

/**
 * Get a queue item by ID
 * 
 * @param {number} itemId - Queue item ID
 * @returns {Promise<Object>} Queue item
 */
async function getQueueItem(itemId) {
  try {
    const { rows } = await db.query(`
      SELECT * FROM hubspot_webhook_queue
      WHERE id = $1
    `, [itemId]);
    
    return rows[0];
  } catch (error) {
    logger.error(`Error getting queue item ${itemId}`, error);
    throw error;
  }
}

/**
 * Process a single queue item
 * 
 * @param {Object} item - Queue item
 * @returns {Promise<void>}
 */
async function processQueueItem(item) {
  logger.debug(`Processing queue item ${item.id}`);
  
  // Extract contact data
  const contactData = {
    email: item.contact_email,
    firstName: item.contact_firstname,
    lastName: item.contact_lastname,
    phone: item.contact_phone,
    address: {
      houseNumber: item.um_house_number,
      streetName: item.um_street_name,
      streetType: item.um_street_type,
      streetDirection: item.um_street_direction,
      unitType: item.um_unit_type,
      unitNumber: item.um_unit_number,
      city: item.um_city,
      state: item.um_state_province,
      country: item.um_country,
      countryCode: item.um_country_code,
      postalCode: item.um_postal_code
    }
  };
  
  // Determine what validations are needed
  const validations = {
    email: item.needs_email_validation,
    name: item.needs_name_validation,
    phone: item.needs_phone_validation,
    address: item.needs_address_validation
  };
  
  // Run validations and collect results
  const validationResults = {};
  
  // Email validation
  if (validations.email && contactData.email) {
    validationResults.email = await validationService.validateEmail(
      contactData.email,
      { clientId: item.client_id }
    );
  }
  
  // Name validation
  if (validations.name && (contactData.firstName || contactData.lastName)) {
    validationResults.name = await validationService.validateName(
      contactData.firstName,
      contactData.lastName,
      { clientId: item.client_id }
    );
  }
  
  // Phone validation
  if (validations.phone && contactData.phone) {
    validationResults.phone = await validationService.validatePhone(
      contactData.phone,
      { 
        clientId: item.client_id,
        country: contactData.address.countryCode || 'US'
      }
    );
  }
  
  // Address validation
  if (validations.address && 
      (contactData.address.streetName || 
       contactData.address.city || 
       contactData.address.postalCode)) {
    validationResults.address = await validationService.validateAddress(
      contactData.address,
      { 
        clientId: item.client_id,
        country: contactData.address.countryCode || 'US'
      }
    );
  }
  
  // Get HubSpot config for the client
  const clientHubspotConfig = await getClientHubspotConfig(item.client_id);
  
  if (!clientHubspotConfig) {
    throw new Error(`No HubSpot configuration found for client ${item.client_id}`);
  }
  
  // Submit results back to HubSpot
  const submissionResult = await hubspotService.submitValidationResults(
    {
      properties: {
        email: contactData.email,
        firstname: contactData.firstName,
        lastname: contactData.lastName,
        phone: contactData.phone
      },
      objectId: item.object_id
    },
    validationResults,
    clientHubspotConfig
  );
  
  // Update the queue item with results
  await db.query(`
    UPDATE hubspot_webhook_queue
    SET 
      validation_results = $1,
      form_submission_response = $2
    WHERE id = $3
  `, [
    JSON.stringify(validationResults),
    JSON.stringify(submissionResult),
    item.id
  ]);
  
  logger.info(`Successfully processed queue item ${item.id}`);
}

/**
 * Get HubSpot configuration for a client
 * 
 * @param {string} clientId - Client ID
 * @returns {Promise<Object>} HubSpot configuration
 */
async function getClientHubspotConfig(clientId) {
  try {
    const { rows } = await db.query(`
      SELECT 
        hubspot_portal_id as portalId,
        hubspot_form_guid as formGuid,
        hubspot_private_key as privateKey
      FROM clients
      WHERE 
        client_id = $1
        AND hubspot_enabled = true
    `, [clientId]);
    
    return rows[0];
  } catch (error) {
    logger.error(`Error getting HubSpot config for client ${clientId}`, error);
    throw error;
  }
}

// Export the main function and the entire object
export { processQueueBatch };
export default {
  processQueueBatch
};