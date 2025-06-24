// src/api/routes/hubspot-webhook.js
import express from 'express';
import { asyncHandler } from '../middleware/error-handler.js';
import { ValidationError, ExternalServiceError } from '../../core/errors.js';
import { createServiceLogger } from '../../core/logger.js';
import { config } from '../../core/config.js';
import db from '../../core/db.js';
import clientService from '../../services/client-service.js';
import hubspotService from '../../services/external/hubspot.js';
import queueService from '../../services/queue-service.js';

// Create logger instance
const logger = createServiceLogger('hubspot-webhook');

const router = express.Router();

/**
 * Verify HubSpot webhook signature
 * @param {Object} req - Express request object
 * @param {string} secret - Webhook secret
 * @returns {boolean} Whether the signature is valid
 */
function verifySignature(req, secret) {
  // Safely check config with proper fallback
  const shouldVerifySignature = config?.services?.hubspot?.verifySignature ?? true;
  
  if (!shouldVerifySignature) {
    logger.debug('Signature verification disabled by config');
    return true; // Skip verification if disabled
  }

  if (!secret) {
    logger.warn('No webhook secret provided for signature verification');
    return false;
  }

  try {
    const signature = req.headers['x-hubspot-signature'];
    const version = req.headers['x-hubspot-signature-version'] || 'v1';
    
    if (!signature) {
      logger.warn('Missing signature header');
      return false;
    }

    // IMPORTANT: Use the raw body for signature verification
    let body;
    
    if (req.rawBody) {
      // If we have raw body from middleware
      body = req.rawBody;
    } else {
      // Reconstruct the body - handle both array and object cases
      const parsedBody = req.body;
      
      // Check if body was parsed as object with numeric keys (Express parsed array as object)
      if (parsedBody && typeof parsedBody === 'object' && !Array.isArray(parsedBody)) {
        // Check if it looks like a parsed array (has keys "0", "1", etc.)
        const keys = Object.keys(parsedBody);
        const isArrayLike = keys.every(key => !isNaN(parseInt(key)));
        
        if (isArrayLike) {
          // Reconstruct as array
          const array = keys.sort((a, b) => parseInt(a) - parseInt(b))
            .map(key => parsedBody[key]);
          body = JSON.stringify(array);
        } else {
          body = JSON.stringify(parsedBody);
        }
      } else {
        body = JSON.stringify(parsedBody);
      }
    }
    
    return hubspotService.verifyWebhookSignature(body, signature, secret, version);
  } catch (error) {
    logger.error('Signature verification error', { 
      error: error.message,
      stack: error.stack 
    });
    return false;
  }
}

/**
 * Find client ID from HubSpot portal ID
 * @param {string} portalId - HubSpot portal ID
 * @returns {Promise<string|null>} Client ID or null if not found
 */
async function findClientIdFromPortal(portalId) {
  try {
    const { data } = await db.executeWithRetry(
      async (supabase) => {
        return await supabase
          .from('clients')
          .select('client_id')
          .eq('hubspot_portal_id', portalId.toString())
          .eq('hubspot_enabled', true)
          .single();
      }
    );

    return data ? data.client_id.toString() : null;
  } catch (error) {
    logger.debug('Could not find client for portal ID', {
      portalId,
      error: error.message
    });
    return null;
  }
}

/**
 * Fetch contact data from HubSpot
 * @param {string} contactId - HubSpot contact ID
 * @param {string} apiKey - HubSpot API key
 * @returns {Promise<Object|null>} Contact data or null if failed
 */
async function fetchContactData(contactId, apiKey) {
  if (!apiKey) {
    logger.error('No HubSpot API key provided');
    return null;
  }

  try {
    return await hubspotService.fetchContact(contactId, apiKey);
  } catch (error) {
    logger.error('Error fetching contact', {
      contactId,
      error: error.message
    });
    return null;
  }
}

/**
 * Process a single webhook event
 * @param {Object} event - The webhook event
 * @param {Object} req - Express request object
 * @returns {Promise<Object|null>} Enriched event or null
 */
async function processWebhookEvent(event, req) {
  try {
    // Validate event structure
    if (!event.eventId || !event.subscriptionType || !event.objectId) {
      logger.warn('Invalid event structure', { event });
      return null;
    }

    // Only process contact events
    if (!['contact.propertyChange', 'contact.creation'].includes(event.subscriptionType)) {
      logger.info('Skipping unsupported event type', {
        eventId: event.eventId,
        type: event.subscriptionType
      });
      return null;
    }

    // Determine client ID from portal ID or use default
    let clientId = config?.clients?.defaultClientId || '1';

    // If portal ID is provided, try to find the corresponding client
    if (event.portalId) {
      const foundClientId = await findClientIdFromPortal(event.portalId);
      if (foundClientId) {
        clientId = foundClientId;
      } else {
        logger.warn('No client found for portal ID, using default', {
          portalId: event.portalId,
          defaultClientId: clientId
        });
      }
    }

    // Get client HubSpot configuration
    let clientConfig;
    try {
      clientConfig = await clientService.getClientHubSpotConfig(clientId);
    } catch (error) {
      logger.error('Failed to get client HubSpot config', {
        clientId,
        error: error.message
      });
      return null;
    }

    if (!clientConfig || !clientConfig.enabled) {
      logger.info('HubSpot not enabled for client', {
        clientId,
        eventId: event.eventId
      });
      return null;
    }

    // Verify signature if enabled and secret is available
    const shouldVerifySignature = config?.services?.hubspot?.verifySignature ?? true;
    if (clientConfig.webhookSecret && shouldVerifySignature) {
      const isValid = verifySignature(req, clientConfig.webhookSecret);
      if (!isValid) {
        logger.error('Invalid signature for client', { 
          clientId,
          eventId: event.eventId 
        });
        return null;
      }
    }

    // Fetch contact data using client-specific API key
    const contact = await fetchContactData(event.objectId, clientConfig.apiKey);

    if (!contact) {
      logger.error('Failed to fetch contact', {
        eventId: event.eventId,
        contactId: event.objectId,
        clientId
      });
      return null;
    }

    // Create enriched event with all necessary fields
    const enrichedEvent = {
      event_id: event.eventId,
      subscription_type: event.subscriptionType,
      object_id: event.objectId,
      portal_id: event.portalId || null,
      property_name: event.propertyName || null,
      property_value: event.propertyValue ? String(event.propertyValue).substring(0, 1000) : null,
      contact_email: contact.properties?.email || null,
      contact_firstname: contact.properties?.firstname || null,
      contact_lastname: contact.properties?.lastname || null,
      contact_phone: contact.properties?.phone || null,
      occurred_at: event.occurredAt ? new Date(event.occurredAt).toISOString() : new Date().toISOString(),
      event_data: event,
      contact_data: contact,
      status: 'pending',
      client_id: clientId,
      attempts: 0,
      created_at: new Date().toISOString(),
      
      // Extract address components if available
      um_house_number: contact.properties?.um_house_number || null,
      um_street_name: contact.properties?.um_street_name || null,
      um_street_type: contact.properties?.um_street_type || null,
      um_street_direction: contact.properties?.um_street_direction || null,
      um_unit_type: contact.properties?.um_unit_type || null,
      um_unit_number: contact.properties?.um_unit_number || null,
      um_city: contact.properties?.city || null,
      um_state_province: contact.properties?.state || null,
      um_country: contact.properties?.country || null,
      um_country_code: contact.properties?.um_country_code || null,
      um_postal_code: contact.properties?.zip || contact.properties?.postal_code || null,
      um_address_status: contact.properties?.um_address_status || null,
      
      // Determine validation types needed based on missing Unmessy fields
      needs_email_validation: !!(contact.properties?.email && 
        (!contact.properties?.um_email || 
         !contact.properties?.um_email_status || 
         !contact.properties?.um_bounce_status)),
      
      needs_name_validation: !!(
        (contact.properties?.firstname || contact.properties?.lastname) &&
        (!contact.properties?.um_firstname || 
         !contact.properties?.um_lastname ||
         !contact.properties?.um_name_status)
      ),
      
      needs_phone_validation: !!(contact.properties?.phone &&
        (!contact.properties?.um_phone || 
         !contact.properties?.um_phone_type ||
         !contact.properties?.um_phone_status)),
      
      needs_address_validation: !!(
        (contact.properties?.address || 
         contact.properties?.city || 
         contact.properties?.state || 
         contact.properties?.zip) &&
        (!contact.properties?.um_address_status || 
         !contact.properties?.um_city ||
         !contact.properties?.um_formatted_address)
      )
    };

    return enrichedEvent;
  } catch (error) {
    logger.error('Error processing webhook event', {
      eventId: event.eventId,
      error: error.message,
      stack: error.stack
    });
    return null;
  }
}

/**
 * Webhook handler for HubSpot events
 * This handler processes events quickly and returns 200 OK immediately
 * while queuing events for asynchronous processing
 */
router.post('/webhook', asyncHandler(async (req, res) => {
  const startTime = Date.now();

  try {
    // Only accept POST
    if (req.method !== 'POST') {
      return res.status(405).json({ error: 'Method not allowed' });
    }

    logger.info('Received webhook request', {
      timestamp: new Date().toISOString(),
      headers: {
        'x-hubspot-signature': req.headers['x-hubspot-signature'] ? 'present' : 'missing',
        'content-type': req.headers['content-type']
      }
    });

    // Parse events from the body
    let events = [];
    
    if (Array.isArray(req.body)) {
      events = req.body;
    } else if (typeof req.body === 'object' && req.body !== null) {
      // Handle object with numeric keys (Express parsed array as object)
      const keys = Object.keys(req.body);
      const isArrayLike = keys.length > 0 && keys.every(key => !isNaN(parseInt(key)));
      
      if (isArrayLike) {
        // Convert to array, sorting by numeric key
        events = keys
          .sort((a, b) => parseInt(a) - parseInt(b))
          .map(key => req.body[key]);
      } else {
        // Single event as object
        events = [req.body];
      }
    }

    if (events.length === 0) {
      logger.warn('No events in webhook request');
      return res.status(200).json({
        success: true,
        eventsReceived: 0,
        eventsQueued: 0
      });
    }

    logger.info(`Processing ${events.length} events`);

    // Process events and queue them
    const enrichedEvents = [];
    const processingErrors = [];
    
    for (const event of events) {
      try {
        const enrichedEvent = await processWebhookEvent(event, req);
        if (enrichedEvent) {
          enrichedEvents.push(enrichedEvent);
        }
      } catch (error) {
        processingErrors.push({
          eventId: event.eventId,
          error: error.message
        });
        logger.error('Error enriching event', {
          eventId: event.eventId,
          error: error.message
        });
      }
    }

    // Queue enriched events for processing if any were successfully processed
    let queueResult = { success: false, queued: 0 };
    if (enrichedEvents.length > 0) {
      try {
        // Try to queue events with a timeout
        const queuePromise = queueService.enqueueWebhookEvents(enrichedEvents);
        
        // Wait maximum 2 seconds for queue operation
        queueResult = await Promise.race([
          queuePromise.then(() => ({ success: true, queued: enrichedEvents.length })),
          new Promise(resolve => setTimeout(() => resolve({ success: false, queued: 0 }), 2000))
        ]);
        
        if (queueResult.success) {
          logger.info(`Successfully queued ${enrichedEvents.length} events`);
        } else {
          logger.warn('Queue operation timed out');
        }
      } catch (error) {
        logger.error('Failed to queue events', {
          error: error.message,
          eventCount: enrichedEvents.length
        });
        // Continue anyway - we'll return 200 to prevent HubSpot retries
      }
    }

    // Calculate response time
    const responseTime = Date.now() - startTime;
    
    // Log final results
    logger.info('Webhook processing complete', {
      responseTime: `${responseTime}ms`,
      eventsReceived: events.length,
      eventsProcessed: enrichedEvents.length,
      eventsQueued: queueResult.queued,
      errors: processingErrors.length
    });

    // Always return 200 to prevent HubSpot retries
    return res.status(200).json({
      success: true,
      eventsReceived: events.length,
      eventsProcessed: enrichedEvents.length,
      eventsQueued: queueResult.queued,
      responseTime: `${responseTime}ms`
    });
  } catch (error) {
    logger.error('Critical error in webhook handler', {
      message: error.message,
      stack: error.stack
    });

    // Always return 200 to prevent HubSpot retries
    return res.status(200).json({
      success: true,
      error: 'Internal processing error',
      eventsReceived: 0,
      eventsQueued: 0
    });
  }
}));

/**
 * Health check endpoint for the HubSpot webhook service
 */
router.get('/health', asyncHandler(async (req, res) => {
  try {
    // Check queue status
    let queueStats = null;
    try {
      queueStats = await queueService.getQueueStats();
    } catch (error) {
      queueStats = { 
        accessible: false, 
        error: error.message 
      };
    }

    // Get count of enabled HubSpot clients
    let hubspotClientsCount = 0;
    try {
      hubspotClientsCount = await clientService.getHubSpotEnabledClientsCount();
    } catch (error) {
      logger.error('Error counting HubSpot clients', error);
    }

    // Check HubSpot service health
    let hubspotServiceHealth = null;
    try {
      hubspotServiceHealth = await hubspotService.healthCheck();
    } catch (error) {
      hubspotServiceHealth = {
        status: 'error',
        error: error.message
      };
    }

    return res.status(200).json({
      status: 'ok',
      timestamp: new Date().toISOString(),
      webhook: 'active',
      queue: queueStats,
      hubspot: {
        enabledClients: hubspotClientsCount,
        verifySignature: config?.services?.hubspot?.verifySignature ?? true,
        service: hubspotServiceHealth
      }
    });
  } catch (error) {
    logger.error('Health check error', error);
    return res.status(500).json({
      status: 'error',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}));

/**
 * Process webhook queue manually (for testing or recovery)
 * This endpoint should be protected by admin auth middleware
 */
router.post('/process-queue', asyncHandler(async (req, res) => {
  const limit = parseInt(req.body?.limit) || 10;
  const maxLimit = 100;
  
  // Validate limit
  if (limit > maxLimit) {
    return res.status(400).json({
      error: `Limit cannot exceed ${maxLimit}`
    });
  }
  
  try {
    logger.info('Manual queue processing requested', { limit });
    
    const result = await queueService.processQueueBatch(limit);
    
    logger.info('Manual queue processing complete', result);
    
    return res.status(200).json({
      success: true,
      processed: result.processed,
      failed: result.failed,
      remaining: result.remaining,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Error processing queue', error);
    throw new ExternalServiceError('Queue Processing', error.message);
  }
}));

export default router;