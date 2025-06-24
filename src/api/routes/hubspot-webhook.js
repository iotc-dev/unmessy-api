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
  if (!config.services.hubspot.verifySignature) {
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

    const body = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
    return hubspotService.verifyWebhookSignature(body, signature, secret, version);
  } catch (error) {
    logger.error('Signature verification error', error);
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
    return await hubspotService.getContact(contactId, apiKey);
  } catch (error) {
    logger.error('Error fetching contact', {
      contactId,
      error: error.message
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

    // Parse events
    const events = Array.isArray(req.body) ? req.body : [req.body];

    if (events.length === 0) {
      return res.status(400).json({ error: 'No events provided' });
    }

    // Validate event structure
    for (const event of events) {
      if (!event.eventId || !event.subscriptionType || !event.objectId) {
        logger.error('Invalid event structure', { event });
        return res.status(400).json({ error: 'Invalid event structure' });
      }
    }

    // Process events and queue for processing
    const enrichedEvents = [];

    for (const event of events) {
      try {
        // Only process creation and property changes
        if (!['contact.propertyChange', 'contact.creation'].includes(event.subscriptionType)) {
          logger.info('Skipping unsupported event type', {
            eventId: event.eventId,
            type: event.subscriptionType
          });
          continue;
        }

        // Determine client ID from portal ID or use default
        let clientId = config.clients.defaultClientId;

        // If portal ID is provided, try to find the corresponding client
        if (event.portalId) {
          const foundClientId = await findClientIdFromPortal(event.portalId);
          if (foundClientId) {
            clientId = foundClientId;
          }
        }

        // Get client HubSpot configuration
        const clientConfig = await clientService.getClientHubSpotConfig(clientId);

        if (!clientConfig || !clientConfig.enabled) {
          logger.info('HubSpot not enabled for client', {
            clientId,
            eventId: event.eventId
          });
          continue;
        }

        // Verify signature if enabled
        if (clientConfig.webhookSecret && config.services.hubspot.verifySignature) {
          const isValid = verifySignature(req, clientConfig.webhookSecret);
          if (!isValid) {
            logger.error('Invalid signature for client', { clientId });
            continue;
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
          continue;
        }

        // Create enriched event
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
          occurred_at: event.occurredAt ? new Date(event.occurredAt).toISOString() : new Date().toISOString(),
          event_data: event,
          contact_data: contact,
          status: 'pending',
          client_id: clientId,
          attempts: 0,
          created_at: new Date().toISOString(),
          
          // Determine validation types needed
          needs_email_validation: !!(contact.properties?.email && 
            (!contact.properties.um_email || 
             !contact.properties.um_email_status || 
             !contact.properties.um_bounce_status)),
          
          needs_name_validation: !!((contact.properties?.firstname || contact.properties?.lastname) && 
            (!contact.properties.um_first_name || 
             !contact.properties.um_last_name || 
             !contact.properties.um_name_status))
        };

        enrichedEvents.push(enrichedEvent);
      } catch (error) {
        logger.error('Error enriching event', {
          eventId: event.eventId,
          error: error.message
        });
      }
    }

    // Queue enriched events (don't wait for too long)
    if (enrichedEvents.length > 0) {
      try {
        // Use promise but don't wait for it to complete
        const queuePromise = queueService.enqueueWebhookEvents(enrichedEvents);

        // Wait maximum 2 seconds for queue operation
        await Promise.race([
          queuePromise,
          new Promise(resolve => setTimeout(resolve, 2000))
        ]);
      } catch (error) {
        logger.error('Failed to queue events', {
          error: error.message,
          eventCount: enrichedEvents.length
        });
        // Continue anyway - we'll return 200 to prevent HubSpot retries
      }
    }

    // Return immediately
    const responseTime = Date.now() - startTime;
    logger.info('Returning response', {
      responseTime: `${responseTime}ms`,
      eventsReceived: events.length,
      eventsQueued: enrichedEvents.length
    });

    return res.status(200).json({
      success: true,
      eventsReceived: events.length,
      eventsQueued: enrichedEvents.length,
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
      error: 'Internal processing error'
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
      queueStats = { accessible: false, error: error.message };
    }

    // Get count of enabled HubSpot clients
    let hubspotClientsCount = 0;
    try {
      hubspotClientsCount = await clientService.getHubSpotEnabledClientsCount();
    } catch (error) {
      logger.error('Error counting HubSpot clients', error);
    }

    return res.status(200).json({
      status: 'ok',
      timestamp: new Date().toISOString(),
      webhook: 'active',
      queue: queueStats,
      hubspot: {
        enabledClients: hubspotClientsCount,
        verifySignature: config.services.hubspot.verifySignature
      }
    });
  } catch (error) {
    return res.status(500).json({
      status: 'error',
      error: error.message
    });
  }
}));

/**
 * Process webhook queue manually (for testing or recovery)
 * This endpoint is admin-only
 */
router.post('/process-queue', asyncHandler(async (req, res) => {
  // This should be protected by admin auth middleware
  const limit = req.body.limit || 10;
  
  try {
    const result = await queueService.processQueueBatch(limit);
    
    return res.status(200).json({
      success: true,
      processed: result.processed,
      failed: result.failed,
      remaining: result.remaining
    });
  } catch (error) {
    logger.error('Error processing queue', error);
    throw new ExternalServiceError('Queue Processing', error.message);
  }
}));

export default router;