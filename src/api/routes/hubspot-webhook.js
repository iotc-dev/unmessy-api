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
 * Fetch contact data from HubSpot with comprehensive properties
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
    // Comprehensive list of properties to fetch
    const properties = [
      // Standard contact fields
      'email', 'firstname', 'lastname',
      
      // ALL possible phone fields
      'phone', 'mobilephone', 'hs_phone_number',
      'phone_number', 'mobile_phone_number',
      'work_phone', 'home_phone', 'cell_phone',
      
      // ALL possible address fields
      'address', 'address2', 
      'hs_street_address_1', 'hs_street_address_2',
      'city', 'state', 'zip', 'postal_code',
      'hs_city', 'hs_state_region', 'hs_postal_code',
      'country', 'country_code', 'hs_country',
      
      // Unmessy processed fields
      'um_email', 'um_first_name', 'um_last_name',
      'um_email_status', 'um_bounce_status', 'um_name_status',
      
      // Unmessy phone fields
      'um_phone1', 'um_phone1_status', 'um_phone1_format',
      'um_phone1_country_code', 'um_phone1_is_mobile',
      'um_phone1_country', 'um_phone1_area_code', 'um_phone1_area',
      'um_phone2', 'um_phone2_status', 'um_phone2_format',
      'um_phone2_country_code', 'um_phone2_is_mobile',
      'um_phone2_country', 'um_phone2_area_code', 'um_phone2_area',
      
      // Unmessy address fields
      'um_house_number', 'um_street_name', 'um_street_type', 
      'um_street_direction', 'um_unit_type', 'um_unit_number',
      'um_address_line_1', 'um_address_line_2',
      'um_city', 'um_state_province', 'um_country', 
      'um_country_code', 'um_postal_code', 'um_address_status',
      
      // Additional metadata
      'createdate', 'lastmodifieddate', 'hs_object_id'
    ];

    const contact = await hubspotService.fetchContact(contactId, apiKey, {
      properties: properties
    });

    // Log what data we found for debugging
    const dataFound = {
      hasEmail: !!contact.properties?.email,
      hasPhone: !!contact.properties?.phone,
      hasMobile: !!contact.properties?.mobilephone,
      hasAddress: !!contact.properties?.address,
      hasCity: !!contact.properties?.city,
      hasState: !!contact.properties?.state,
      hasZip: !!contact.properties?.zip,
      hasCountry: !!contact.properties?.country
    };

    logger.info('Contact data fetched from HubSpot', {
      contactId,
      dataFound
    });

    return contact;
  } catch (error) {
    logger.error('Error fetching contact', {
      contactId,
      error: error.message
    });
    return null;
  }
}

/**
 * Extract all possible address data from contact properties
 * @param {Object} contact - HubSpot contact object
 * @returns {Object} Extracted address data
 */
function extractAddressData(contact) {
  const props = contact.properties || {};
  
  return {
    // Original address fields
    address: props.address || props.hs_street_address_1 || null,
    address2: props.address2 || props.hs_street_address_2 || null,
    city: props.city || props.hs_city || null,
    state: props.state || props.hs_state_region || null,
    zip: props.zip || props.postal_code || props.hs_postal_code || null,
    country: props.country || props.hs_country || null,
    country_code: props.country_code || null,
    
    // Unmessy processed fields (if they exist)
    um_house_number: props.um_house_number || null,
    um_street_name: props.um_street_name || null,
    um_street_type: props.um_street_type || null,
    um_street_direction: props.um_street_direction || null,
    um_unit_type: props.um_unit_type || null,
    um_unit_number: props.um_unit_number || null,
    um_city: props.um_city || null,
    um_state_province: props.um_state_province || null,
    um_country: props.um_country || null,
    um_country_code: props.um_country_code || null,
    um_postal_code: props.um_postal_code || null,
    um_address_status: props.um_address_status || null,
    um_address_line_1: props.um_address_line_1 || null,
    um_address_line_2: props.um_address_line_2 || null
  };
}

/**
 * Extract all possible phone data from contact properties
 * @param {Object} contact - HubSpot contact object
 * @returns {Object} Extracted phone data
 */
function extractPhoneData(contact) {
  const props = contact.properties || {};
  
  return {
    // Original phone fields
    phone: props.phone || null,
    mobilephone: props.mobilephone || null,
    hs_phone_number: props.hs_phone_number || null,
    phone_number: props.phone_number || null,
    mobile_phone_number: props.mobile_phone_number || null,
    work_phone: props.work_phone || null,
    home_phone: props.home_phone || null,
    cell_phone: props.cell_phone || null,
    
    // Unmessy processed fields (if they exist)
    um_phone1: props.um_phone1 || null,
    um_phone1_status: props.um_phone1_status || null,
    um_phone1_format: props.um_phone1_format || null,
    um_phone1_country_code: props.um_phone1_country_code || null,
    um_phone1_is_mobile: props.um_phone1_is_mobile || null,
    um_phone1_country: props.um_phone1_country || null,
    um_phone1_area_code: props.um_phone1_area_code || null,
    um_phone1_area: props.um_phone1_area || null,
    
    um_phone2: props.um_phone2 || null,
    um_phone2_status: props.um_phone2_status || null,
    um_phone2_format: props.um_phone2_format || null,
    um_phone2_country_code: props.um_phone2_country_code || null,
    um_phone2_is_mobile: props.um_phone2_is_mobile || null,
    um_phone2_country: props.um_phone2_country || null,
    um_phone2_area_code: props.um_phone2_area_code || null,
    um_phone2_area: props.um_phone2_area || null
  };
}

/**
 * Determine if address validation is needed
 * @param {Object} addressData - Extracted address data
 * @returns {boolean} Whether address validation is needed
 */
function determineAddressValidationNeeds(addressData) {
  // Has any original address data
  const hasOriginalAddress = !!(
    addressData.address || 
    addressData.city || 
    addressData.state || 
    addressData.zip ||
    addressData.country
  );
  
  // Missing Unmessy processed data
  const missingProcessedData = !(
    addressData.um_address_status && 
    addressData.um_city && 
    (addressData.um_address_line_1 || addressData.um_street_name)
  );
  
  return hasOriginalAddress && missingProcessedData;
}

/**
 * Determine if phone validation is needed
 * @param {Object} phoneData - Extracted phone data
 * @returns {boolean} Whether phone validation is needed
 */
function determinePhoneValidationNeeds(phoneData) {
  // Has any original phone data
  const hasOriginalPhone = !!(
    phoneData.phone || 
    phoneData.mobilephone ||
    phoneData.hs_phone_number ||
    phoneData.phone_number ||
    phoneData.mobile_phone_number
  );
  
  // Missing processed data for at least one phone
  const missingProcessedData = !(
    (phoneData.um_phone1 && phoneData.um_phone1_status && phoneData.um_phone1_format) ||
    (phoneData.um_phone2 && phoneData.um_phone2_status && phoneData.um_phone2_format)
  );
  
  return hasOriginalPhone && missingProcessedData;
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

    // Fetch contact data using client-specific API key with comprehensive properties
    const contact = await fetchContactData(event.objectId, clientConfig.apiKey);

    if (!contact) {
      logger.error('Failed to fetch contact', {
        eventId: event.eventId,
        contactId: event.objectId,
        clientId
      });
      return null;
    }

    // Extract comprehensive data
    const addressData = extractAddressData(contact);
    const phoneData = extractPhoneData(contact);
    
    // Determine validation needs
    const needsAddressValidation = determineAddressValidationNeeds(addressData);
    const needsPhoneValidation = determinePhoneValidationNeeds(phoneData);
    
    // Log validation assessment
    logger.info('Validation needs assessment', {
      eventId: event.eventId,
      contactId: event.objectId,
      needsAddressValidation,
      needsPhoneValidation,
      hasOriginalAddress: !!(addressData.address || addressData.city),
      hasOriginalPhone: !!(phoneData.phone || phoneData.mobilephone),
      hasProcessedAddress: !!addressData.um_address_status,
      hasProcessedPhone: !!(phoneData.um_phone1_status || phoneData.um_phone2_status)
    });

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
      
      // Include ALL address data in the queue item
      ...addressData,
      
      // Include ALL phone data in the queue item
      ...phoneData,
      
      // Determine validation types needed based on comprehensive analysis
      needs_email_validation: !!(contact.properties?.email && 
        (!contact.properties?.um_email || 
         !contact.properties?.um_email_status || 
         !contact.properties?.um_bounce_status)),
      
      needs_name_validation: !!(
        (contact.properties?.firstname || contact.properties?.lastname) &&
        (!contact.properties?.um_first_name || 
         !contact.properties?.um_last_name ||
         !contact.properties?.um_name_status)
      ),
      
      needs_phone_validation: needsPhoneValidation,
      needs_address_validation: needsAddressValidation
    };

    logger.info('Enriched event created', {
      eventId: event.eventId,
      validationFlags: {
        email: enrichedEvent.needs_email_validation,
        name: enrichedEvent.needs_name_validation,
        phone: enrichedEvent.needs_phone_validation,
        address: enrichedEvent.needs_address_validation
      }
    });

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