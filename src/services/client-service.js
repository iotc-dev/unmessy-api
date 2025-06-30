// src/services/client-service.js
import db from '../core/db.js';
import { config } from '../core/config.js';
import { createServiceLogger } from '../core/logger.js';
import { 
  AuthenticationError,
  DatabaseError, 
  ValidationError,
  RateLimitError 
} from '../core/errors.js';

const logger = createServiceLogger('client-service');

class ClientService {
  constructor() {
    this.logger = logger;
    this.clientCache = new Map();
    this.clientConfigCache = new Map();
    this.cacheTimeout = 300000; // 5 minutes
    
    // Cache for API key mappings
    this.apiKeyCache = new Map();
    
    // Initialize API keys from environment on startup
    this.loadApiKeysFromEnv();
  }
  
  // Load API keys from environment variables
  loadApiKeysFromEnv() {
    try {
      // Check if config.clients exists and has getAll method
      if (!config.clients || typeof config.clients.getAll !== 'function') {
        this.logger.warn('Config clients not properly initialized', {
          hasClients: !!config.clients,
          type: typeof config.clients
        });
        
        // Fallback: Try to load directly from environment
        for (let i = 1; i <= 10; i++) {
          const key = process.env[`CLIENT_${i}_KEY`];
          const id = process.env[`CLIENT_${i}_ID`];
          
          if (key && id) {
            this.apiKeyCache.set(key, id);
            this.logger.info(`Loaded client ${i} from env directly: ID=${id}`);
          }
        }
        
        return;
      }
      
      const clients = config.clients.getAll();
      
      for (const [apiKey, client] of clients) {
        this.apiKeyCache.set(apiKey, client.id || client);
        this.logger.info('API key loaded for client', { 
          clientId: client.id || client
        });
      }
      
      this.logger.info('API keys loaded from environment', {
        totalKeys: this.apiKeyCache.size
      });
    } catch (error) {
      this.logger.error('Failed to load API keys from environment', error);
      
      // Fallback: Try to load directly from environment
      try {
        for (let i = 1; i <= 10; i++) {
          const key = process.env[`CLIENT_${i}_KEY`];
          const id = process.env[`CLIENT_${i}_ID`];
          
          if (key && id) {
            this.apiKeyCache.set(key, id);
            this.logger.info(`Loaded client ${i} from env directly: ID=${id}`);
          }
        }
      } catch (fallbackError) {
        this.logger.error('Fallback loading also failed', fallbackError);
      }
    }
  }
  
  // Validate API key and return client ID
  async validateApiKey(apiKey) {
    if (!apiKey) {
      return {
        valid: false,
        error: 'API key is required'
      };
    }
    
    // Check memory cache first
    const clientId = this.apiKeyCache.get(apiKey);
    
    if (!clientId) {
      this.logger.warn('API key not found in cache', {
        providedKeyPrefix: apiKey.substring(0, 4) + '****',
        cachedKeys: this.apiKeyCache.size,
        availableKeys: Array.from(this.apiKeyCache.keys()).map(k => k.substring(0, 4) + '****')
      });
      
      return {
        valid: false,
        error: 'Invalid API key'
      };
    }
    
    return {
      valid: true,
      clientId: clientId
    };
  }
  
  // Get client by ID
  async getClient(clientId) {
    try {
      // Check cache first
      const cacheKey = `client:${clientId}`;
      const cached = this.getFromCache(cacheKey);
      if (cached) {
        return cached;
      }
      
      // Query database
      const { rows } = await db.select('clients', 
        { client_id: clientId },
        { limit: 1 }
      );
      
      if (rows.length === 0) {
        this.logger.warn('Client not found', { clientId });
        return null;
      }
      
      const client = rows[0];
      
      // Cache the result
      this.setInCache(cacheKey, client);
      
      return client;
    } catch (error) {
      this.logger.error('Failed to get client', error, { clientId });
      throw new DatabaseError('Failed to retrieve client', 'select', error);
    }
  }
  
  // Get client by API key (combined validation and retrieval)
  async getClientByApiKey(apiKey) {
    try {
      // First validate the API key
      const validation = await this.validateApiKey(apiKey);
      
      if (!validation.valid) {
        return null;
      }
      
      // Get full client data from database
      const client = await this.getClient(validation.clientId);
      
      if (!client) {
        this.logger.error('Client configured but not found in database', {
          clientId: validation.clientId
        });
        return null;
      }
      
      // Verify client is active
      if (!client.active) {
        this.logger.warn('Inactive client attempted access', {
          clientId: client.client_id,
          name: client.name
        });
        return null;
      }
      
      return client;
    } catch (error) {
      this.logger.error('Failed to get client by API key', error);
      throw new DatabaseError('Failed to authenticate client', 'select', error);
    }
  }
  
  // Check if client has HubSpot enabled
  async hasHubSpotEnabled(clientId) {
    try {
      const client = await this.getClient(clientId);
      return client?.hubspot_enabled || false;
    } catch (error) {
      this.logger.error('Failed to check HubSpot status', error, { clientId });
      return false;
    }
  }
  
  // Get client's HubSpot configuration
  async getClientHubSpotConfig(clientId) {
    try {
      // Check cache first
      const cacheKey = `hubspot:${clientId}`;
      const cached = this.getFromCache(cacheKey);
      if (cached) {
        return cached;
      }
      
      const { rows } = await db.select('clients',
        { client_id: clientId },
        {
          columns: 'client_id, hubspot_enabled, hubspot_private_key, hubspot_portal_id, hubspot_form_guid, hubspot_webhook_secret',
          limit: 1
        }
      );
      
      if (rows.length === 0) {
        return null;
      }
      
      const data = rows[0];
      const config = {
        apiKey: data.hubspot_private_key,
        portalId: data.hubspot_portal_id,
        formGuid: data.hubspot_form_guid,
        webhookSecret: data.hubspot_webhook_secret,
        enabled: data.hubspot_enabled
      };
      
      // Cache the configuration
      this.setInCache(cacheKey, config);
      
      return config;
    } catch (error) {
      this.logger.error('Failed to get client HubSpot config', error, { clientId });
      return null;
    }
  }
  
  // Check rate limit for specific validation type
  async checkRateLimit(clientId, validationType, count = 1) {
    try {
      const client = await this.getClient(clientId);
      
      if (!client) {
        throw new DatabaseError('Client not found');
      }
      
      // Get the appropriate limit and remaining fields
      const limitField = `daily_${validationType}_limit`;
      const remainingField = `remaining_${validationType}`;
      
      const limit = client[limitField] || 0;
      const remaining = client[remainingField] || 0;
      
      const allowed = remaining >= count;
      
      return {
        allowed,
        remaining,
        limit,
        limited: !allowed
      };
    } catch (error) {
      if (error instanceof RateLimitError) {
        throw error;
      }
      
      this.logger.error('Failed to check rate limit', error, {
        clientId,
        validationType
      });
      
      // On error, allow the request but log warning
      return {
        allowed: true,
        remaining: -1,
        limit: -1,
        limited: false
      };
    }
  }
  
  // Increment usage counter (atomic operation)
  async incrementUsage(clientId, validationType, count = 1) {
    try {
      // Use the database function via RPC
      const result = await db.rpc('decrement_validation_count', {
        p_client_id: clientId,
        p_validation_type: validationType
      });
      
      // The function returns the remaining count or -1 on error
      const remaining = result ?? -1;
      
      if (remaining === -1) {
        this.logger.warn('Failed to decrement rate limit', {
          clientId,
          validationType
        });
      } else {
        this.logger.debug('Usage incremented', {
          clientId,
          validationType,
          remaining
        });
      }
      
      // Invalidate cache to ensure fresh data
      this.invalidateCache(clientId);
      
      return remaining;
    } catch (error) {
      this.logger.error('Failed to increment usage', error, {
        clientId,
        validationType
      });
      return -1;
    }
  }
  
  // Record validation metric
  async recordValidationMetric(clientId, validationType, success, responseTime, errorType = null) {
    try {
      // Use RPC to call the database function
      await db.rpc('record_validation_metric', {
        p_client_id: clientId,
        p_validation_type: validationType,
        p_success: success,
        p_response_time_ms: responseTime,
        p_error_type: errorType
      });
      
      this.logger.debug('Validation metric recorded', {
        clientId,
        validationType,
        success,
        responseTime
      });
    } catch (error) {
      // Don't throw - metrics are non-critical
      this.logger.error('Failed to record metric', error, {
        clientId,
        validationType,
        success,
        responseTime
      });
    }
  }
  
  // Get comprehensive client statistics
  async getClientStats(clientId) {
    try {
      const client = await this.getClient(clientId);
      
      if (!client) {
        return null;
      }
      
      // Calculate today's usage
      const todayUsage = {
        email: client.daily_email_limit - client.remaining_email,
        name: client.daily_name_limit - client.remaining_name,
        phone: client.daily_phone_limit - client.remaining_phone,
        address: client.daily_address_limit - client.remaining_address
      };
      
      return {
        clientId: client.client_id,
        name: client.name,
        active: client.active,
        um_account_type: client.um_account_type,
        dailyEmailLimit: client.daily_email_limit,
        remainingEmail: client.remaining_email,
        emailCount: todayUsage.email,
        totalEmailCount: client.total_email_count,
        dailyNameLimit: client.daily_name_limit,
        remainingName: client.remaining_name,
        nameCount: todayUsage.name,
        totalNameCount: client.total_name_count,
        dailyPhoneLimit: client.daily_phone_limit,
        remainingPhone: client.remaining_phone,
        phoneCount: todayUsage.phone,
        totalPhoneCount: client.total_phone_count,
        dailyAddressLimit: client.daily_address_limit,
        remainingAddress: client.remaining_address,
        addressCount: todayUsage.address,
        totalAddressCount: client.total_address_count,
        lastResetDate: client.last_reset_date,
        createdAt: client.created_at,
        updatedAt: client.updated_at
      };
    } catch (error) {
      this.logger.error('Failed to get client stats', error, { clientId });
      throw new DatabaseError('Failed to retrieve client statistics', 'select', error);
    }
  }
  
  // Update client settings
  async updateClient(clientId, updates) {
    try {
      const allowedUpdates = [
        'name', 'active', 'daily_email_limit', 'daily_name_limit',
        'daily_phone_limit', 'daily_address_limit', 'um_account_type',
        'hubspot_enabled', 'hubspot_private_key', 'hubspot_portal_id',
        'hubspot_form_guid', 'hubspot_webhook_secret'
      ];
      
      // Filter out any disallowed fields
      const filteredUpdates = {};
      Object.keys(updates).forEach(key => {
        if (allowedUpdates.includes(key)) {
          filteredUpdates[key] = updates[key];
        }
      });
      
      if (Object.keys(filteredUpdates).length === 0) {
        throw new ValidationError('No valid fields to update');
      }
      
      // Add updated_at timestamp
      filteredUpdates.updated_at = new Date().toISOString();
      
      const { rows } = await db.update(
        'clients',
        filteredUpdates,
        { client_id: clientId },
        { returning: true }
      );
      
      if (rows.length === 0) {
        throw new DatabaseError('Client not found');
      }
      
      // Invalidate cache
      this.invalidateCache(clientId);
      
      this.logger.info('Client updated', { clientId, updates: Object.keys(filteredUpdates) });
      
      return rows[0];
    } catch (error) {
      this.logger.error('Failed to update client', error, { clientId });
      throw new DatabaseError('Failed to update client', 'update', error);
    }
  }
  
  // Reset daily limits for all clients
  async resetDailyLimits() {
    try {
      // Call the stored procedure to reset limits using RPC
      await db.rpc('reset_remaining_counts', {});
      
      // Clear all caches
      this.clientCache.clear();
      this.clientConfigCache.clear();
      
      this.logger.info('Daily limits reset for all clients');
      
      return { success: true };
    } catch (error) {
      this.logger.error('Failed to reset daily limits', error);
      throw new DatabaseError('Failed to reset daily limits', 'update', error);
    }
  }
  
  // Get all active clients
  async getAllActiveClients() {
    try {
      const { rows } = await db.select('clients',
        { active: true },
        {
          columns: 'client_id, name, um_account_type, daily_email_limit, remaining_email, daily_name_limit, remaining_name, daily_phone_limit, remaining_phone, daily_address_limit, remaining_address, created_at',
          order: { column: 'client_id', ascending: true }
        }
      );
      
      return rows;
    } catch (error) {
      this.logger.error('Failed to get all clients', error);
      throw new DatabaseError('Failed to retrieve clients', 'select', error);
    }
  }
  
  // Cache management methods
  getFromCache(key) {
    const cached = this.clientCache.get(key);
    if (cached && (Date.now() - cached.timestamp < this.cacheTimeout)) {
      return cached.data;
    }
    this.clientCache.delete(key);
    return null;
  }
  
  setInCache(key, data) {
    this.clientCache.set(key, {
      data,
      timestamp: Date.now()
    });
  }
  
  invalidateCache(clientId) {
    // Remove all cache entries for this client
    const keysToDelete = [];
    for (const key of this.clientCache.keys()) {
      if (key.includes(clientId)) {
        keysToDelete.push(key);
      }
    }
    keysToDelete.forEach(key => this.clientCache.delete(key));
    
    // Also clear from config cache
    this.clientConfigCache.delete(clientId);
  }
  
  // Health check
  async healthCheck() {
    try {
      // Try to get a client to verify database connection
      const { rows } = await db.select('clients', {}, { limit: 1 });
      
      return {
        status: 'healthy',
        clientCount: rows.length,
        cacheSize: this.clientCache.size,
        apiKeysLoaded: this.apiKeyCache.size
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
const clientService = new ClientService();

// Export both the instance and the class
export { clientService as default, ClientService };