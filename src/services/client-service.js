// src/services/client-service.js
import db from '../core/db.js';
import { config } from '../core/config.js';
import { createServiceLogger } from '../core/logger.js';
import { 
  AuthenticationError, 
  RateLimitError, 
  DatabaseError,
  ValidationError 
} from '../core/errors.js';

const logger = createServiceLogger('client-service');

class ClientService {
  constructor() {
    this.logger = logger;
    
    // Cache for client data (TTL: 5 minutes)
    this.clientCache = new Map();
    this.cacheTTL = 5 * 60 * 1000; // 5 minutes
    
    // Cache for API key mappings
    this.apiKeyCache = new Map();
    
    // Initialize API keys from environment on startup
    this.loadApiKeysFromEnv();
  }
  
  // Load API keys from environment variables
  loadApiKeysFromEnv() {
    try {
      const clients = config.clients.getAll();
      
      for (const [apiKey, clientId] of clients) {
        this.apiKeyCache.set(apiKey, clientId);
        this.logger.info('API key loaded for client', { clientId });
      }
      
      this.logger.info('API keys loaded from environment', {
        totalKeys: this.apiKeyCache.size
      });
    } catch (error) {
      this.logger.error('Failed to load API keys from environment', error);
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
    const cachedClientId = this.apiKeyCache.get(apiKey);
    if (!cachedClientId) {
      return {
        valid: false,
        error: 'Invalid API key'
      };
    }
    
    return {
      valid: true,
      clientId: cachedClientId
    };
  }
  
  // Get client by ID with caching
  async getClient(clientId) {
    // Check cache first
    const cached = this.getFromCache(clientId);
    if (cached) {
      return cached;
    }
    
    try {
      // Use Supabase query
      const { rows } = await db.select('clients', 
        { client_id: clientId },
        { limit: 1 }
      );
      
      const client = rows[0];
      
      if (client) {
        // Add to cache
        this.addToCache(clientId, client);
      }
      
      return client;
    } catch (error) {
      this.logger.error('Failed to get client', error, { clientId });
      throw new DatabaseError('Failed to retrieve client data', 'select', error);
    }
  }
  
  // Get client HubSpot configuration
  async getClientHubSpotConfig(clientId) {
    try {
      const client = await this.getClient(clientId);
      
      if (!client) {
        return null;
      }
      
      return {
        enabled: client.hubspot_enabled,
        apiKey: client.hubspot_private_key,
        portalId: client.hubspot_portal_id,
        formGuid: client.hubspot_form_guid,
        webhookSecret: client.hubspot_webhook_secret
      };
    } catch (error) {
      this.logger.error('Failed to get HubSpot config', error, { clientId });
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
      // Use the decrement function from database
      const { rows } = await db.query('decrement_validation_count', {
        p_client_id: clientId,
        p_validation_type: validationType
      });
      
      const remaining = rows?.[0] || -1;
      
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
      await db.query('record_validation_metric', {
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
        'daily_phone_limit', 'daily_address_limit', 'hubspot_enabled',
        'hubspot_private_key', 'hubspot_portal_id', 'hubspot_form_guid',
        'hubspot_webhook_secret', 'is_admin'
      ];
      
      // Filter to allowed fields only
      const filteredUpdates = {};
      for (const [key, value] of Object.entries(updates)) {
        if (allowedUpdates.includes(key)) {
          filteredUpdates[key] = value;
        }
      }
      
      if (Object.keys(filteredUpdates).length === 0) {
        throw new ValidationError('No valid update fields provided');
      }
      
      const { rows } = await db.update(
        'clients',
        filteredUpdates,
        { client_id: clientId },
        { returning: true }
      );
      
      const result = rows[0];
      
      // Invalidate cache
      this.invalidateCache(clientId);
      
      this.logger.info('Client updated', {
        clientId,
        updates: Object.keys(filteredUpdates)
      });
      
      return result;
    } catch (error) {
      this.logger.error('Failed to update client', error, { clientId });
      throw new DatabaseError('Failed to update client', 'update', error);
    }
  }
  
  // Create a new client
  async createClient(clientData) {
    try {
      // Generate a new client ID (you might want to implement a better ID generation strategy)
      const maxIdResult = await db.query('SELECT MAX(client_id) as max_id FROM clients');
      const newClientId = (maxIdResult.rows[0]?.max_id || 0) + 1;
      
      const newClient = {
        client_id: newClientId,
        name: clientData.name,
        active: clientData.active !== undefined ? clientData.active : true,
        daily_email_limit: clientData.daily_email_limit || 1000,
        daily_name_limit: clientData.daily_name_limit || 1000,
        daily_phone_limit: clientData.daily_phone_limit || 1000,
        daily_address_limit: clientData.daily_address_limit || 1000,
        remaining_email: clientData.daily_email_limit || 1000,
        remaining_name: clientData.daily_name_limit || 1000,
        remaining_phone: clientData.daily_phone_limit || 1000,
        remaining_address: clientData.daily_address_limit || 1000,
        hubspot_enabled: clientData.hubspot_enabled || false,
        hubspot_private_key: clientData.hubspot_private_key || null,
        hubspot_portal_id: clientData.hubspot_portal_id || null,
        hubspot_form_guid: clientData.hubspot_form_guid || null,
        hubspot_webhook_secret: clientData.hubspot_webhook_secret || null,
        is_admin: clientData.is_admin || false
      };
      
      const { rows } = await db.insert('clients', newClient, { returning: true });
      
      this.logger.info('Client created', {
        clientId: newClientId,
        name: clientData.name
      });
      
      return rows[0];
    } catch (error) {
      this.logger.error('Failed to create client', error);
      throw new DatabaseError('Failed to create client', 'insert', error);
    }
  }
  
  // Deactivate a client (soft delete)
  async deactivateClient(clientId) {
    try {
      const { rows } = await db.update(
        'clients',
        { active: false },
        { client_id: clientId },
        { returning: true }
      );
      
      if (rows.length === 0) {
        return null;
      }
      
      // Invalidate cache
      this.invalidateCache(clientId);
      
      this.logger.info('Client deactivated', { clientId });
      
      return rows[0];
    } catch (error) {
      this.logger.error('Failed to deactivate client', error, { clientId });
      throw new DatabaseError('Failed to deactivate client', 'update', error);
    }
  }
  
  // List all clients with pagination
  async listClients(page = 1, limit = 20) {
    try {
      const offset = (page - 1) * limit;
      
      // Get total count
      const countResult = await db.query('SELECT COUNT(*) as total FROM clients');
      const total = parseInt(countResult.rows[0].total);
      
      // Get paginated results
      const { rows } = await db.query(
        `SELECT * FROM clients 
         ORDER BY client_id ASC 
         LIMIT $1 OFFSET $2`,
        [limit, offset]
      );
      
      return {
        clients: rows,
        pagination: {
          page,
          limit,
          total,
          pages: Math.ceil(total / limit)
        }
      };
    } catch (error) {
      this.logger.error('Failed to list clients', error);
      throw new DatabaseError('Failed to list clients', 'select', error);
    }
  }
  
  // Get count of HubSpot enabled clients
  async getHubSpotEnabledClientsCount() {
    try {
      const { rows } = await db.query(
        'SELECT COUNT(*) as count FROM clients WHERE hubspot_enabled = true AND active = true'
      );
      return parseInt(rows[0].count);
    } catch (error) {
      this.logger.error('Failed to count HubSpot enabled clients', error);
      return 0;
    }
  }
  
  // Reset rate limits for a specific client
  async resetRateLimits(clientId) {
    try {
      const client = await this.getClient(clientId);
      if (!client) {
        return null;
      }
      
      const { rows } = await db.update(
        'clients',
        {
          remaining_email: client.daily_email_limit,
          remaining_name: client.daily_name_limit,
          remaining_phone: client.daily_phone_limit,
          remaining_address: client.daily_address_limit,
          last_reset_date: new Date()
        },
        { client_id: clientId },
        { returning: true }
      );
      
      // Invalidate cache
      this.invalidateCache(clientId);
      
      this.logger.info('Rate limits reset for client', { clientId });
      
      return rows[0];
    } catch (error) {
      this.logger.error('Failed to reset rate limits', error, { clientId });
      throw new DatabaseError('Failed to reset rate limits', 'update', error);
    }
  }
  
  // Reset all client rate limits (called by cron job)
  async resetAllRateLimits() {
    try {
      const { rowCount } = await db.query('SELECT reset_remaining_counts()');
      
      // Clear all caches
      this.clearCaches();
      
      this.logger.info('All client rate limits reset', { count: rowCount });
      
      return { count: rowCount };
    } catch (error) {
      this.logger.error('Failed to reset all rate limits', error);
      throw new DatabaseError('Failed to reset all rate limits', 'function', error);
    }
  }
  
  // Cache management methods
  getFromCache(clientId) {
    const cached = this.clientCache.get(clientId);
    if (!cached) return null;
    
    // Check if expired
    if (Date.now() - cached.timestamp > this.cacheTTL) {
      this.clientCache.delete(clientId);
      return null;
    }
    
    return cached.data;
  }
  
  addToCache(clientId, data) {
    this.clientCache.set(clientId, {
      data,
      timestamp: Date.now()
    });
  }
  
  invalidateCache(clientId) {
    this.clientCache.delete(clientId);
  }
  
  // Clear all caches
  clearCaches() {
    this.clientCache.clear();
    this.logger.info('Client cache cleared');
  }
  
  // Health check
  async healthCheck() {
    try {
      // Try to get a client to test DB connection
      const testResult = await db.select('clients', { client_id: '1' }, { limit: 1 });
      
      return {
        status: 'healthy',
        cacheSize: this.clientCache.size,
        apiKeysLoaded: this.apiKeyCache.size,
        dbConnected: true
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        error: error.message,
        cacheSize: this.clientCache.size,
        apiKeysLoaded: this.apiKeyCache.size,
        dbConnected: false
      };
    }
  }
}

// Create singleton instance
const clientService = new ClientService();

// Export both the instance and the class
export { clientService as default, ClientService };