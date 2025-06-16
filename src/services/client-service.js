// src/services/client-service.js
import { db } from '../core/db.js';
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
      
      for (const [clientId, clientConfig] of clients) {
        if (clientConfig.apiKey) {
          this.apiKeyCache.set(clientConfig.apiKey, clientId);
          this.logger.info('API key loaded for client', { clientId });
        }
      }
      
      this.logger.info('API keys loaded from environment', {
        totalKeys: this.apiKeyCache.size
      });
    } catch (error) {
      this.logger.error('Failed to load API keys from environment', error);
    }
  }
  
  // Validate API key and return client data
  async validateApiKey(apiKey) {
    if (!apiKey) {
      throw new AuthenticationError('API key is required');
    }
    
    // Check memory cache first
    const cachedClientId = this.apiKeyCache.get(apiKey);
    if (!cachedClientId) {
      throw new AuthenticationError('Invalid API key');
    }
    
    // Get full client data
    const client = await this.getClient(cachedClientId);
    
    if (!client) {
      throw new AuthenticationError('Client not found');
    }
    
    if (!client.active) {
      throw new AuthenticationError('Client account is inactive');
    }
    
    return client;
  }
  
  // Get client by ID with caching
  async getClient(clientId) {
    // Check cache first
    const cached = this.getFromCache(clientId);
    if (cached) {
      return cached;
    }
    
    try {
      const client = await db.getClient(clientId);
      
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
  
  // Check rate limit for specific validation type
  async checkRateLimit(clientId, validationType) {
    try {
      const result = await db.checkRateLimit(clientId, validationType);
      
      if (!result.allowed) {
        const error = new RateLimitError(
          `${validationType} validation rate limit exceeded`,
          validationType,
          result.limit || 0,
          result.limit || 0,
          0
        );
        
        this.logger.warn('Rate limit exceeded', {
          clientId,
          validationType,
          limit: result.limit,
          remaining: result.remaining
        });
        
        throw error;
      }
      
      return {
        allowed: true,
        remaining: result.remaining,
        limit: result.limit,
        limited: false,
        used: result.limit - result.remaining
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
        limited: false,
        used: 0
      };
    }
  }
  
  // Increment usage counter (atomic operation)
  async incrementUsage(clientId, validationType) {
    try {
      const remaining = await db.decrementRateLimit(clientId, validationType);
      
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
  
  // Get comprehensive client statistics
  async getClientStats(clientId) {
    try {
      const client = await this.getClient(clientId);
      
      if (!client) {
        return null;
      }
      
      // Get today's metrics
      const metrics = await db.getClientMetrics(clientId, new Date());
      
      return {
        clientId: client.client_id,
        name: client.name,
        active: client.active,
        limits: {
          email: client.daily_email_limit,
          name: client.daily_name_limit,
          phone: client.daily_phone_limit,
          address: client.daily_address_limit
        },
        remaining: {
          email: client.remaining_email,
          name: client.remaining_name,
          phone: client.remaining_phone,
          address: client.remaining_address
        },
        totalUsage: {
          email: client.total_email_count,
          name: client.total_name_count,
          phone: client.total_phone_count,
          address: client.total_address_count
        },
        todayMetrics: metrics,
        lastResetDate: client.last_reset_date,
        createdAt: client.created_at,
        updatedAt: client.updated_at
      };
    } catch (error) {
      this.logger.error('Failed to get client stats', error, { clientId });
      throw new DatabaseError('Failed to retrieve client statistics', 'select', error);
    }
  }
  
  // Get HubSpot configuration for client
  async getHubSpotConfig(clientId) {
    try {
      const client = await this.getClient(clientId);
      
      if (!client) {
        return null;
      }
      
      return {
        enabled: client.hubspot_enabled,
        validationTypes: {
          email: client.hubspot_validate_email,
          name: client.hubspot_validate_name,
          phone: client.hubspot_validate_phone,
          address: client.hubspot_validate_address
        },
        mapping: client.hubspot_field_mapping || {},
        webhookSecret: client.hubspot_webhook_secret
      };
    } catch (error) {
      this.logger.error('Failed to get HubSpot config', error, { clientId });
      return null;
    }
  }
  
  // Update client settings
  async updateClient(clientId, updates) {
    try {
      const allowedUpdates = [
        'name', 'active', 'daily_email_limit', 'daily_name_limit',
        'daily_phone_limit', 'daily_address_limit', 'hubspot_enabled',
        'hubspot_validate_email', 'hubspot_validate_name',
        'hubspot_validate_phone', 'hubspot_validate_address',
        'hubspot_field_mapping'
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
      
      const result = await db.updateClient(clientId, filteredUpdates);
      
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
  
  // List all clients with their stats
  async listClients() {
    try {
      const clients = await db.getAllClients();
      
      // Get stats for each client
      const clientsWithStats = await Promise.all(
        clients.map(async (client) => {
          try {
            const stats = await this.getClientStats(client.client_id);
            return stats;
          } catch (error) {
            this.logger.error('Failed to get stats for client', error, {
              clientId: client.client_id
            });
            return {
              clientId: client.client_id,
              name: client.name,
              active: client.active,
              error: 'Failed to load stats'
            };
          }
        })
      );
      
      return clientsWithStats;
    } catch (error) {
      this.logger.error('Failed to list clients', error);
      throw new DatabaseError('Failed to list clients', 'select', error);
    }
  }
  
  // Record validation metric
  async recordMetric(clientId, validationType, success, responseTime, errorType = null) {
    try {
      await db.recordValidationMetric(
        clientId,
        validationType,
        success,
        responseTime,
        errorType
      );
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
      const testClient = await db.getClient('0001');
      
      return {
        status: 'healthy',
        cacheSize: this.clientCache.size,
        apiKeysLoaded: this.apiKeyCache.size,
        dbConnected: !!testClient
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
export { clientService, ClientService };