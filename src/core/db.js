// src/core/db.js
import { createClient } from '@supabase/supabase-js';
import pg from 'pg';
import { DatabaseError, DatabaseConnectionError, DatabaseTimeoutError } from './errors.js';
import { createServiceLogger } from './logger.js';

const logger = createServiceLogger('database');
const { Pool } = pg;

class DatabaseManager {
  constructor() {
    this.supabase = null;
    this.pool = null;
    this.config = this.loadConfig();
    this.isInitialized = false;
    this.connectionAttempts = 0;
    this.maxConnectionAttempts = 3;
  }

  loadConfig() {
    const config = {
      supabase: {
        url: process.env.SUPABASE_URL,
        key: process.env.SUPABASE_SERVICE_ROLE_KEY,
        options: {
          auth: {
            persistSession: false,
            autoRefreshToken: false
          },
          db: {
            schema: 'public'
          },
          global: {
            headers: {
              'X-Client-Info': 'unmessy-api'
            }
          }
        }
      },
      postgres: {
        connectionString: process.env.DATABASE_URL,
        max: parseInt(process.env.DB_POOL_SIZE || '20'),
        idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT || '30000'),
        connectionTimeoutMillis: parseInt(process.env.DB_CONNECTION_TIMEOUT || '10000'),
        statement_timeout: parseInt(process.env.DB_STATEMENT_TIMEOUT || '30000'),
        query_timeout: parseInt(process.env.DB_QUERY_TIMEOUT || '30000'),
        application_name: 'unmessy-api'
      }
    };

    // Validate configuration
    if (!config.supabase.url || !config.supabase.key) {
      throw new DatabaseError('Missing database configuration');
    }

    return config;
  }

  async initialize() {
    if (this.isInitialized) {
      return;
    }

    try {
      logger.info('Initializing database connections');

      // Initialize Supabase client
      this.supabase = createClient(
        this.config.supabase.url,
        this.config.supabase.key,
        this.config.supabase.options
      );

      // Initialize PostgreSQL connection pool if direct connection string is available
      if (this.config.postgres.connectionString) {
        this.pool = new Pool(this.config.postgres);
        
        // Test the connection
        await this.testConnection();
        
        // Set up error handlers
        this.pool.on('error', (err) => {
          logger.error('Unexpected pool error', err);
        });

        this.pool.on('connect', (client) => {
          logger.debug('New client connected to pool');
          // Set statement timeout for each client
          client.query(`SET statement_timeout = ${this.config.postgres.statement_timeout}`);
        });
      }

      this.isInitialized = true;
      logger.info('Database connections initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize database', error);
      throw new DatabaseConnectionError(error);
    }
  }

  async testConnection() {
    try {
      if (this.pool) {
        const result = await this.pool.query('SELECT NOW()');
        logger.debug('PostgreSQL connection test successful', { 
          timestamp: result.rows[0].now 
        });
      }

      // Test Supabase connection
      const { data, error } = await this.supabase
        .from('clients')
        .select('count')
        .limit(1);

      if (error) throw error;
      
      logger.debug('Supabase connection test successful');
      return true;
    } catch (error) {
      logger.error('Database connection test failed', error);
      throw new DatabaseConnectionError(error);
    }
  }

  // Get a client from the pool for transactions
  async getClient() {
    if (!this.pool) {
      throw new DatabaseError('Direct PostgreSQL connection not available');
    }
    return this.pool.connect();
  }

  // Execute a query with retry logic
  async query(text, params, options = {}) {
    const { 
      retries = 2, 
      timeout = this.config.postgres.query_timeout 
    } = options;

    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const start = Date.now();
        
        let result;
        if (this.pool) {
          // Use direct PostgreSQL connection
          result = await this.pool.query({
            text,
            values: params,
            query_timeout: timeout
          });
        } else {
          // Fallback to Supabase RPC or raw query
          throw new DatabaseError('Direct query not supported without PostgreSQL pool');
        }

        const duration = Date.now() - start;
        logger.debug('Query executed successfully', {
          query: text.substring(0, 100),
          duration,
          rowCount: result.rowCount
        });

        return result;
      } catch (error) {
        const isLastAttempt = attempt === retries;
        
        if (isLastAttempt || !this.isRetryableError(error)) {
          logger.error('Query failed', error, {
            query: text.substring(0, 100),
            attempt: attempt + 1,
            retries
          });
          throw new DatabaseError(`Query failed: ${error.message}`, 'query', error);
        }

        logger.warn('Query failed, retrying', {
          attempt: attempt + 1,
          error: error.message
        });

        // Wait before retry with exponential backoff
        await this.sleep(Math.pow(2, attempt) * 100);
      }
    }
  }

  // Transaction support
  async transaction(callback) {
    const client = await this.getClient();
    
    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // Validation-specific methods

  // Check if email exists in validation cache
  async getEmailValidation(email) {
    try {
      const { data, error } = await this.supabase
        .from('email_validations')
        .select('*')
        .eq('email', email)
        .single();

      if (error && error.code !== 'PGRST116') {
        throw error;
      }

      return data;
    } catch (error) {
      logger.error('Failed to get email validation', error, { email });
      throw new DatabaseError('Failed to retrieve email validation', 'select', error);
    }
  }

  // Save email validation result
  async saveEmailValidation(validationData) {
    try {
      // First, ensure contact exists
      const { data: contact, error: contactError } = await this.supabase
        .from('contacts')
        .insert({})
        .select('id')
        .single();

      if (contactError) throw contactError;

      // Save validation
      const { data, error } = await this.supabase
        .from('email_validations')
        .upsert({
          ...validationData,
          contact_id: contact.id,
          updated_at: new Date().toISOString()
        })
        .select()
        .single();

      if (error) throw error;

      return data;
    } catch (error) {
      logger.error('Failed to save email validation', error);
      throw new DatabaseError('Failed to save email validation', 'insert', error);
    }
  }

  // Get email validation rules
  async getEmailValidationRules(ruleType = null) {
    try {
      let query = this.supabase
        .from('email_validation_rules')
        .select('*')
        .eq('is_active', true);

      if (ruleType) {
        query = query.eq('rule_type', ruleType);
      }

      const { data, error } = await query;

      if (error) throw error;

      // Update usage statistics
      if (data && data.length > 0) {
        const ruleIds = data.map(r => r.id);
        await this.supabase
          .from('email_validation_rules')
          .update({ 
            last_used_at: new Date().toISOString(),
            usage_count: this.supabase.sql`usage_count + 1`
          })
          .in('id', ruleIds);
      }

      return data;
    } catch (error) {
      logger.error('Failed to get email validation rules', error, { ruleType });
      throw new DatabaseError('Failed to retrieve validation rules', 'select', error);
    }
  }

  // Client management methods

  async getClient(clientId) {
    try {
      const { data, error } = await this.supabase
        .from('clients')
        .select('*')
        .eq('client_id', parseInt(clientId))
        .single();

      if (error) throw error;

      return data;
    } catch (error) {
      logger.error('Failed to get client', error, { clientId });
      throw new DatabaseError('Failed to retrieve client', 'select', error);
    }
  }

  async validateApiKey(apiKey) {
    // This would typically be done via environment variables as shown in current implementation
    // But if API keys were stored in DB, it would look like this:
    try {
      const { data, error } = await this.supabase
        .from('api_keys')
        .select('client_id, active')
        .eq('key_hash', this.hashApiKey(apiKey))
        .single();

      if (error || !data || !data.active) {
        return { valid: false };
      }

      return { valid: true, clientId: data.client_id };
    } catch (error) {
      logger.error('Failed to validate API key', error);
      return { valid: false };
    }
  }

  // Rate limiting methods using atomic operations

  async checkRateLimit(clientId, validationType) {
    try {
      const columnMap = {
        email: 'remaining_email',
        name: 'remaining_name',
        phone: 'remaining_phone',
        address: 'remaining_address'
      };

      const column = columnMap[validationType];
      if (!column) {
        throw new Error(`Invalid validation type: ${validationType}`);
      }

      const { data, error } = await this.supabase
        .from('clients')
        .select(`${column}, daily_${validationType}_limit, active`)
        .eq('client_id', parseInt(clientId))
        .single();

      if (error) throw error;

      if (!data.active) {
        return { allowed: false, reason: 'inactive' };
      }

      return {
        allowed: data[column] > 0,
        remaining: data[column],
        limit: data[`daily_${validationType}_limit`]
      };
    } catch (error) {
      logger.error('Failed to check rate limit', error, { clientId, validationType });
      throw new DatabaseError('Failed to check rate limit', 'select', error);
    }
  }

  async decrementRateLimit(clientId, validationType) {
    try {
      // Use stored procedure for atomic decrement
      const { data, error } = await this.supabase
        .rpc('decrement_validation_count', {
          p_client_id: clientId,
          p_validation_type: validationType
        });

      if (error) throw error;

      return data; // Returns remaining count or -1 if failed
    } catch (error) {
      logger.error('Failed to decrement rate limit', error, { clientId, validationType });
      throw new DatabaseError('Failed to update rate limit', 'update', error);
    }
  }

  // Queue management methods

  async enqueueWebhookEvent(eventData) {
    try {
      const { data, error } = await this.supabase
        .from('hubspot_webhook_queue')
        .insert(eventData)
        .select()
        .single();

      if (error) throw error;

      logger.info('Webhook event enqueued', {
        eventId: data.event_id,
        eventType: data.event_type
      });

      return data;
    } catch (error) {
      logger.error('Failed to enqueue webhook event', error);
      throw new DatabaseError('Failed to enqueue event', 'insert', error);
    }
  }

  async getQueuedEvents(limit = 10) {
    try {
      const { data, error } = await this.supabase
        .from('hubspot_webhook_queue')
        .select('*')
        .eq('status', 'pending')
        .lt('attempts', 3)
        .order('created_at', { ascending: true })
        .limit(limit);

      if (error) throw error;

      return data || [];
    } catch (error) {
      logger.error('Failed to get queued events', error);
      throw new DatabaseError('Failed to retrieve queue', 'select', error);
    }
  }

  async updateQueueEvent(eventId, updates) {
    try {
      const { data, error } = await this.supabase
        .from('hubspot_webhook_queue')
        .update(updates)
        .eq('event_id', eventId)
        .select()
        .single();

      if (error) throw error;

      return data;
    } catch (error) {
      logger.error('Failed to update queue event', error, { eventId });
      throw new DatabaseError('Failed to update queue event', 'update', error);
    }
  }

  // Metrics and monitoring

  async recordValidationMetric(metricData) {
    try {
      await this.supabase.rpc('record_validation_metric', metricData);
    } catch (error) {
      // Don't throw - metrics are not critical
      logger.warn('Failed to record validation metric', { error: error.message });
    }
  }

  async logApiRequest(requestData) {
    try {
      await this.supabase
        .from('api_request_logs')
        .insert(requestData);
    } catch (error) {
      // Don't throw - logging is not critical
      logger.warn('Failed to log API request', { error: error.message });
    }
  }

  // Cleanup and maintenance

  async cleanup() {
    logger.info('Cleaning up database connections');
    
    if (this.pool) {
      await this.pool.end();
    }
    
    this.isInitialized = false;
  }

  // Helper methods

  isRetryableError(error) {
    const retryableCodes = [
      'ECONNREFUSED',
      'ETIMEDOUT',
      'ENOTFOUND',
      'ECONNRESET',
      '57P03', // cannot_connect_now
      '58000', // system_error
      '58P01', // undefined_file
      '40001', // serialization_failure
      '40P01'  // deadlock_detected
    ];

    return retryableCodes.includes(error.code) || 
           error.message.includes('connection') ||
           error.message.includes('timeout');
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  hashApiKey(apiKey) {
    // Implement secure hashing for API keys
    const crypto = require('crypto');
    return crypto.createHash('sha256').update(apiKey).digest('hex');
  }

  // Get database statistics
  async getStats() {
    const stats = {
      pool: null,
      connections: {
        total: 0,
        idle: 0,
        waiting: 0
      }
    };

    if (this.pool) {
      stats.pool = {
        totalCount: this.pool.totalCount,
        idleCount: this.pool.idleCount,
        waitingCount: this.pool.waitingCount
      };
    }

    return stats;
  }
}

// Create singleton instance
const db = new DatabaseManager();

// Initialize on first import
if (process.env.NODE_ENV !== 'test') {
  db.initialize().catch(error => {
    logger.error('Failed to initialize database on startup', error);
    process.exit(1);
  });
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, closing database connections');
  await db.cleanup();
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, closing database connections');
  await db.cleanup();
});

export default db;
export { DatabaseManager };