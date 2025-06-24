// src/core/db.js
import { createClient } from '@supabase/supabase-js';
import { config } from './config.js';
import { createServiceLogger } from './logger.js';

// Create logger instance
const logger = createServiceLogger('database');

// Connection pool management
let supabaseClient = null;
let isInitialized = false;
let lastUsedTimestamp = Date.now();

/**
 * Create a new Supabase client with optimized settings
 * @returns {Object} Supabase client
 */
const createConnection = () => {
  try {
    logger.debug('Creating new Supabase client');
    
    const client = createClient(
      config.database.url,
      config.database.key,
      {
        auth: {
          persistSession: false // Don't persist session in serverless environment
        },
        realtime: {
          enabled: false // Disable realtime subscription to reduce connection overhead
        },
        db: {
          schema: 'public'
        },
        global: {
          // Adjust fetch options for better timeout handling in serverless
          fetch: (url, options) => {
            return fetch(url, {
              ...options,
              signal: AbortSignal.timeout(config.database.connectionTimeoutMs)
            });
          }
        }
      }
    );
    
    return client;
  } catch (error) {
    logger.error('Failed to create database connection', error);
    throw error;
  }
};

/**
 * Get or create database connection
 * Optimized for both serverless and traditional environments
 * @returns {Object} Supabase client
 */
const getConnection = () => {
  // If in serverless mode, check if connection is stale
  if (config.isVercel) {
    // Create new connection if needed
    if (!supabaseClient || (Date.now() - lastUsedTimestamp > 10000)) {
      if (supabaseClient) {
        logger.debug('Creating new database connection (previous connection stale)');
      }
      supabaseClient = createConnection();
    }
  } else {
    // In traditional mode, maintain a persistent connection
    if (!supabaseClient) {
      supabaseClient = createConnection();
    }
  }
  
  lastUsedTimestamp = Date.now();
  return supabaseClient;
};

/**
 * Database interface with connection management
 */
export const db = {
  /**
   * Initialize database connection
   * @returns {Promise<boolean>} Success status
   */
  initialize: async () => {
    if (!isInitialized) {
      logger.info('Initializing database connection');
      
      try {
        // Create initial connection
        supabaseClient = createConnection();
        
        // Test connection with a simple query
        const { data, error } = await supabaseClient
          .from('clients')
          .select('id')
          .limit(1);
          
        if (error) throw error;
        
        isInitialized = true;
        logger.info('Database connection successful');
      } catch (error) {
        logger.error('Database initialization failed', error);
        throw error;
      }
    }
    
    return true;
  },
  
  /**
   * Clean up database connections
   * @returns {Promise<boolean>} Success status
   */
  cleanup: async () => {
    logger.info('Cleaning up database connections');
    
    // Supabase client doesn't have a formal close method,
    // so we just dereference it to allow garbage collection
    supabaseClient = null;
    isInitialized = false;
    
    return true;
  },
  
  /**
   * Get the timestamp of last database usage
   * @returns {number} Timestamp
   */
  getLastUsedTimestamp: () => {
    return lastUsedTimestamp;
  },
  
  /**
   * Execute a query function with retry logic
   * @param {Function} queryFn - Function that takes a Supabase client and executes a query
   * @param {number} retries - Number of retries on failure
   * @returns {Promise<any>} Query result
   */
  executeWithRetry: async (queryFn, retries = 3) => {
    let attempts = 0;
    let lastError = null;
    
    while (attempts < retries) {
      try {
        const connection = getConnection();
        const result = await queryFn(connection);
        return result;
      } catch (error) {
        lastError = error;
        attempts++;
        
        // Check if we should retry based on error type
        const shouldRetry = error.code === 'ECONNRESET' || 
                           error.code === 'ETIMEDOUT' ||
                           error.message.includes('connection');
        
        // Log retry attempts
        if (attempts < retries && shouldRetry) {
          logger.warn(`Database query failed, retrying (${attempts}/${retries})`, {
            error: error.message,
            code: error.code
          });
          
          // Exponential backoff with jitter
          const backoffTime = Math.min(
            200 * Math.pow(2, attempts) + Math.random() * 100,
            2000
          );
          
          await new Promise(resolve => setTimeout(resolve, backoffTime));
        } else if (!shouldRetry) {
          // Don't retry for non-connection errors
          logger.error('Database query failed with non-retryable error', error);
          break;
        }
      }
    }
    
    logger.error(`Database query failed after ${attempts} attempts`, lastError);
    throw lastError;
  },
  
  /**
   * Execute a SQL query with parameters
   * @param {string} text - SQL query text or function name for RPC
   * @param {Array|Object} params - Query parameters
   * @returns {Promise<Object>} Query result
   */
  query: async (text, params = []) => {
    return db.executeWithRetry(async (supabase) => {
      const { data, error, count } = await supabase.rpc(text, params);
      
      if (error) throw error;
      
      return {
        rows: data || [],
        rowCount: count || 0
      };
    });
  },
  
  /**
   * Insert data into a table
   * @param {string} table - Table name
   * @param {Object|Array} data - Data to insert
   * @param {Object} options - Insert options
   * @returns {Promise<Object>} Insert result
   */
  insert: async (table, data, options = {}) => {
    return db.executeWithRetry(async (supabase) => {
      const query = supabase.from(table).insert(data);
      
      // Apply options
      if (options.returning) {
        let selectColumns;
        
        if (options.returning === true) {
          selectColumns = '*';
        } else if (Array.isArray(options.returning)) {
          // Join array elements into a comma-separated string
          selectColumns = options.returning.join(',');
        } else {
          // Assume it's already a string
          selectColumns = options.returning;
        }
        
        query.select(selectColumns);
      }
      
      const { data: result, error } = await query;
      
      if (error) throw error;
      
      return { rows: result || [] };
    });
  },
  
  /**
   * Update data in a table
   * @param {string} table - Table name
   * @param {Object} data - Data to update
   * @param {Object} conditions - Where conditions
   * @param {Object} options - Update options
   * @returns {Promise<Object>} Update result
   */
  update: async (table, data, conditions, options = {}) => {
    return db.executeWithRetry(async (supabase) => {
      let query = supabase.from(table).update(data);
      
      // Apply conditions
      Object.entries(conditions).forEach(([column, value]) => {
        query = query.eq(column, value);
      });
      
      // Apply options
      if (options.returning) {
        let selectColumns;
        
        if (options.returning === true) {
          selectColumns = '*';
        } else if (Array.isArray(options.returning)) {
          // Join array elements into a comma-separated string
          selectColumns = options.returning.join(',');
        } else {
          // Assume it's already a string
          selectColumns = options.returning;
        }
        
        query.select(selectColumns);
      }
      
      const { data: result, error } = await query;
      
      if (error) throw error;
      
      return { rows: result || [] };
    });
  },
  
  /**
   * Select data from a table
   * @param {string} table - Table name
   * @param {Object} conditions - Where conditions
   * @param {Object} options - Select options
   * @returns {Promise<Object>} Select result
   */
  select: async (table, conditions = {}, options = {}) => {
    return db.executeWithRetry(async (supabase) => {
      let query = supabase.from(table).select(options.columns || '*');
      
      // Apply conditions
      Object.entries(conditions).forEach(([column, value]) => {
        query = query.eq(column, value);
      });
      
      // Apply limits
      if (options.limit) {
        query = query.limit(options.limit);
      }
      
      // Apply ordering
      if (options.order) {
        const { column, ascending = true } = options.order;
        query = query.order(column, { ascending });
      }
      
      const { data, error, count } = await query;
      
      if (error) throw error;
      
      return {
        rows: data || [],
        rowCount: count || (data ? data.length : 0)
      };
    });
  },
  
  /**
   * Get database statistics
   * @returns {Promise<Object>} Database stats
   */
  getStats: async () => {
    try {
      // For serverless environment, provide limited stats
      if (config.isVercel) {
        return {
          initialized: isInitialized,
          lastUsed: new Date(lastUsedTimestamp).toISOString(),
          environment: 'serverless'
        };
      }
      
      // For traditional environment, query for stats
      return db.executeWithRetry(async (supabase) => {
        // This is a simplified example - actual stats would depend on Supabase capabilities
        const { data, error } = await supabase
          .from('pg_stat_activity')
          .select('count');
          
        if (error) {
          logger.warn('Failed to get detailed DB stats', error);
          return {
            initialized: isInitialized,
            connections: 'unknown',
            environment: 'traditional'
          };
        }
        
        return {
          initialized: isInitialized,
          connections: data?.length || 0,
          environment: 'traditional'
        };
      });
    } catch (error) {
      logger.error('Error getting database stats', error);
      return {
        initialized: isInitialized,
        error: error.message
      };
    }
  }
};

export default db;