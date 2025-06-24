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
   * Execute a SQL query with parameters or call a stored function
   * @param {string} textOrFunction - SQL query text or function name
   * @param {Array|Object} params - Query parameters or function arguments
   * @returns {Promise<Object>} Query result
   */
  query: async (textOrFunction, params = []) => {
    return db.executeWithRetry(async (supabase) => {
      // Check if this is a function call (no spaces, SELECT, UPDATE, etc.)
      const isFunction = !textOrFunction.includes(' ') && 
                        !textOrFunction.toUpperCase().includes('SELECT') &&
                        !textOrFunction.toUpperCase().includes('UPDATE') &&
                        !textOrFunction.toUpperCase().includes('INSERT') &&
                        !textOrFunction.toUpperCase().includes('DELETE');
      
      // Handle RPC function calls
      if (isFunction) {
        logger.debug('Executing RPC function', { function: textOrFunction, params });
        
        try {
          const { data, error } = await supabase.rpc(textOrFunction, params);
          
          if (error) throw error;
          
          // Format response to match expected structure
          // If data is a single value, wrap it in an array of objects
          let rows;
          if (Array.isArray(data)) {
            rows = data;
          } else if (data !== null && data !== undefined) {
            // Single value returned - wrap it
            rows = [{ result: data }];
          } else {
            rows = [];
          }
          
          return { rows, rowCount: rows.length };
        } catch (error) {
          logger.error('RPC function call failed', error, { function: textOrFunction });
          throw error;
        }
      }
      
      // Handle specific SQL query patterns
      const text = textOrFunction;
      
      // 1. Queue status query with GROUP BY
      if (text.includes('hubspot_webhook_queue') && text.includes('GROUP BY status')) {
        const { data, error } = await supabase
          .from('hubspot_webhook_queue')
          .select('status, created_at');
        
        if (error) throw error;
        
        // Process results to match expected format
        const statusMap = new Map();
        let oldestCreatedAt = null;
        
        data.forEach(row => {
          // Count by status
          const count = statusMap.get(row.status) || 0;
          statusMap.set(row.status, count + 1);
          
          // Track oldest
          const createdAt = new Date(row.created_at);
          if (!oldestCreatedAt || createdAt < oldestCreatedAt) {
            oldestCreatedAt = createdAt;
          }
        });
        
        // Calculate oldest in seconds
        const oldestSeconds = oldestCreatedAt 
          ? Math.floor((Date.now() - oldestCreatedAt.getTime()) / 1000)
          : 0;
        
        // Convert map to array of rows
        const rows = [];
        statusMap.forEach((count, status) => {
          rows.push({
            status,
            count: count.toString(),
            oldest_seconds: oldestSeconds
          });
        });
        
        return {
          rows: rows.length > 0 ? rows : [{ status: 'empty', count: '0', oldest_seconds: 0 }],
          rowCount: rows.length || 1
        };
      }
      
      // 2. Simple SELECT with LIMIT and OFFSET
      if (text.toLowerCase().includes('select * from')) {
        const tableMatch = text.match(/from\s+(\w+)/i);
        const whereMatch = text.match(/where\s+(\w+)\s*=\s*\$1/i);
        const limitMatch = text.match(/limit\s+\$(\d+)/i);
        const offsetMatch = text.match(/offset\s+\$(\d+)/i);
        
        if (tableMatch) {
          let query = supabase.from(tableMatch[1]).select('*');
          
          if (whereMatch && params.length > 0) {
            query = query.eq(whereMatch[1], params[0]);
          }
          
          if (limitMatch) {
            const limitIndex = parseInt(limitMatch[1]) - 1;
            query = query.limit(params[limitIndex]);
          }
          
          if (offsetMatch) {
            const offsetIndex = parseInt(offsetMatch[1]) - 1;
            query = query.range(params[offsetIndex], params[offsetIndex] + (params[limitIndex - 1] || 20) - 1);
          }
          
          const { data, error } = await query;
          if (error) throw error;
          
          return {
            rows: data || [],
            rowCount: data ? data.length : 0
          };
        }
      }
      
      // 3. UPDATE with complex WHERE
      if (text.toLowerCase().startsWith('update')) {
        const tableMatch = text.match(/update\s+(\w+)/i);
        if (!tableMatch) throw new Error('Invalid UPDATE query');
        
        const tableName = tableMatch[1];
        
        // Handle queue reset query
        if (tableName === 'hubspot_webhook_queue' && text.includes('processing_started_at <')) {
          const intervalMatch = params[0]?.match(/(\d+)\s*minutes/);
          const minutes = intervalMatch ? parseInt(intervalMatch[1]) : 30;
          const cutoffDate = new Date(Date.now() - minutes * 60 * 1000);
          
          const { data, error } = await supabase
            .from('hubspot_webhook_queue')
            .update({
              status: 'pending',
              processing_started_at: null,
              next_retry_at: new Date().toISOString()
            })
            .eq('status', 'processing')
            .lt('processing_started_at', cutoffDate.toISOString())
            .select('id');
          
          if (error) throw error;
          
          // Handle attempts increment separately if needed
          if (data && data.length > 0 && text.includes('attempts + 1')) {
            for (const item of data) {
              await supabase
                .from('hubspot_webhook_queue')
                .update({ attempts: item.attempts + 1 })
                .eq('id', item.id);
            }
          }
          
          return { 
            rows: data || [], 
            rowCount: data ? data.length : 0 
          };
        }
      }
      
      // 4. Simple SELECT COUNT queries
      if (text.toLowerCase().includes('select count(*)')) {
        const tableMatch = text.match(/from\s+(\w+)/i);
        const whereMatch = text.match(/where\s+(\w+)\s*=\s*\$1/i);
        
        if (tableMatch) {
          let query = supabase.from(tableMatch[1]).select('*', { count: 'exact', head: true });
          
          if (whereMatch && params.length > 0) {
            query = query.eq(whereMatch[1], params[0]);
          }
          
          const { count, error } = await query;
          if (error) throw error;
          
          return {
            rows: [{ total: count.toString() }],
            rowCount: 1
          };
        }
      }
      
      // 5. DELETE queries
      if (text.toLowerCase().startsWith('delete from')) {
        const tableMatch = text.match(/delete from\s+(\w+)/i);
        const whereMatch = text.match(/where\s+(\w+)\s*=\s*\$1/i);
        
        if (tableMatch) {
          let query = supabase.from(tableMatch[1]).delete();
          
          if (whereMatch && params.length > 0) {
            query = query.eq(whereMatch[1], params[0]);
          }
          
          const { data, error } = await query.select('id');
          if (error) throw error;
          
          return {
            rows: data || [],
            rowCount: data ? data.length : 0
          };
        }
      }
      
      // If we can't handle the query, suggest using query builder
      throw new Error(`Complex SQL queries are not supported directly. Please use Supabase query builder methods or create a database function for: ${text.substring(0, 100)}...`);
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
   * Delete data from a table
   * @param {string} table - Table name
   * @param {Object} conditions - Where conditions
   * @param {Object} options - Delete options
   * @returns {Promise<Object>} Delete result
   */
  delete: async (table, conditions = {}, options = {}) => {
    return db.executeWithRetry(async (supabase) => {
      let query = supabase.from(table).delete();
      
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
          selectColumns = options.returning.join(',');
        } else {
          selectColumns = options.returning;
        }
        
        query.select(selectColumns);
      }
      
      const { data: result, error } = await query;
      
      if (error) throw error;
      
      return { rows: result || [], rowCount: result ? result.length : 0 };
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
   * Call a stored procedure/function via RPC
   * @param {string} functionName - Name of the database function
   * @param {Object} args - Function arguments
   * @returns {Promise<any>} Function result
   */
  rpc: async (functionName, args = {}) => {
    return db.executeWithRetry(async (supabase) => {
      const { data, error } = await supabase.rpc(functionName, args);
      
      if (error) throw error;
      
      return data;
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
      
      // For traditional environment, provide basic stats
      return {
        initialized: isInitialized,
        lastUsed: new Date(lastUsedTimestamp).toISOString(),
        environment: 'traditional'
      };
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