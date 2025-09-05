// src/core/config.js
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

/**
 * Application configuration
 * Centralizes all configuration settings with environment variable support
 */

// Helper functions for type conversion
const parseBoolean = (value, defaultValue = false) => {
  if (value === undefined || value === null) return defaultValue;
  return value === 'true' || value === '1' || value === true;
};

const parseInteger = (value, defaultValue = 0) => {
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
};

const getOptional = (envVar, defaultValue = '') => {
  return process.env[envVar] || defaultValue;
};

const getRequired = (envVar, defaultValue = null) => {
  const value = process.env[envVar] || defaultValue;
  if (!value) {
    console.error(`Required environment variable ${envVar} is not set`);
  }
  return value;
};

// Client configuration management
class ClientConfig {
  constructor() {
    this.clients = new Map();
    this.loadClients();
  }
  
  loadClients() {
    // Load up to 10 client configurations from environment
    for (let i = 1; i <= 10; i++) {
      const key = process.env[`CLIENT_${i}_KEY`];
      const id = process.env[`CLIENT_${i}_ID`];
      
      if (key && id) {
        this.clients.set(key, { id, key, index: i });
        console.log(`Loaded client ${i}: ID=${id}`);
      }
    }
    
    if (this.clients.size === 0) {
      console.warn('No client configurations found. API authentication will fail.');
    }
  }
  
  getByKey(apiKey) {
    return this.clients.get(apiKey);
  }
  
  getById(clientId) {
    for (const [key, client] of this.clients) {
      if (client.id === clientId) {
        return client;
      }
    }
    return null;
  }
  
  getAllIds() {
    return Array.from(this.clients.values()).map(c => c.id);
  }
  
  getAll() {
    return this.clients;
  }
}

// Main configuration object
export const config = {
  // Environment
  env: process.env.NODE_ENV || 'development',
  isProduction: process.env.NODE_ENV === 'production',
  isDevelopment: process.env.NODE_ENV === 'development',
  isTest: process.env.NODE_ENV === 'test',
  
  // Vercel-specific
  isVercel: process.env.VERCEL === '1',
  vercelEnv: process.env.VERCEL_ENV,
  vercelRegion: process.env.VERCEL_REGION || process.env.AWS_REGION,
  
  // Server
  port: parseInteger(process.env.PORT, 3000),
  host: getOptional('HOST', '0.0.0.0'),
  
  // Database
  database: {
    url: getRequired('SUPABASE_URL'),
    key: getRequired('SUPABASE_SERVICE_ROLE_KEY'),
    maxRetries: parseInteger(process.env.DB_MAX_RETRIES, 3),
    retryDelay: parseInteger(process.env.DB_RETRY_DELAY, 1000),
    poolMin: parseInteger(process.env.DB_POOL_MIN, 2),
    poolMax: parseInteger(process.env.DB_POOL_MAX, 10),
    connectionTimeoutMs: parseInteger(process.env.DB_CONNECTION_TIMEOUT_MS, 10000)
  },
  
  // Clients
  clients: new ClientConfig(),
  
  // Security
  security: {
    saltRounds: parseInteger(process.env.BCRYPT_SALT_ROUNDS, 10),
    jwtSecret: getOptional('JWT_SECRET', 'default-secret-change-in-production'),
    jwtExpiry: getOptional('JWT_EXPIRY', '24h'),
    corsOrigin: getOptional('CORS_ORIGIN', '*'),
    trustedProxies: parseInteger(process.env.TRUSTED_PROXIES, 1),
    cronSecret: getOptional('CRON_SECRET', '')
  },
  
  // Rate Limiting
  rateLimit: {
    windowMs: parseInteger(process.env.RATE_LIMIT_WINDOW_MS, 60000), // 1 minute
    maxRequests: parseInteger(process.env.RATE_LIMIT_MAX_REQUESTS, 100),
    skipSuccessfulRequests: parseBoolean(process.env.RATE_LIMIT_SKIP_SUCCESSFUL, false),
    skipFailedRequests: parseBoolean(process.env.RATE_LIMIT_SKIP_FAILED, false)
  },
  
  // External Services
  services: {
    zeroBounce: {
      apiKey: getOptional('ZEROBOUNCE_API_KEY'),
      baseUrl: getOptional('ZEROBOUNCE_BASE_URL', 'https://api.zerobounce.net/v2'),
      timeout: parseInteger(process.env.ZEROBOUNCE_TIMEOUT, 10000),
      retries: parseInteger(process.env.ZEROBOUNCE_RETRIES, 2),
      enabled: parseBoolean(process.env.ZEROBOUNCE_ENABLED, true)
    },
    openCage: {
      apiKey: getOptional('OPENCAGE_API_KEY'),
      baseUrl: getOptional('OPENCAGE_BASE_URL', 'https://api.opencagedata.com/geocode/v1'),
      timeout: parseInteger(process.env.OPENCAGE_TIMEOUT, 10000),
      retries: parseInteger(process.env.OPENCAGE_RETRIES, 2),
      enabled: parseBoolean(process.env.OPENCAGE_ENABLED, true)
    },
    numverify: {
      apiKey: getOptional('NUMVERIFY_API_KEY'),
      baseUrl: getOptional('NUMVERIFY_BASE_URL', 'http://apilayer.net/api'),
      timeout: parseInteger(process.env.NUMVERIFY_TIMEOUT, 10000),
      retries: parseInteger(process.env.NUMVERIFY_RETRIES, 2),
      enabled: parseBoolean(process.env.NUMVERIFY_ENABLED, false)
    }
  },
  
  // Logging
  logging: {
    level: getOptional('LOG_LEVEL', 'info'),
    format: getOptional('LOG_FORMAT', 'json'),
    colorize: parseBoolean(process.env.LOG_COLORIZE, true),
    timestamp: parseBoolean(process.env.LOG_TIMESTAMP, true),
    maxFiles: parseInteger(process.env.LOG_MAX_FILES, 5),
    maxFileSize: getOptional('LOG_MAX_FILE_SIZE', '20m'),
    directory: getOptional('LOG_DIRECTORY', 'logs'),
    enableConsole: parseBoolean(process.env.LOG_CONSOLE, true),
    enableFile: parseBoolean(process.env.LOG_FILE, false)
  },
  
  // Monitoring
  monitoring: {
    enableMetrics: parseBoolean(process.env.ENABLE_METRICS, true),
    enableHealthCheck: parseBoolean(process.env.ENABLE_HEALTH_CHECK, true),
    enableAlerting: parseBoolean(process.env.ENABLE_ALERTING, true),
    alertWebhook: getOptional('ALERT_WEBHOOK_URL'),
    alertEmail: getOptional('ALERT_EMAIL'),
    persistMetrics: parseBoolean(process.env.PERSIST_METRICS, false),
    metricsPort: parseInteger(process.env.METRICS_PORT, 9090),
    healthCheckPath: '/api/health',
    readinessPath: '/api/ready',
    livenessPath: '/api/live'
  },
  
  // Queue Configuration - UPDATED WITH YOUR REQUIREMENTS
  queue: {
    pendingThreshold: parseInteger(process.env.QUEUE_PENDING_THRESHOLD, 100),
    stalledThresholdMinutes: parseInteger(process.env.QUEUE_STALLED_THRESHOLD_MINUTES, 30),
    stalledThresholdHours: parseInteger(process.env.QUEUE_STALLED_THRESHOLD_HOURS, 1),
    maxRetries: parseInteger(process.env.QUEUE_MAX_RETRIES, 3),
    retryBackoffBase: parseInteger(process.env.QUEUE_RETRY_BACKOFF_BASE, 5), // minutes
    retryBackoffMax: parseInteger(process.env.QUEUE_RETRY_BACKOFF_MAX, 120), // minutes
    completedRetentionDays: parseInteger(process.env.QUEUE_COMPLETED_RETENTION_DAYS, 30),
    batchSize: parseInteger(process.env.QUEUE_BATCH_SIZE, 3), // Changed to 30 items per minute
    maxConcurrency: parseInteger(process.env.QUEUE_MAX_CONCURRENCY, 1), // Stays at 5
    maxRuntime: parseInteger(process.env.QUEUE_MAX_RUNTIME, 300000), // Changed to 60 seconds
    lockTimeout: parseInteger(process.env.QUEUE_LOCK_TIMEOUT, 300000), // 5 minutes
    // Cron job configuration
    cronJobs: {
      processor: {
        schedule: getOptional('CRON_PROCESSOR_SCHEDULE', '*/5 * * * *'), // Every 5 minutes
        operations: getOptional('CRON_PROCESSOR_OPS', 'process,monitor,reset-stalled').split(',')
      },
      cleanup: {
        schedule: getOptional('CRON_CLEANUP_SCHEDULE', '0 0 * * 0'), // Every Sunday at midnight
        operations: getOptional('CRON_CLEANUP_OPS', 'cleanup').split(',')
      },
      resetLimits: {
        schedule: getOptional('CRON_RESET_LIMITS_SCHEDULE', '0 0 * * *') // Every day at midnight
      }
    }
  },
  
  // Unmessy Specific
  unmessy: {
    version: getOptional('UNMESSY_VERSION', '2.0.0'),
    supportEmail: getOptional('SUPPORT_EMAIL', 'support@unmessy.com'),
    maxValidationTimeout: parseInteger(process.env.MAX_VALIDATION_TIMEOUT, 30000), // 30 seconds
    enableCache: parseBoolean(process.env.ENABLE_CACHE, true),
    cacheTimeout: parseInteger(process.env.CACHE_TIMEOUT, 300000), // 5 minutes
    
    // Feature flags
    features: {
      emailValidation: parseBoolean(process.env.FEATURE_EMAIL_VALIDATION, true),
      nameValidation: parseBoolean(process.env.FEATURE_NAME_VALIDATION, true),
      phoneValidation: parseBoolean(process.env.FEATURE_PHONE_VALIDATION, true),
      addressValidation: parseBoolean(process.env.FEATURE_ADDRESS_VALIDATION, true),
      hubspotIntegration: parseBoolean(process.env.FEATURE_HUBSPOT, true),
      asyncProcessing: parseBoolean(process.env.FEATURE_ASYNC_PROCESSING, true)
    }
  },
  
  // Validation Rules
  validation: {
    email: {
      maxLength: parseInteger(process.env.EMAIL_MAX_LENGTH, 320),
      removeGmailAliases: parseBoolean(process.env.REMOVE_GMAIL_ALIASES, true),
      checkMxRecords: parseBoolean(process.env.CHECK_MX_RECORDS, false)
    },
    name: {
      maxLength: parseInteger(process.env.NAME_MAX_LENGTH, 100),
      allowNumbers: parseBoolean(process.env.NAME_ALLOW_NUMBERS, false),
      detectScript: parseBoolean(process.env.NAME_DETECT_SCRIPT, true)
    },
    phone: {
      defaultCountry: getOptional('PHONE_DEFAULT_COUNTRY', 'US'),
      validateCarrier: parseBoolean(process.env.PHONE_VALIDATE_CARRIER, true),
      formatE164: parseBoolean(process.env.PHONE_FORMAT_E164, true)
    },
    address: {
      defaultCountry: getOptional('ADDRESS_DEFAULT_COUNTRY', 'US'),
      geocode: parseBoolean(process.env.ADDRESS_GEOCODE, true),
      standardize: parseBoolean(process.env.ADDRESS_STANDARDIZE, true)
    }
  }
};

// Validate critical configuration on startup
export function validateConfig() {
  const errors = [];
  
  // Check required database config
  if (!config.database.url) {
    errors.push('SUPABASE_URL is required');
  }
  if (!config.database.key) {
    errors.push('SUPABASE_SERVICE_ROLE_KEY is required');
  }
  
  // Check at least one client is configured
  const clients = config.clients.getAll();
  if (clients.size === 0 && config.isProduction) {
    errors.push('No client API keys configured. Set CLIENT_1_KEY and CLIENT_1_ID');
  }
  
  // Check security in production
  if (config.isProduction) {
    if (!config.security.cronSecret) {
      errors.push('CRON_SECRET is required in production');
    }
    if (config.security.jwtSecret === 'default-secret-change-in-production') {
      errors.push('JWT_SECRET must be changed from default in production');
    }
  }
  
  // Check cron configuration if async processing is enabled
  if (config.unmessy.features.asyncProcessing && !config.security.cronSecret) {
    errors.push('CRON_SECRET is required when async processing is enabled');
  }
  
  // Log configuration status
  console.log('Configuration loaded:', {
    environment: config.env,
    isVercel: config.isVercel,
    vercelEnv: config.vercelEnv,
    databaseConfigured: !!config.database.url,
    clientsConfigured: clients.size,
    externalServices: {
      zeroBounce: config.services.zeroBounce.enabled,
      openCage: config.services.openCage.enabled,
      numverify: config.services.numverify.enabled
    },
    queue: {
      batchSize: config.queue.batchSize,
      maxConcurrency: config.queue.maxConcurrency,
      maxRuntime: config.queue.maxRuntime
    }
  });
  
  if (errors.length > 0) {
    console.error('Configuration errors:', errors);
    return false;
  }
  
  return true;
}

// Export default
export default config;