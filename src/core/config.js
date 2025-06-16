// src/core/config.js
import dotenv from 'dotenv';

// Load .env file if it exists (for local development)
// This won't error in Vercel where .env doesn't exist
dotenv.config();

// Helper function to parse boolean environment variables
const parseBoolean = (value, defaultValue = false) => {
  if (value === undefined || value === null || value === '') return defaultValue;
  return value === 'true' || value === '1';
};

// Helper function to parse integer environment variables
const parseInteger = (value, defaultValue) => {
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
};

// Helper function to get required environment variable
const getRequired = (key, description) => {
  const value = process.env[key];
  if (!value && process.env.NODE_ENV === 'production') {
    console.error(`Missing required environment variable: ${key} (${description})`);
  }
  return value;
};

// Helper function to get optional environment variable with default
const getOptional = (key, defaultValue) => {
  return process.env[key] || defaultValue;
};

export const config = {
  // Environment
  env: process.env.NODE_ENV || 'development',
  isProduction: process.env.NODE_ENV === 'production',
  isDevelopment: process.env.NODE_ENV === 'development',
  isTest: process.env.NODE_ENV === 'test',
  
  // Vercel specific
  isVercel: !!process.env.VERCEL,
  vercelEnv: process.env.VERCEL_ENV, // 'production', 'preview', or 'development'
  vercelRegion: process.env.VERCEL_REGION,
  vercelUrl: process.env.VERCEL_URL,
  
  // Server
  port: parseInteger(process.env.PORT, 3000),
  
  // Database (Supabase)
  database: {
    url: getRequired('SUPABASE_URL', 'Supabase project URL'),
    key: getRequired('SUPABASE_SERVICE_ROLE_KEY', 'Supabase service role key'),
    poolMin: parseInteger(process.env.DB_POOL_MIN, 2),
    poolMax: parseInteger(process.env.DB_POOL_MAX, 10),
    idleTimeoutMs: parseInteger(process.env.DB_IDLE_TIMEOUT, 30000),
    connectionTimeoutMs: parseInteger(process.env.DB_CONNECTION_TIMEOUT, 5000)
  },
  
  // External Services
  services: {
    // ZeroBounce (Email Validation)
    zeroBounce: {
      apiKey: getOptional('ZERO_BOUNCE_API_KEY', ''),
      enabled: parseBoolean(process.env.USE_ZERO_BOUNCE, false),
      timeout: parseInteger(process.env.ZERO_BOUNCE_TIMEOUT, 6000),
      retryTimeout: parseInteger(process.env.ZERO_BOUNCE_RETRY_TIMEOUT, 8000),
      maxRetries: parseInteger(process.env.ZERO_BOUNCE_MAX_RETRIES, 2),
      baseUrl: 'https://api.zerobounce.net/v2'
    },
    
    // OpenCage (Address Validation)
    openCage: {
      apiKey: getOptional('OPENCAGE_API_KEY', ''),
      enabled: parseBoolean(process.env.USE_OPENCAGE, false),
      timeout: parseInteger(process.env.OPENCAGE_TIMEOUT, 5000),
      maxRetries: parseInteger(process.env.OPENCAGE_MAX_RETRIES, 2),
      baseUrl: 'https://api.opencagedata.com/geocode/v1'
    },
    
    // Twilio (Phone Validation)
    twilio: {
      accountSid: getOptional('TWILIO_ACCOUNT_SID', ''),
      authToken: getOptional('TWILIO_AUTH_TOKEN', ''),
      enabled: parseBoolean(process.env.USE_TWILIO, false),
      timeout: parseInteger(process.env.TWILIO_TIMEOUT, 5000),
      maxRetries: parseInteger(process.env.TWILIO_MAX_RETRIES, 2)
    },
    
    // HubSpot
    hubspot: {
      webhookSecret: getOptional('HUBSPOT_WEBHOOK_SECRET', ''),
      verifySignature: parseBoolean(process.env.VERIFY_WEBHOOK_SIGNATURE, false),
      defaultPortalId: getOptional('HUBSPOT_DEFAULT_PORTAL_ID', ''),
      defaultFormGuid: getOptional('HUBSPOT_DEFAULT_FORM_GUID', '')
    }
  },
  
  // Queue Configuration
  queue: {
    batchSize: parseInteger(process.env.QUEUE_BATCH_SIZE, 10),
    maxRetries: parseInteger(process.env.QUEUE_MAX_RETRIES, 3),
    processInterval: parseInteger(process.env.QUEUE_PROCESS_INTERVAL, 60000), // 1 minute
    maxRuntime: parseInteger(process.env.QUEUE_MAX_RUNTIME, 55000), // 55 seconds
    lockTimeout: parseInteger(process.env.QUEUE_LOCK_TIMEOUT, 120000) // 2 minutes
  },
  
  // Rate Limiting
  rateLimit: {
    windowMs: parseInteger(process.env.RATE_LIMIT_WINDOW, 60000), // 1 minute
    maxRequests: parseInteger(process.env.RATE_LIMIT_MAX, 100),
    skipSuccessfulRequests: parseBoolean(process.env.RATE_LIMIT_SKIP_SUCCESSFUL, false),
    skipFailedRequests: parseBoolean(process.env.RATE_LIMIT_SKIP_FAILED, false)
  },
  
  // Client Configuration
  clients: {
    defaultClientId: getOptional('DEFAULT_CLIENT_ID', '0001'),
    apiKeyHeader: 'X-API-Key', // Header name for API key
    // Load client configurations from environment
    // Format: CLIENT_1_KEY, CLIENT_1_ID, CLIENT_2_KEY, CLIENT_2_ID, etc.
    getAll: () => {
      const clients = new Map();
      const envKeys = Object.keys(process.env);
      
      // Find all CLIENT_N_KEY variables
      const clientKeyPattern = /^CLIENT_(\d+)_KEY$/;
      const clientKeys = envKeys.filter(key => clientKeyPattern.test(key));
      
      clientKeys.forEach(keyVar => {
        const match = keyVar.match(clientKeyPattern);
        if (match) {
          const num = match[1];
          const idVar = `CLIENT_${num}_ID`;
          const apiKey = process.env[keyVar];
          const clientId = process.env[idVar];
          
          if (apiKey && clientId) {
            clients.set(apiKey, clientId);
          }
        }
      });
      
      return clients;
    }
  },
  
  // Security
  security: {
    corsOrigins: getOptional('ALLOWED_ORIGINS', '*').split(',').map(o => o.trim()),
    corsCredentials: parseBoolean(process.env.CORS_CREDENTIALS, true),
    helmet: {
      contentSecurityPolicy: parseBoolean(process.env.HELMET_CSP, false) // Usually false for APIs
    },
    cronSecret: getOptional('CRON_SECRET', ''),
    adminSecret: getOptional('ADMIN_SECRET', ''),
    jwtSecret: getOptional('JWT_SECRET', 'default-secret-change-in-production')
  },
  
  // Logging
  logging: {
    level: getOptional('LOG_LEVEL', 'info'),
    format: getOptional('LOG_FORMAT', 'json'), // 'json' or 'pretty'
    includeTimestamp: parseBoolean(process.env.LOG_TIMESTAMP, true),
    includeLevel: parseBoolean(process.env.LOG_LEVEL_NAME, true)
  },
  
  // Monitoring
  monitoring: {
    enabled: parseBoolean(process.env.ENABLE_METRICS, true),
    metricsPort: parseInteger(process.env.METRICS_PORT, 9090),
    healthCheckPath: '/api/health',
    readinessPath: '/api/ready',
    livenessPath: '/api/live'
  },
  
  // Unmessy Specific
  unmessy: {
    version: getOptional('UNMESSY_VERSION', '200'),
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
  
  // Log configuration status
  console.log('Configuration loaded:', {
    environment: config.env,
    isVercel: config.isVercel,
    vercelEnv: config.vercelEnv,
    databaseConfigured: !!config.database.url,
    clientsConfigured: clients.size,
    servicesEnabled: {
      zeroBounce: config.services.zeroBounce.enabled,
      openCage: config.services.openCage.enabled,
      twilio: config.services.twilio.enabled
    },
    featuresEnabled: config.unmessy.features
  });
  
  if (errors.length > 0) {
    console.error('Configuration errors:', errors);
    if (config.isProduction) {
      throw new Error(`Configuration validation failed: ${errors.join(', ')}`);
    }
  }
  
  return errors.length === 0;
}

// Export default
export default config;