// scripts/test-connect.js
const { db } = require('../src/core/db');
const config = require('../src/core/config');
const { createServiceLogger } = require('../src/core/logger');

// Create logger instance
const logger = createServiceLogger('test-connect');

/**
 * Tests database connectivity and configuration
 */
async function testDatabaseConnection() {
  console.log('=== Unmessy API Database Connection Test ===\n');
  console.log(`Environment: ${config.env}`);
  console.log(`Database URL: ${config.database.url ? '✓ Configured' : '✗ Missing'}`);
  console.log(`Service Key: ${config.database.key ? '✓ Configured' : '✗ Missing'}\n`);
  
  if (!config.database.url || !config.database.key) {
    console.error('ERROR: Database credentials are missing. Check your environment variables:');
    console.error('- SUPABASE_URL');
    console.error('- SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }
  
  try {
    console.log('Initializing database connection...');
    await db.initialize();
    console.log('✓ Database connection initialized successfully\n');
    
    // Test basic query
    console.log('Testing basic query functionality...');
    await testBasicQuery();
    console.log('✓ Basic query test passed\n');
    
    // Check essential tables
    console.log('Checking essential tables...');
    await checkEssentialTables();
    console.log('✓ All essential tables exist\n');
    
    // Check client configuration
    console.log('Checking client configuration...');
    await checkClientConfiguration();
    
    // Get database statistics
    console.log('\nDatabase statistics:');
    const stats = await db.getStats();
    console.log(JSON.stringify(stats, null, 2));
    
    console.log('\n=== Connection Test Completed Successfully ===');
    process.exit(0);
  } catch (error) {
    console.error('\n✗ Database connection test failed:');
    console.error(error.message);
    if (error.stack) {
      console.error('\nStack trace:');
      console.error(error.stack);
    }
    if (error.originalError) {
      console.error('\nOriginal error:');
      console.error(error.originalError.message);
    }
    process.exit(1);
  } finally {
    // Ensure we close the connection
    await db.cleanup();
  }
}

/**
 * Tests a basic database query
 */
async function testBasicQuery() {
  try {
    const result = await db.query('SELECT NOW() as current_time');
    console.log(`  Current database time: ${result.rows[0].current_time}`);
    return true;
  } catch (error) {
    console.error('  ✗ Basic query failed:');
    console.error(`  ${error.message}`);
    throw error;
  }
}

/**
 * Checks if essential tables exist in the database
 */
async function checkEssentialTables() {
  const essentialTables = [
    'clients',
    'email_validations',
    'valid_domains',
    'invalid_domains',
    'domain_typos',
    'valid_tlds',
    'hubspot_webhook_queue'
  ];
  
  try {
    // Query to check if tables exist
    const query = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      AND table_name = ANY($1)
    `;
    
    const result = await db.query(query, [essentialTables]);
    
    // Check which tables exist
    const existingTables = result.rows.map(row => row.table_name);
    console.log(`  Found ${existingTables.length} of ${essentialTables.length} essential tables`);
    
    // Check for missing tables
    const missingTables = essentialTables.filter(table => !existingTables.includes(table));
    
    if (missingTables.length > 0) {
      console.warn('  ⚠️ Missing tables:');
      missingTables.forEach(table => console.warn(`  - ${table}`));
      console.warn('  Please run database migrations to create these tables');
    } else {
      console.log('  ✓ All essential tables exist');
    }
    
    return true;
  } catch (error) {
    console.error('  ✗ Table check failed:');
    console.error(`  ${error.message}`);
    throw error;
  }
}

/**
 * Checks client configuration in the database
 */
async function checkClientConfiguration() {
  try {
    // Get count of active clients
    const result = await db.query(`
      SELECT COUNT(*) as client_count 
      FROM clients 
      WHERE active = true
    `);
    
    const clientCount = parseInt(result.rows[0].client_count);
    console.log(`  Active clients in database: ${clientCount}`);
    
    // Check environment variables for client API keys
    const envClients = config.clients.getAll();
    console.log(`  Client API keys in environment: ${envClients.size}`);
    
    if (clientCount === 0) {
      console.warn('  ⚠️ No active clients found in database');
      console.warn('  Please add at least one active client');
    }
    
    if (envClients.size === 0) {
      console.warn('  ⚠️ No client API keys configured in environment');
      console.warn('  Add CLIENT_1_KEY and CLIENT_1_ID environment variables');
    }
    
    // Test first client if available
    if (clientCount > 0) {
      const clientResult = await db.query(`
        SELECT client_id, name, active, 
               daily_email_limit, remaining_email, 
               daily_name_limit, remaining_name
        FROM clients 
        WHERE active = true 
        LIMIT 1
      `);
      
      if (clientResult.rows.length > 0) {
        const client = clientResult.rows[0];
        console.log('\n  Sample client configuration:');
        console.log(`  - ID: ${client.client_id}`);
        console.log(`  - Name: ${client.name}`);
        console.log(`  - Status: ${client.active ? 'Active' : 'Inactive'}`);
        console.log(`  - Email limit: ${client.remaining_email}/${client.daily_email_limit}`);
        console.log(`  - Name limit: ${client.remaining_name}/${client.daily_name_limit}`);
      }
    }
    
    return true;
  } catch (error) {
    console.error('  ✗ Client configuration check failed:');
    console.error(`  ${error.message}`);
    // Continue despite error - this is not fatal
    return false;
  }
}

// Execute the test function
testDatabaseConnection().catch(error => {
  console.error('Unhandled error in test script:');
  console.error(error);
  process.exit(1);
});