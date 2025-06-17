#!/usr/bin/env node
// scripts/health-check.js
// A comprehensive health check script for the Unmessy API
// Can be run standalone or as part of a monitoring system

import fetch from 'node-fetch';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { program } from 'commander';

// Get current file's directory (ES modules equivalent of __dirname)
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load environment variables
dotenv.config();

// Define CLI options
program
  .option('-u, --url <url>', 'API URL to check', process.env.API_URL || 'http://localhost:3000')
  .option('-k, --key <key>', 'API key for authentication', process.env.API_KEY)
  .option('-o, --output <file>', 'Output file for report', 'health-report.json')
  .option('-v, --verbose', 'Verbose output', false)
  .option('-d, --database', 'Check database connection', false)
  .option('-e, --external', 'Check external services', false)
  .option('-q, --queue', 'Check queue status', false)
  .option('-a, --all', 'Run all checks', false)
  .option('-f, --format <format>', 'Output format (json, text)', 'json')
  .parse(process.argv);

const options = program.opts();

// Define colors for terminal output
const colors = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m'
};

// Initialize health report
const healthReport = {
  timestamp: new Date().toISOString(),
  status: 'ok',
  checks: {},
  summary: {
    passed: 0,
    failed: 0,
    warnings: 0,
    total: 0
  }
};

/**
 * Run the health check
 */
async function runHealthCheck() {
  console.log(`${colors.cyan}Running Unmessy API health check...${colors.reset}`);
  console.log(`${colors.cyan}Target API: ${options.url}${colors.reset}`);
  
  try {
    // Run basic API health check first
    await checkApiHealth();
    
    // Run additional checks based on options
    if (options.all || options.database) {
      await checkDatabase();
    }
    
    if (options.all || options.external) {
      await checkExternalServices();
    }
    
    if (options.all || options.queue) {
      await checkQueue();
    }
    
    // Calculate overall status
    calculateOverallStatus();
    
    // Output report
    outputReport();
    
    // Exit with appropriate code
    process.exit(healthReport.status === 'ok' ? 0 : 1);
  } catch (error) {
    console.error(`${colors.red}Health check failed:${colors.reset}`, error);
    process.exit(1);
  }
}

/**
 * Check basic API health
 */
async function checkApiHealth() {
  try {
    console.log(`${colors.blue}Checking API health...${colors.reset}`);
    
    // Start with a basic health check
    const healthResponse = await fetch(`${options.url}/api/health`);
    const healthData = await healthResponse.json();
    
    // Add to report
    healthReport.checks.api = {
      status: healthResponse.ok ? 'ok' : 'error',
      statusCode: healthResponse.status,
      response: healthData
    };
    
    if (healthResponse.ok) {
      console.log(`${colors.green}✓ API is healthy${colors.reset}`);
      healthReport.summary.passed++;
    } else {
      console.log(`${colors.red}✗ API health check failed: ${healthResponse.status}${colors.reset}`);
      healthReport.summary.failed++;
    }
    
    // If we have an API key, try the detailed health check
    if (options.key) {
      console.log(`${colors.blue}Checking detailed API health...${colors.reset}`);
      
      const detailedResponse = await fetch(`${options.url}/api/health/detailed`, {
        headers: {
          'X-API-Key': options.key
        }
      });
      
      const detailedData = await detailedResponse.json();
      
      // Add to report
      healthReport.checks.apiDetailed = {
        status: detailedResponse.ok ? detailedData.status : 'error',
        statusCode: detailedResponse.status,
        response: detailedData
      };
      
      if (detailedResponse.ok) {
        console.log(`${colors.green}✓ Detailed API check passed${colors.reset}`);
        
        // Check if there are any degraded services in the detailed response
        if (detailedData.status === 'degraded' || detailedData.status === 'warning') {
          console.log(`${colors.yellow}⚠ API status is ${detailedData.status}${colors.reset}`);
          healthReport.summary.warnings++;
        } else {
          healthReport.summary.passed++;
        }
        
        // Log any issues found
        if (detailedData.database && detailedData.database.status !== 'connected') {
          console.log(`${colors.yellow}⚠ Database status: ${detailedData.database.status}${colors.reset}`);
        }
        
        if (detailedData.services) {
          for (const [service, info] of Object.entries(detailedData.services)) {
            if (info.status !== 'available' && info.enabled) {
              console.log(`${colors.yellow}⚠ ${service} status: ${info.status}${colors.reset}`);
            }
          }
        }
      } else {
        console.log(`${colors.red}✗ Detailed API check failed: ${detailedResponse.status}${colors.reset}`);
        healthReport.summary.failed++;
      }
    }
    
    healthReport.summary.total += options.key ? 2 : 1;
  } catch (error) {
    console.error(`${colors.red}API health check failed:${colors.reset}`, error);
    
    // Add to report
    healthReport.checks.api = {
      status: 'error',
      error: error.message
    };
    
    healthReport.summary.failed++;
    healthReport.summary.total++;
  }
}

/**
 * Check database connection directly
 */
async function checkDatabase() {
  try {
    console.log(`${colors.blue}Checking database connection...${colors.reset}`);
    
    // Check for required environment variables
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
      console.log(`${colors.yellow}⚠ Missing database credentials in environment${colors.reset}`);
      
      // Add to report
      healthReport.checks.database = {
        status: 'warning',
        message: 'Missing database credentials'
      };
      
      healthReport.summary.warnings++;
      healthReport.summary.total++;
      return;
    }
    
    // Create Supabase client
    const supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_SERVICE_ROLE_KEY
    );
    
    // Test connection with a simple query
    const { data, error } = await supabase
      .from('clients')
      .select('count')
      .limit(1);
    
    if (error) {
      throw error;
    }
    
    console.log(`${colors.green}✓ Database connection successful${colors.reset}`);
    
    // Get database version (Note: This RPC might not exist, so we handle it gracefully)
    const { data: versionData, error: versionError } = await supabase.rpc('get_pg_version').catch(() => ({ 
      data: null, 
      error: 'Version check not available' 
    }));
    
    // Add to report
    healthReport.checks.database = {
      status: 'ok',
      version: versionError ? 'unknown' : versionData
    };
    
    healthReport.summary.passed++;
    healthReport.summary.total++;
  } catch (error) {
    console.error(`${colors.red}Database check failed:${colors.reset}`, error);
    
    // Add to report
    healthReport.checks.database = {
      status: 'error',
      error: error.message
    };
    
    healthReport.summary.failed++;
    healthReport.summary.total++;
  }
}

/**
 * Check external services (ZeroBounce, OpenCage)
 */
async function checkExternalServices() {
  // ZeroBounce check
  try {
    console.log(`${colors.blue}Checking ZeroBounce service...${colors.reset}`);
    
    if (!process.env.ZERO_BOUNCE_API_KEY) {
      console.log(`${colors.yellow}⚠ Missing ZeroBounce API key${colors.reset}`);
      
      // Add to report
      healthReport.checks.zeroBounce = {
        status: 'warning',
        message: 'Missing API key'
      };
      
      healthReport.summary.warnings++;
    } else {
      // Check ZeroBounce credits
      const response = await fetch(
        `https://api.zerobounce.net/v2/getcredits?api_key=${process.env.ZERO_BOUNCE_API_KEY}`
      );
      
      const data = await response.json();
      
      if (response.ok && data.Credits !== undefined) {
        console.log(`${colors.green}✓ ZeroBounce service check passed: ${data.Credits} credits remaining${colors.reset}`);
        
        // Add to report
        healthReport.checks.zeroBounce = {
          status: 'ok',
          credits: data.Credits
        };
        
        // Warn if credits are low
        if (data.Credits < 100) {
          console.log(`${colors.yellow}⚠ ZeroBounce credits are low: ${data.Credits}${colors.reset}`);
          healthReport.checks.zeroBounce.status = 'warning';
          healthReport.summary.warnings++;
        } else {
          healthReport.summary.passed++;
        }
      } else {
        console.log(`${colors.red}✗ ZeroBounce service check failed${colors.reset}`);
        
        // Add to report
        healthReport.checks.zeroBounce = {
          status: 'error',
          response: data
        };
        
        healthReport.summary.failed++;
      }
    }
  } catch (error) {
    console.error(`${colors.red}ZeroBounce check failed:${colors.reset}`, error);
    
    // Add to report
    healthReport.checks.zeroBounce = {
      status: 'error',
      error: error.message
    };
    
    healthReport.summary.failed++;
  } finally {
    healthReport.summary.total++;
  }
  
  // OpenCage check
  try {
    console.log(`${colors.blue}Checking OpenCage service...${colors.reset}`);
    
    if (!process.env.OPENCAGE_API_KEY) {
      console.log(`${colors.yellow}⚠ Missing OpenCage API key${colors.reset}`);
      
      // Add to report
      healthReport.checks.openCage = {
        status: 'warning',
        message: 'Missing API key'
      };
      
      healthReport.summary.warnings++;
    } else {
      // Check OpenCage with a simple query
      const response = await fetch(
        `https://api.opencagedata.com/geocode/v1/json?q=London&key=${process.env.OPENCAGE_API_KEY}&limit=1`
      );
      
      const data = await response.json();
      
      if (response.ok && data.results && data.results.length > 0) {
        console.log(`${colors.green}✓ OpenCage service check passed${colors.reset}`);
        
        // Add to report
        healthReport.checks.openCage = {
          status: 'ok',
          rateLimit: {
            limit: response.headers.get('x-ratelimit-limit'),
            remaining: response.headers.get('x-ratelimit-remaining')
          }
        };
        
        healthReport.summary.passed++;
      } else {
        console.log(`${colors.red}✗ OpenCage service check failed${colors.reset}`);
        
        // Add to report
        healthReport.checks.openCage = {
          status: 'error',
          response: data
        };
        
        healthReport.summary.failed++;
      }
    }
  } catch (error) {
    console.error(`${colors.red}OpenCage check failed:${colors.reset}`, error);
    
    // Add to report
    healthReport.checks.openCage = {
      status: 'error',
      error: error.message
    };
    
    healthReport.summary.failed++;
  } finally {
    healthReport.summary.total++;
  }
}

/**
 * Check queue status
 */
async function checkQueue() {
  try {
    console.log(`${colors.blue}Checking queue status...${colors.reset}`);
    
    // Check if we have API key for authentication
    if (!options.key) {
      console.log(`${colors.yellow}⚠ API key required for queue check${colors.reset}`);
      
      // Add to report
      healthReport.checks.queue = {
        status: 'warning',
        message: 'API key required'
      };
      
      healthReport.summary.warnings++;
      healthReport.summary.total++;
      return;
    }
    
    // Check queue status via API
    const response = await fetch(`${options.url}/api/admin/queue/status`, {
      headers: {
        'X-API-Key': options.key
      }
    });
    
    if (!response.ok) {
      throw new Error(`Queue status check failed: ${response.status}`);
    }
    
    const data = await response.json();
    
    // Add to report
    healthReport.checks.queue = {
      status: 'ok',
      response: data
    };
    
    // Check for queue backlog
    const pendingCount = data.pending || 0;
    const failedCount = data.failed || 0;
    
    if (pendingCount > 50) {
      console.log(`${colors.yellow}⚠ Queue has ${pendingCount} pending items${colors.reset}`);
      healthReport.checks.queue.status = 'warning';
      healthReport.summary.warnings++;
    } else {
      console.log(`${colors.green}✓ Queue status: ${pendingCount} pending, ${failedCount} failed${colors.reset}`);
      healthReport.summary.passed++;
    }
    
    // Check for failures
    if (failedCount > 10) {
      console.log(`${colors.yellow}⚠ Queue has ${failedCount} failed items${colors.reset}`);
      
      if (healthReport.checks.queue.status !== 'warning') {
        healthReport.checks.queue.status = 'warning';
        healthReport.summary.warnings++;
      }
    }
    
    healthReport.summary.total++;
  } catch (error) {
    console.error(`${colors.red}Queue check failed:${colors.reset}`, error);
    
    // Add to report
    healthReport.checks.queue = {
      status: 'error',
      error: error.message
    };
    
    healthReport.summary.failed++;
    healthReport.summary.total++;
  }
}

/**
 * Calculate overall status based on check results
 */
function calculateOverallStatus() {
  // Start with ok status
  let status = 'ok';
  
  // Check all test results
  for (const check of Object.values(healthReport.checks)) {
    if (check.status === 'error') {
      status = 'error';
      break;
    } else if (check.status === 'warning' && status === 'ok') {
      status = 'warning';
    }
  }
  
  healthReport.status = status;
  
  // Print summary
  console.log(`\n${colors.cyan}Health Check Summary:${colors.reset}`);
  console.log(`${colors.cyan}Total checks: ${healthReport.summary.total}${colors.reset}`);
  console.log(`${colors.green}Passed: ${healthReport.summary.passed}${colors.reset}`);
  console.log(`${colors.yellow}Warnings: ${healthReport.summary.warnings}${colors.reset}`);
  console.log(`${colors.red}Failed: ${healthReport.summary.failed}${colors.reset}`);
  
  // Print overall status
  const statusColor = status === 'ok' ? colors.green : status === 'warning' ? colors.yellow : colors.red;
  console.log(`\n${statusColor}Overall status: ${status}${colors.reset}`);
}

/**
 * Output health report to file and/or console
 */
function outputReport() {
  // Format report based on options
  let formattedReport;
  
  if (options.format === 'json') {
    formattedReport = JSON.stringify(healthReport, null, 2);
  } else { // text format
    formattedReport = `Unmessy API Health Report\n`;
    formattedReport += `Timestamp: ${healthReport.timestamp}\n`;
    formattedReport += `Status: ${healthReport.status}\n\n`;
    
    formattedReport += `Summary:\n`;
    formattedReport += `- Total checks: ${healthReport.summary.total}\n`;
    formattedReport += `- Passed: ${healthReport.summary.passed}\n`;
    formattedReport += `- Warnings: ${healthReport.summary.warnings}\n`;
    formattedReport += `- Failed: ${healthReport.summary.failed}\n\n`;
    
    formattedReport += `Check Results:\n`;
    
    for (const [name, check] of Object.entries(healthReport.checks)) {
      formattedReport += `- ${name}: ${check.status}\n`;
      
      if (check.error) {
        formattedReport += `  Error: ${check.error}\n`;
      }
      
      if (check.message) {
        formattedReport += `  Message: ${check.message}\n`;
      }
      
      if (options.verbose) {
        if (check.response) {
          formattedReport += `  Response: ${JSON.stringify(check.response, null, 2)}\n`;
        }
      }
    }
  }
  
  // Write to file if output option is provided
  if (options.output) {
    const outputPath = path.resolve(options.output);
    
    fs.writeFileSync(outputPath, formattedReport);
    console.log(`\nHealth report saved to: ${outputPath}`);
  }
  
  // Print full report if verbose
  if (options.verbose && options.format === 'json') {
    console.log(`\n${formattedReport}`);
  }
}

// Run the health check
runHealthCheck();