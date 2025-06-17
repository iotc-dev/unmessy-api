// src/services/workers/queue-monitor.js
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import db from '../../core/db.js';
import { triggerAlert, ALERT_TYPES } from '../../monitoring/alerts.js';

// Create logger instance
const logger = createServiceLogger('queue-monitor');

/**
 * Check queue status and alert if needed
 * Called by the cron endpoint
 */
export async function checkQueueStatus() {
  try {
    logger.debug('Checking queue status');
    
    // Query for queue stats
    const { rows } = await db.query(`
      SELECT 
        status, 
        COUNT(*) as count,
        MAX(EXTRACT(EPOCH FROM (NOW() - created_at))) as oldest_seconds
      FROM hubspot_webhook_queue
      GROUP BY status
    `);
    
    // Process results
    const stats = {
      pending: 0,
      processing: 0,
      completed: 0,
      failed: 0,
      oldest: 0,
      timestamp: new Date().toISOString()
    };
    
    rows.forEach(row => {
      stats[row.status.toLowerCase()] = parseInt(row.count);
      if (row.oldest_seconds > stats.oldest) {
        stats.oldest = row.oldest_seconds;
      }
    });
    
    // Check for backed up queue
    const pendingThreshold = config.queue?.pendingThreshold || 100;
    if (stats.pending > pendingThreshold) {
      triggerAlert(ALERT_TYPES.APPLICATION.QUEUE_BACKED_UP, {
        pendingCount: stats.pending,
        threshold: pendingThreshold
      });
    }
    
    // Check for stalled items
    const stalledThresholdHours = config.queue?.stalledThresholdHours || 1;
    const stalledThresholdSeconds = stalledThresholdHours * 3600;
    if (stats.processing > 0 && stats.oldest > stalledThresholdSeconds) {
      triggerAlert(ALERT_TYPES.APPLICATION.QUEUE_PROCESSING_ERROR, {
        processingCount: stats.processing,
        oldestHours: Math.round(stats.oldest / 3600 * 10) / 10
      });
    }
    
    // Check for failed items
    if (stats.failed > 0) {
      logger.warn(`Queue has ${stats.failed} failed items`);
    }
    
    logger.info('Queue status check completed', stats);
    return stats;
    
  } catch (error) {
    logger.error('Error checking queue status', error);
    throw error;
  }
}

/**
 * Reset stalled queue items
 * Called by the cron endpoint
 */
export async function resetStalledItems() {
  try {
    logger.info('Resetting stalled queue items');
    
    const stalledThresholdMinutes = config.queue?.stalledThresholdMinutes || 30;
    
    // Find and reset stalled items
    const { rowCount } = await db.query(`
      UPDATE hubspot_webhook_queue
      SET 
        status = 'pending',
        processing_started_at = NULL,
        next_retry_at = NOW(),
        attempts = attempts + 1
      WHERE 
        status = 'processing'
        AND processing_started_at < NOW() - INTERVAL '${stalledThresholdMinutes} minutes'
        AND attempts < max_attempts
    `);
    
    logger.info(`Reset ${rowCount} stalled queue items`);
    
    return {
      resetCount: rowCount
    };
  } catch (error) {
    logger.error('Error resetting stalled queue items', error);
    throw error;
  }
}

/**
 * Clean up old completed queue items
 * Called by the cron endpoint
 */
export async function cleanupCompletedItems() {
  try {
    logger.info('Cleaning up old completed queue items');
    
    const retentionDays = config.queue?.completedRetentionDays || 30;
    
    // Delete old completed items
    const { rowCount } = await db.query(`
      DELETE FROM hubspot_webhook_queue
      WHERE 
        status = 'completed'
        AND processing_completed_at < NOW() - INTERVAL '${retentionDays} days'
    `);
    
    logger.info(`Cleaned up ${rowCount} old completed queue items`);
    
    return {
      deletedCount: rowCount
    };
  } catch (error) {
    logger.error('Error cleaning up completed queue items', error);
    throw error;
  }
}

// Export all functions as default object
export default {
  checkQueueStatus,
  resetStalledItems,
  cleanupCompletedItems
};