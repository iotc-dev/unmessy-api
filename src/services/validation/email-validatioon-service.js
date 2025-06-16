// src/services/validation/email-validation-service.js
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ValidationError, 
  ErrorRecovery 
} from '../../core/errors.js';
import { zeroBounceService } from '../external/zerobounce.js';

const logger = createServiceLogger('email-validation-service');

class EmailValidationService {
  constructor() {
    this.logger = logger;
    
    // Use the external ZeroBounce service
    this.zeroBounce = zeroBounceService;
    
    // Initialize normalization data
    this.validDomains = new Set();
    this.invalidDomains = new Set();
    this.domainTypos = new Map();
    this.validTlds = new Set();
    
    // Load normalization data on startup
    this.loadNormalizationData();
  }
  
  async loadNormalizationData() {
    try {
      // Load all data in parallel from database
      const [validDomainsData, invalidDomainsData, domainTyposData, validTldsData] = await Promise.all([
        db.query('SELECT domain FROM valid_domains').catch(() => ({ rows: [] })),
        db.query('SELECT domain FROM invalid_domains').catch(() => ({ rows: [] })),
        db.query('SELECT typo_domain, correct_domain FROM domain_typos').catch(() => ({ rows: [] })),
        db.query('SELECT tld FROM valid_tlds').catch(() => ({ rows: [] }))
      ]);
      
      // Populate sets and maps
      if (validDomainsData?.rows) {
        validDomainsData.rows.forEach(row => this.validDomains.add(row.domain.toLowerCase()));
      }
      if (invalidDomainsData?.rows) {
        invalidDomainsData.rows.forEach(row => this.invalidDomains.add(row.domain.toLowerCase()));
      }
      if (domainTyposData?.rows) {
        domainTyposData.rows.forEach(row => {
          this.domainTypos.set(row.typo_domain.toLowerCase(), row.correct_domain.toLowerCase());
        });
      }
      if (validTldsData?.rows) {
        validTldsData.rows.forEach(row => this.validTlds.add(row.tld.toLowerCase()));
      }
      
      this.logger.info('Email normalization data loaded', {
        validDomains: this.validDomains.size,
        invalidDomains: this.invalidDomains.size,
        domainTypos: this.domainTypos.size,
        validTlds: this.validTlds.size
      });
    } catch (error) {
      this.logger.error('Failed to load normalization data', error);
      // Continue with empty collections - don't fail the service
    }
  }
  
  // Basic format validation
  isValidEmailFormat(email) {
    const emailRegex = /^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
    return emailRegex.test(email);
  }
  
  // Generate um_check_id
  generateUmCheckId(clientId) {
    const epochTime = Date.now();
    const lastSixDigits = String(epochTime).slice(-6);
    const clientIdStr = clientId || config.clients.defaultClientId;
    const firstThreeDigits = String(epochTime).slice(0, 3);
    const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
    const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
    return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${config.unmessy.version}`);
  }
  
  // Email typo correction
  async correctEmailTypos(email) {
    if (!email) {
      return { corrected: false, email };
    }
    
    let corrected = false;
    let cleanedEmail = email.trim().toLowerCase();
    
    // Remove spaces
    const noSpaceEmail = cleanedEmail.replace(/\s/g, '');
    if (noSpaceEmail !== cleanedEmail) {
      cleanedEmail = noSpaceEmail;
      corrected = true;
    }
    
    // Split email
    const [localPart, domain] = cleanedEmail.split('@');
    
    if (domain) {
      // Check domain typos
      if (this.domainTypos.has(domain)) {
        cleanedEmail = `${localPart}@${this.domainTypos.get(domain)}`;
        corrected = true;
      }
      
      // Handle Gmail aliases
      if (config.validation.email.removeGmailAliases && domain === 'gmail.com' && localPart.includes('+')) {
        const baseLocal = localPart.split('+')[0];
        cleanedEmail = `${baseLocal}@gmail.com`;
        corrected = true;
      }
      
      // Check TLD corrections
      const tldCorrectedDomain = await this.checkTldCorrection(domain);
      if (tldCorrectedDomain) {
        cleanedEmail = `${localPart}@${tldCorrectedDomain}`;
        corrected = true;
      }
    }
    
    return { corrected, email: cleanedEmail };
  }
  
  // Check for TLD corrections
  async checkTldCorrection(domain) {
    if (!domain) return null;
    
    // Check each TLD for potential corrections
    for (const tld of this.validTlds) {
      const tldNoDot = tld.replace(/\./g, '');
      
      if (domain.endsWith(tldNoDot) && !domain.endsWith(tld)) {
        const index = domain.lastIndexOf(tldNoDot);
        const correctedDomain = domain.substring(0, index) + tld;
        
        this.logger.debug('TLD correction found', {
          domain,
          correctedDomain,
          tld
        });
        
        return correctedDomain;
      }
    }
    
    return null;
  }
  
  // Quick validation (no external calls)
  async quickValidate(email, clientId = null) {
    const startTime = Date.now();
    
    // Format check
    const formatValid = this.isValidEmailFormat(email);
    if (!formatValid) {
      return this.buildValidationResult(email, {
        formatValid: false,
        status: 'invalid',
        subStatus: 'bad_format',
        wasCorrected: false,
        recheckNeeded: false
      }, clientId);
    }
    
    // Correct typos
    const { corrected, email: correctedEmail } = await this.correctEmailTypos(email);
    
    // Extract domain
    const domain = correctedEmail.split('@')[1];
    
    // Check invalid domains
    if (this.invalidDomains.has(domain)) {
      this.logger.debug('Domain is in invalid list', { domain });
      return this.buildValidationResult(email, {
        currentEmail: correctedEmail,
        formatValid: true,
        wasCorrected: corrected,
        domainValid: false,
        isInvalidDomain: true,
        status: 'invalid',
        subStatus: 'invalid_domain',
        recheckNeeded: false
      }, clientId);
    }
    
    // Check valid domains
    const domainValid = this.validDomains.has(domain);
    const status = domainValid ? 'valid' : 'unknown';
    
    return this.buildValidationResult(email, {
      currentEmail: correctedEmail,
      formatValid: true,
      wasCorrected: corrected,
      domainValid,
      status,
      recheckNeeded: !domainValid
    }, clientId);
  }
  
  // Full validation with external services
  async validateEmail(email, options = {}) {
    const { 
      skipZeroBounce = false, 
      clientId = null,
      timeout = config.services.zeroBounce.timeout
    } = options;
    
    this.logger.debug('Starting email validation', { 
      email, 
      skipZeroBounce, 
      clientId 
    });
    
    // Quick validation first
    const quickResult = await this.quickValidate(email, clientId);
    
    // If invalid format or domain, return immediately
    if (!quickResult.formatValid || quickResult.isInvalidDomain) {
      this.logger.debug('Email failed quick validation', {
        email,
        formatValid: quickResult.formatValid,
        isInvalidDomain: quickResult.isInvalidDomain
      });
      return quickResult;
    }
    
    // Check cache
    const cached = await this.checkEmailCache(quickResult.currentEmail);
    if (cached) {
      this.logger.debug('Email found in cache', { email: quickResult.currentEmail });
      return { ...cached, isFromCache: true };
    }
    
    // Enhance with external validation if enabled
    let result = { ...quickResult };
    
    if (!skipZeroBounce && config.services.zeroBounce.enabled && quickResult.recheckNeeded) {
      try {
        const zbResponse = await ErrorRecovery.withTimeout(
          this.zeroBounce.validateEmail(quickResult.currentEmail),
          timeout,
          'ZeroBounce validation'
        );
        
        // Parse ZeroBounce response
        const parsedResponse = this.zeroBounce.parseResponse(zbResponse);
        
        // Update result with ZeroBounce data
        result = this.mergeWithZeroBounceResult(result, parsedResponse);
        
        // Handle ZeroBounce email suggestion
        if (parsedResponse.did_you_mean && parsedResponse.did_you_mean !== quickResult.currentEmail) {
          this.logger.info('ZeroBounce suggested email correction', {
            original: quickResult.currentEmail,
            suggested: parsedResponse.did_you_mean
          });
          
          // Validate the suggested email
          const suggestedValidation = await this.quickValidate(parsedResponse.did_you_mean, clientId);
          if (suggestedValidation.status === 'valid') {
            result.suggestedEmail = parsedResponse.did_you_mean;
            result.um_email = parsedResponse.did_you_mean;
            result.wasCorrected = true;
            result.um_email_status = 'Changed';
          }
        }
        
      } catch (error) {
        this.logger.warn('External email validation failed', error, {
          email: quickResult.currentEmail,
          service: 'ZeroBounce'
        });
        
        // If domain is valid in our database, consider it valid despite ZeroBounce failure
        if (result.domainValid) {
          result.status = 'valid';
          result.recheckNeeded = false;
          result.um_bounce_status = 'Unlikely to bounce';
        }
        
        result.validationSteps.push({
          step: 'zerobounce_check',
          error: error.message,
          fallbackToDatabase: result.domainValid
        });
      }
    }
    
    // Save to cache if valid
    if (result.status === 'valid') {
      await this.saveEmailCache(email, result, clientId);
    }
    
    return result;
  }
  
  // Merge results with ZeroBounce response
  mergeWithZeroBounceResult(quickResult, zbParsed) {
    // Determine final status
    let finalStatus = quickResult.status;
    let umBounceStatus = 'Unknown';
    
    if (zbParsed.status === 'valid') {
      finalStatus = 'valid';
      umBounceStatus = 'Unlikely to bounce';
    } else if (zbParsed.status === 'invalid') {
      finalStatus = 'invalid';
      umBounceStatus = 'Likely to bounce';
    } else if (zbParsed.catch_all) {
      // Catch-all addresses are technically valid but risky
      finalStatus = 'valid';
      umBounceStatus = 'Catch-all domain';
    }
    
    return {
      ...quickResult,
      status: finalStatus,
      subStatus: zbParsed.sub_status || quickResult.subStatus,
      
      // ZeroBounce specific fields
      freeEmail: zbParsed.free_email,
      roleEmail: zbParsed.role,
      catchAll: zbParsed.catch_all,
      disposable: zbParsed.disposable,
      toxic: zbParsed.toxic,
      doNotMail: zbParsed.do_not_mail,
      score: zbParsed.score,
      mxFound: zbParsed.mx_found,
      mxRecord: zbParsed.mx_record,
      smtpProvider: zbParsed.smtp_provider,
      
      // Update unmessy fields
      um_bounce_status: umBounceStatus,
      recheckNeeded: false, // We got a definitive answer
      
      // Validation steps
      validationSteps: [
        ...quickResult.validationSteps,
        {
          step: 'zerobounce_validation',
          provider: 'zerobounce',
          passed: finalStatus === 'valid',
          status: zbParsed.status,
          processedAt: zbParsed.processed_at
        }
      ]
    };
  }
  
  // Build validation result
  buildValidationResult(originalEmail, validationData, clientId) {
    const now = new Date();
    const epochMs = now.getTime();
    const umCheckId = this.generateUmCheckId(clientId);
    
    // Determine um_email_status
    const umEmailStatus = validationData.wasCorrected ? 'Changed' : 'Unchanged';
    
    // Determine um_bounce_status
    let umBounceStatus = validationData.um_bounce_status || 'Unknown';
    if (!validationData.um_bounce_status) {
      if (validationData.status === 'valid') {
        umBounceStatus = 'Unlikely to bounce';
      } else if (validationData.status === 'invalid') {
        umBounceStatus = 'Likely to bounce';
      }
    }
    
    return {
      originalEmail,
      currentEmail: validationData.currentEmail || originalEmail,
      formatValid: validationData.formatValid !== false,
      wasCorrected: validationData.wasCorrected || false,
      status: validationData.status || 'unknown',
      subStatus: validationData.subStatus || null,
      recheckNeeded: validationData.recheckNeeded !== false,
      domainValid: validationData.domainValid,
      isInvalidDomain: validationData.isInvalidDomain,
      
      // Unmessy fields
      um_email: validationData.currentEmail || originalEmail,
      um_email_status: umEmailStatus,
      um_bounce_status: umBounceStatus,
      date_last_um_check: now.toISOString(),
      date_last_um_check_epoch: epochMs,
      um_check_id: umCheckId,
      
      // Validation steps
      validationSteps: validationData.validationSteps || [
        { step: 'format_check', passed: validationData.formatValid !== false },
        { step: 'typo_correction', applied: validationData.wasCorrected, corrected: validationData.currentEmail },
        { step: 'domain_check', passed: validationData.domainValid }
      ]
    };
  }
  
  // Cache operations
  async checkEmailCache(email) {
    try {
      const { data, error } = await db.getEmailValidation(email);
      
      if (data) {
        return {
          originalEmail: email,
          currentEmail: data.um_email || email,
          formatValid: true,
          wasCorrected: data.um_email !== email,
          status: data.um_bounce_status === 'Unlikely to bounce' ? 'valid' : 'invalid',
          recheckNeeded: false,
          um_email: data.um_email,
          um_email_status: data.um_email_status,
          um_bounce_status: data.um_bounce_status,
          date_last_um_check: data.date_last_um_check,
          date_last_um_check_epoch: data.date_last_um_check_epoch,
          um_check_id: data.um_check_id,
          isFromCache: true
        };
      }
      
      return null;
    } catch (error) {
      this.logger.error('Failed to check email cache', error, { email });
      return null;
    }
  }
  
  async saveEmailCache(email, validationResult, clientId) {
    // Only save valid emails
    if (validationResult.status !== 'valid') {
      return;
    }
    
    try {
      await db.saveEmailValidation({
        email,
        um_email: validationResult.um_email || validationResult.currentEmail,
        um_email_status: validationResult.um_email_status,
        um_bounce_status: validationResult.um_bounce_status,
        date_last_um_check: validationResult.date_last_um_check,
        date_last_um_check_epoch: validationResult.date_last_um_check_epoch,
        um_check_id: validationResult.um_check_id
      });
      
      this.logger.debug('Email validation saved to cache', { email, clientId });
    } catch (error) {
      this.logger.error('Failed to save email validation', error, { email });
    }
  }
  
  // Format date string
  formatDateString(date) {
    return date.toISOString();
  }
}

// Export the class for use in validation-service.js
export { EmailValidationService };