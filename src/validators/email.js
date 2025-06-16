// src/services/validation/email-validation-service.js
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ValidationError, 
  ExternalServiceError, 
  ZeroBounceError,
  CircuitBreaker,
  ErrorRecovery 
} from '../../core/errors.js';

const logger = createServiceLogger('email-validation-service');

class EmailValidationService {
  constructor() {
    this.logger = logger;
    
    // Initialize circuit breaker for ZeroBounce
    this.zeroBounceCircuitBreaker = new CircuitBreaker({
      name: 'ZeroBounce',
      failureThreshold: 5,
      resetTimeout: 60000,
      monitoringPeriod: 10000
    });
    
    // Load normalization data on startup
    this.loadNormalizationData();
  }
  
  async loadNormalizationData() {
    try {
      // This will be populated from database tables
      this.validDomains = new Set();
      this.invalidDomains = new Set();
      this.domainTypos = new Map();
      this.validTlds = new Set();
      
      // Load all data in parallel
      const [validDomainsData, invalidDomainsData, domainTyposData, validTldsData] = await Promise.all([
        db.query('SELECT domain FROM valid_domains'),
        db.query('SELECT domain FROM invalid_domains'),
        db.query('SELECT typo_domain, correct_domain FROM domain_typos'),
        db.query('SELECT tld FROM valid_tlds')
      ]);
      
      // Populate sets and maps
      validDomainsData.rows.forEach(row => this.validDomains.add(row.domain.toLowerCase()));
      invalidDomainsData.rows.forEach(row => this.invalidDomains.add(row.domain.toLowerCase()));
      domainTyposData.rows.forEach(row => {
        this.domainTypos.set(row.typo_domain.toLowerCase(), row.correct_domain.toLowerCase());
      });
      validTldsData.rows.forEach(row => this.validTlds.add(row.tld.toLowerCase()));
      
      this.logger.info('Email normalization data loaded', {
        validDomains: this.validDomains.size,
        invalidDomains: this.invalidDomains.size,
        domainTypos: this.domainTypos.size,
        validTlds: this.validTlds.size
      });
    } catch (error) {
      this.logger.error('Failed to load normalization data', error);
      // Initialize with empty collections if DB fails
      this.validDomains = new Set();
      this.invalidDomains = new Set();
      this.domainTypos = new Map();
      this.validTlds = new Set();
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
      clientId = null
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
    
    if (!skipZeroBounce && config.services.zeroBounce.enabled) {
      try {
        const zeroBounceResult = await this.checkWithZeroBounce(quickResult.currentEmail, clientId);
        
        // Handle suggested email
        if (zeroBounceResult.suggestedEmail && zeroBounceResult.suggestedEmail !== quickResult.currentEmail) {
          this.logger.debug('ZeroBounce suggested email correction', {
            original: quickResult.currentEmail,
            suggested: zeroBounceResult.suggestedEmail
          });
          
          // Validate the suggested email
          const suggestedDomain = zeroBounceResult.suggestedEmail.split('@')[1];
          if (!this.invalidDomains.has(suggestedDomain)) {
            // Recursively validate the suggested email
            return this.validateEmail(zeroBounceResult.suggestedEmail, {
              ...options,
              skipZeroBounce: true // Don't check ZeroBounce again
            });
          }
        }
        
        // Update result with ZeroBounce data
        if (zeroBounceResult.status === 'valid' || zeroBounceResult.status === 'invalid') {
          result.status = zeroBounceResult.status;
          result.subStatus = zeroBounceResult.subStatus;
          result.recheckNeeded = zeroBounceResult.recheckNeeded;
          result.um_bounce_status = zeroBounceResult.um_bounce_status;
        }
        
        result.validationSteps.push({
          step: 'zerobounce_check',
          result: zeroBounceResult
        });
      } catch (error) {
        this.logger.error('ZeroBounce check failed', error, { email: quickResult.currentEmail });
        
        // If domain is valid in our database, consider it valid
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
  
  // Check with ZeroBounce
  async checkWithZeroBounce(email, clientId = null) {
    const apiKey = config.services.zeroBounce.apiKey;
    if (!apiKey) {
      throw new ValidationError('ZeroBounce API key not configured');
    }
    
    return this.zeroBounceCircuitBreaker.execute(async () => {
      const url = new URL(`${config.services.zeroBounce.baseUrl}/validate`);
      url.searchParams.append('api_key', apiKey);
      url.searchParams.append('email', email);
      url.searchParams.append('ip_address', '');
      
      this.logger.debug('Calling ZeroBounce API', { email });
      
      const response = await ErrorRecovery.withTimeout(
        fetch(url.toString()),
        config.services.zeroBounce.timeout,
        'ZeroBounce API'
      );
      
      if (!response.ok) {
        throw new ZeroBounceError(
          `API error: ${response.statusText}`,
          response.status,
          null
        );
      }
      
      const result = await response.json();
      
      this.logger.debug('ZeroBounce response received', {
        email,
        status: result.status,
        subStatus: result.sub_status
      });
      
      // Map ZeroBounce status
      let mappedStatus, umBounceStatus;
      switch (result.status) {
        case 'valid':
          mappedStatus = 'valid';
          umBounceStatus = 'Unlikely to bounce';
          break;
        case 'invalid':
        case 'spamtrap':
        case 'abuse':
        case 'do_not_mail':
          mappedStatus = 'invalid';
          umBounceStatus = 'Likely to bounce';
          break;
        case 'catch-all':
        case 'unknown':
        default:
          mappedStatus = 'unknown';
          umBounceStatus = 'Unknown';
      }
      
      return {
        email,
        status: mappedStatus,
        subStatus: result.sub_status,
        recheckNeeded: mappedStatus === 'unknown',
        suggestedEmail: result.did_you_mean || null,
        source: 'zerobounce',
        details: result,
        um_bounce_status: umBounceStatus
      };
    });
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

// Create singleton instance
const emailValidationService = new EmailValidationService();

export { emailValidationService, EmailValidationService };