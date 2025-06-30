// src/services/validation/email-validation-service.js
import dns from 'dns';
import { promisify } from 'util';
import validator from 'validator';
import db from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { 
  ValidationError, 
  ErrorRecovery 
} from '../../core/errors.js';
import { zeroBounceService } from '../external/zerobounce.js';

const logger = createServiceLogger('email-validation-service');

// Promisify DNS functions for async/await
const resolveMx = promisify(dns.resolveMx);

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
    this.tldTypos = new Map();
    
    // MX record cache to avoid repeated DNS lookups
    this.mxCache = new Map();
    this.mxCacheTTL = 3600000; // 1 hour
    
    // Load normalization data on startup
    this.loadNormalizationData();
  }
  
  // Get client account type
  async getClientAccountType(clientId) {
    try {
      const result = await db.select('clients', 
        { client_id: clientId }, 
        { 
          columns: 'um_account_type',
          limit: 1
        }
      );
      
      if (result && result.rows && result.rows.length > 0) {
        return result.rows[0];
      }
      
      return null;
    } catch (error) {
      this.logger.error('Failed to get client account type', error, { clientId });
      return null;
    }
  }
  
  async loadNormalizationData() {
    try {
      // Load all data in parallel from database using proper Supabase methods
      const [
        validDomainsData, 
        invalidDomainsData, 
        domainTyposData, 
        validTldsData,
        tldTyposData
      ] = await Promise.all([
        db.select('valid_domains', {}, { columns: 'domain' }).catch(() => ({ rows: [] })),
        db.select('invalid_domains', {}, { columns: 'domain' }).catch(() => ({ rows: [] })),
        db.select('domain_typos', {}, { columns: 'typo_domain, correct_domain' }).catch(() => ({ rows: [] })),
        db.select('valid_tlds', {}, { columns: 'tld' }).catch(() => ({ rows: [] })),
        db.select('tld_typos', {}, { columns: 'typo_tld, correct_tld' }).catch(() => ({ rows: [] }))
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
      
      // Load TLD typos
      this.tldTypos = new Map();
      if (tldTyposData?.rows) {
        tldTyposData.rows.forEach(row => {
          this.tldTypos.set(row.typo_tld.toLowerCase(), row.correct_tld.toLowerCase());
        });
      }
      
      // Initialize default data if database is empty
      this.initializeDefaultData();
      
      this.logger.info('Email normalization data loaded', {
        validDomains: this.validDomains.size,
        invalidDomains: this.invalidDomains.size,
        domainTypos: this.domainTypos.size,
        validTlds: this.validTlds.size,
        tldTypos: this.tldTypos.size
      });
    } catch (error) {
      this.logger.error('Failed to load normalization data', error);
      // Initialize with defaults
      this.initializeDefaultData();
    }
  }
  
  initializeDefaultData() {
    // Default valid domains if not loaded from DB
    if (this.validDomains.size === 0) {
      const defaultValidDomains = [
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'live.com',
        'aol.com', 'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com',
        'yandex.com', 'gmx.com', 'fastmail.com', 'tutanota.com', 'me.com',
        'msn.com', 'qq.com', '163.com', '126.com', 'sina.com', 'verizon.net',
        'att.net', 'sbcglobal.net', 'cox.net', 'earthlink.net', 'charter.net',
        'comcast.net', 'xfinity.com', 'rocketmail.com', 'ymail.com',
        'mail.ru', 'inbox.ru', 'list.ru', 'bk.ru', 'protonmail.ch',
        'pm.me', 'yahoo.co.uk', 'yahoo.ca', 'yahoo.com.au', 'yahoo.co.in',
        'yahoo.co.jp', 'yahoo.de', 'yahoo.fr', 'yahoo.es', 'yahoo.it',
        'outlook.de', 'outlook.fr', 'outlook.es', 'outlook.it', 'outlook.jp',
        'gmail.co.uk', 'gmail.ca', 'gmail.com.au', 'gmail.co.in', 'gmail.de'
      ];
      defaultValidDomains.forEach(d => this.validDomains.add(d));
    }
    
    // Default invalid domains
    if (this.invalidDomains.size === 0) {
      const defaultInvalidDomains = [
        'example.com', 'test.com', 'email.com', 'tempmail.com', 'throwaway.email',
        'guerrillamail.com', '10minutemail.com', 'mailinator.com', 'maildrop.cc',
        'trashmail.com', 'fake.com', 'dummy.com', 'nowhere.com', 'noemail.com',
        'bounce.com', 'blocked.com', 'invalid.com', 'noreply.com', 'donotreply.com',
        'dispostable.com', 'fakeinbox.com', 'yopmail.com'
      ];
      defaultInvalidDomains.forEach(d => this.invalidDomains.add(d));
    }
    
    // Default domain typos
    if (this.domainTypos.size === 0) {
      const defaultTypos = new Map([
        ['gmial.com', 'gmail.com'],
        ['gmai.com', 'gmail.com'],
        ['gmil.com', 'gmail.com'],
        ['gmal.com', 'gmail.com'],
        ['gmali.com', 'gmail.com'],
        ['gamil.com', 'gmail.com'],
        ['gmail.co', 'gmail.com'],
        ['gmail.cm', 'gmail.com'],
        ['gmaill.com', 'gmail.com'],
        ['gnail.com', 'gmail.com'],
        ['gmailcom', 'gmail.com'],
        ['yahooo.com', 'yahoo.com'],
        ['yaho.com', 'yahoo.com'],
        ['yahou.com', 'yahoo.com'],
        ['yahoo.co', 'yahoo.com'],
        ['yahoo.cm', 'yahoo.com'],
        ['yhaoo.com', 'yahoo.com'],
        ['yahoocom', 'yahoo.com'],
        ['hotmial.com', 'hotmail.com'],
        ['hotmal.com', 'hotmail.com'],
        ['hotmil.com', 'hotmail.com'],
        ['hotmail.co', 'hotmail.com'],
        ['hotmail.cm', 'hotmail.com'],
        ['hotmali.com', 'hotmail.com'],
        ['hotmailcom', 'hotmail.com'],
        ['otmail.com', 'hotmail.com'],
        ['outlok.com', 'outlook.com'],
        ['outloo.com', 'outlook.com'],
        ['outloook.com', 'outlook.com'],
        ['outlook.co', 'outlook.com'],
        ['outlook.cm', 'outlook.com'],
        ['outlookcom', 'outlook.com'],
        ['iclud.com', 'icloud.com'],
        ['icloud.co', 'icloud.com'],
        ['icloud.cm', 'icloud.com'],
        ['icloudcom', 'icloud.com'],
        ['protonmai.com', 'protonmail.com'],
        ['protonmal.com', 'protonmail.com'],
        ['protonmailcom', 'protonmail.com']
      ]);
      defaultTypos.forEach((correct, typo) => this.domainTypos.set(typo, correct));
    }
    
    // Default valid TLDs
    if (this.validTlds.size === 0) {
      const defaultTlds = [
        '.com', '.net', '.org', '.edu', '.gov', '.mil', '.int',
        '.co', '.io', '.ai', '.app', '.dev', '.tech', '.me', '.info', '.biz',
        '.us', '.uk', '.ca', '.au', '.de', '.fr', '.it', '.es', '.nl', '.be',
        '.ch', '.at', '.se', '.no', '.dk', '.fi', '.ie', '.pt', '.gr', '.pl',
        '.cz', '.ro', '.hu', '.ru', '.ua', '.by', '.kz', '.jp', '.cn', '.in',
        '.kr', '.tw', '.hk', '.sg', '.my', '.th', '.vn', '.id', '.ph', '.nz',
        '.za', '.eg', '.ma', '.ng', '.ke', '.tz', '.gh', '.et', '.ug', '.zm',
        '.br', '.mx', '.ar', '.cl', '.co', '.pe', '.ve', '.ec', '.uy', '.py',
        '.bo', '.do', '.gt', '.sv', '.hn', '.ni', '.cr', '.pa', '.jm', '.tt',
        '.co.uk', '.co.jp', '.co.in', '.com.au', '.com.br', '.com.mx'
      ];
      defaultTlds.forEach(t => this.validTlds.add(t));
    }
    
    // Default TLD typos
    if (!this.tldTypos || this.tldTypos.size === 0) {
      this.tldTypos = new Map([
        ['.cmo', '.com'],
        ['.con', '.com'],
        ['.cpm', '.com'],
        ['.comm', '.com'],
        ['.co', '.com'],
        ['.cm', '.com'],
        ['.ogr', '.org'],
        ['.orgg', '.org'],
        ['.nte', '.net'],
        ['.ent', '.net'],
        ['.ner', '.net']
      ]);
    }
  }
  
  // Generate um_check_id
  generateUmCheckId(clientId) {
    const epochTime = Date.now();
    const lastSixDigits = String(epochTime).slice(-6);
    const clientIdStr = clientId || config.clients.defaultClientId || '0001';
    const firstThreeDigits = String(epochTime).slice(0, 3);
    const sum = [...firstThreeDigits].reduce((acc, digit) => acc + parseInt(digit), 0);
    const checkDigit = String(sum * parseInt(clientIdStr)).padStart(3, '0').slice(-3);
    return Number(`${lastSixDigits}${clientIdStr}${checkDigit}${config.unmessy.version.replace(/\./g, '')}`);
  }
  
  // Email typo correction
  async correctEmailTypos(email) {
    if (!email) {
      return { corrected: false, email, suggestions: [] };
    }
    
    let corrected = false;
    let cleanedEmail = email.trim().toLowerCase();
    const suggestions = [];
    
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
        const correctDomain = this.domainTypos.get(domain);
        cleanedEmail = `${localPart}@${correctDomain}`;
        corrected = true;
        suggestions.push({
          type: 'domain_typo',
          original: domain,
          suggestion: correctDomain,
          email: cleanedEmail
        });
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
        suggestions.push({
          type: 'tld_correction',
          original: domain,
          suggestion: tldCorrectedDomain,
          email: cleanedEmail
        });
      }
    }
    
    return { corrected, email: cleanedEmail, suggestions };
  }
  
  // Check for TLD corrections
  async checkTldCorrection(domain) {
    if (!domain) return null;
    
    // First check TLD typos table
    const parts = domain.split('.');
    if (parts.length >= 2) {
      const currentTld = '.' + parts[parts.length - 1];
      
      // Check if current TLD is in typo corrections
      if (this.tldTypos && this.tldTypos.has(currentTld)) {
        const correctTld = this.tldTypos.get(currentTld);
        parts[parts.length - 1] = correctTld.substring(1); // Remove the dot
        const correctedDomain = parts.join('.');
        
        this.logger.debug('TLD typo correction found', {
          domain,
          correctedDomain,
          typoTld: currentTld,
          correctTld
        });
        
        return correctedDomain;
      }
    }
    
    // Check for missing dots in TLDs (e.g., "gmailcom" -> "gmail.com")
    for (const tld of this.validTlds) {
      const tldNoDot = tld.replace(/\./g, '');
      
      if (domain.endsWith(tldNoDot) && !domain.endsWith(tld)) {
        const index = domain.lastIndexOf(tldNoDot);
        const correctedDomain = domain.substring(0, index) + tld;
        
        this.logger.debug('TLD correction found (missing dot)', {
          domain,
          correctedDomain,
          tld
        });
        
        return correctedDomain;
      }
    }
    
    return null;
  }
  
  // MX Record lookup with caching
  async checkMxRecords(domain) {
    // Check if MX check is enabled in config
    if (!config.validation.email.checkMxRecords) {
      return { hasMxRecords: true, mxRecords: [], fromCache: false, skipped: true };
    }
    
    // Check cache first
    const cached = this.mxCache.get(domain);
    if (cached && Date.now() - cached.timestamp < this.mxCacheTTL) {
      this.logger.debug('MX records found in cache', { domain });
      return { ...cached.data, fromCache: true };
    }
    
    try {
      this.logger.debug('Checking MX records for domain', { domain });
      
      // Perform DNS MX lookup
      const mxRecords = await resolveMx(domain);
      
      // Sort by priority (lower number = higher priority)
      mxRecords.sort((a, b) => a.priority - b.priority);
      
      const result = {
        hasMxRecords: mxRecords.length > 0,
        mxRecords: mxRecords,
        primaryMx: mxRecords[0]?.exchange || null,
        fromCache: false
      };
      
      // Cache the result
      this.mxCache.set(domain, {
        data: result,
        timestamp: Date.now()
      });
      
      this.logger.debug('MX records check completed', { 
        domain, 
        hasMxRecords: result.hasMxRecords,
        recordCount: mxRecords.length 
      });
      
      return result;
    } catch (error) {
      this.logger.debug('MX record lookup failed', { domain, error: error.message });
      
      // Check if it's a DNS error (no records found)
      if (error.code === 'ENODATA' || error.code === 'ENOTFOUND') {
        const result = {
          hasMxRecords: false,
          mxRecords: [],
          primaryMx: null,
          fromCache: false
        };
        
        // Cache negative results too
        this.mxCache.set(domain, {
          data: result,
          timestamp: Date.now()
        });
        
        return result;
      }
      
      // For other errors (network issues, etc), assume MX records might exist
      // to avoid false negatives
      this.logger.error('MX lookup error', error, { domain });
      return { 
        hasMxRecords: true, 
        mxRecords: [], 
        error: error.message,
        fromCache: false 
      };
    }
  }
  
  // Clean up old MX cache entries periodically
  cleanupMxCache() {
    const now = Date.now();
    for (const [domain, entry] of this.mxCache.entries()) {
      if (now - entry.timestamp > this.mxCacheTTL) {
        this.mxCache.delete(domain);
      }
    }
  }
  
  // FIXED: Map ZeroBounce status to correct um_bounce_status
  mapBounceStatus(zbStatus, subStatus) {
    // Only two allowed values: "Likely to bounce" or "Unlikely to bounce"
    
    // List of statuses that mean the email is valid and unlikely to bounce
    const validStatuses = ['valid'];
    
    // Everything else is likely to bounce
    if (validStatuses.includes(zbStatus?.toLowerCase())) {
      return 'Unlikely to bounce';
    }
    
    // All other statuses (invalid, catch-all, spamtrap, abuse, do_not_mail, unknown, etc.)
    return 'Likely to bounce';
  }
  
  // Main validation method - UPDATED with new flow
  async validateEmail(email, options = {}) {
    const { clientId = null, useCache = true, useZeroBounce = true } = options;
    
    try {
      // Get client account type
      let accountType = 'basic'; // default
      if (clientId) {
        try {
          const client = await this.getClientAccountType(clientId);
          if (client) {
            accountType = client.um_account_type || 'basic';
          }
        } catch (error) {
          this.logger.warn('Failed to get client account type', { clientId, error: error.message });
        }
      }
      // STEP 1: Check database for valid emails (previously validated as "Unlikely to bounce")
      if (useCache) {
        const existingValidEmail = await this.checkEmailCache(email);
        if (existingValidEmail) {
          this.logger.debug('Email found in valid emails database', { email });
          // Return immediately - trust forever
          return existingValidEmail;
        }
      }
      
      // STEP 2: Format check using validator.js
      const isValidFormat = validator.isEmail(email, {
        allow_display_name: false,
        require_display_name: false,
        allow_utf8_local_part: true,
        require_tld: true,
        allow_ip_domain: false,
        domain_specific_validation: false,
        blacklisted_chars: '',
        host_blacklist: []
      });
      
      if (!isValidFormat) {
        // If validator.js says it's invalid, try ZeroBounce for "did you mean"
        if (useZeroBounce && this.zeroBounce && config.services.zeroBounce.enabled) {
          try {
            const zbResult = await this.zeroBounce.validateEmail(email);
            
            // If ZeroBounce has a "did you mean" suggestion, use it
            if (zbResult.didYouMean && zbResult.didYouMean !== email) {
              this.logger.debug('ZeroBounce suggested correction', { 
                original: email, 
                suggestion: zbResult.didYouMean 
              });
              
              // Validate the suggested email recursively
              return await this.validateEmail(zbResult.didYouMean, options);
            }
          } catch (error) {
            this.logger.error('ZeroBounce validation failed during format check', error);
          }
        }
        
        // No suggestions or ZeroBounce failed, return as invalid
        return this.buildValidationResult(email, {
          formatValid: false,
          status: 'invalid',
          subStatus: 'bad_format',
          wasCorrected: false,
          recheckNeeded: false,
          um_bounce_status: 'Likely to bounce'
        }, clientId, accountType);
      }
      
      // STEP 3: Perform typo corrections
      const { corrected, email: correctedEmail, suggestions } = await this.correctEmailTypos(email);
      
      // Extract domain
      const domain = correctedEmail.split('@')[1];
      
      // STEP 4: Check invalid domains (return as "Likely to bounce")
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
          recheckNeeded: false,
          um_bounce_status: 'Likely to bounce',
          suggestions
        }, clientId, accountType);
      }
      
      // STEP 5: MX record lookup (to reduce external API calls)
      let mxCheck = await this.checkMxRecords(domain);
      
      if (!mxCheck.hasMxRecords && !mxCheck.skipped) {
        // No MX records found - return as invalid without calling ZeroBounce
        return this.buildValidationResult(email, {
          currentEmail: correctedEmail,
          formatValid: true,
          wasCorrected: corrected,
          domainValid: false,
          mxRecordsFound: false,
          status: 'invalid',
          subStatus: 'no_mx_records',
          recheckNeeded: false,
          um_bounce_status: 'Likely to bounce',
          suggestions,
          mxInfo: {
            checked: true,
            hasMxRecords: false,
            records: []
          }
        }, clientId, accountType);
      }
      
      // STEP 6: ZeroBounce validation (only if MX records exist)
      if (useZeroBounce && this.zeroBounce && config.services.zeroBounce.enabled) {
        try {
          // Direct validation without credit check
          let zbResult = await this.zeroBounce.validateEmail(correctedEmail);
          let finalEmail = correctedEmail;
          let totalCorrections = corrected;
          
          // Handle "did you mean" suggestions
          if (zbResult.didYouMean && zbResult.didYouMean !== correctedEmail) {
            this.logger.debug('ZeroBounce suggested correction, validating suggestion', {
              original: correctedEmail,
              suggestion: zbResult.didYouMean
            });
            
            suggestions.push({
              type: 'zerobounce_suggestion',
              original: correctedEmail,
              suggestion: zbResult.didYouMean,
              email: zbResult.didYouMean
            });
            
            // Validate the suggested email
            const suggestionResult = await this.zeroBounce.validateEmail(zbResult.didYouMean);
            
            // Use the suggestion result as our final result
            zbResult = suggestionResult;
            finalEmail = zbResult.email || zbResult.didYouMean;
            totalCorrections = true;
          }
          
          // Build final result with ZeroBounce data
          const finalResult = this.buildZeroBounceResult(
            email,
            finalEmail,
            totalCorrections,
            zbResult,
            mxCheck,
            suggestions,
            clientId,
            accountType
          );
          
          // STEP 7: Save to database only if "Unlikely to bounce"
          if (useCache && finalResult.status === 'valid' && finalResult.um_bounce_status === 'Unlikely to bounce') {
            await this.saveEmailCache(email, finalResult, clientId);
          }
          
          return finalResult;
          
        } catch (error) {
          // Handle specific ZeroBounce errors
          if (error.code === 'insufficient_credits' || 
              (error.message && error.message.toLowerCase().includes('insufficient') && error.message.toLowerCase().includes('credits'))) {
            this.logger.warn('ZeroBounce insufficient credits, falling back to basic validation', { email: correctedEmail });
            // Fall back to basic validation
            return this.performBasicValidation(email, correctedEmail, corrected, suggestions, clientId, mxCheck, accountType);
          }
          
          // For other errors, log and fall back
          this.logger.error('ZeroBounce validation failed', error, { email: correctedEmail });
          
          // Fall back to basic validation
          return this.performBasicValidation(email, correctedEmail, corrected, suggestions, clientId, mxCheck, accountType);
        }
      }
      
      // If ZeroBounce is disabled, perform basic validation
      return this.performBasicValidation(email, correctedEmail, corrected, suggestions, clientId, mxCheck, accountType);
    } catch (error) {
      this.logger.error('Email validation failed', error, { email });
      throw new ValidationError(`Email validation failed: ${error.message}`);
    }
  }
  
  // Perform basic validation when ZeroBounce is not available
  async performBasicValidation(originalEmail, correctedEmail, wasCorrected, suggestions, clientId, mxCheck, accountType = 'basic') {
    const domain = correctedEmail.split('@')[1];
    
    // If MX check wasn't already done, do it now
    if (!mxCheck) {
      mxCheck = await this.checkMxRecords(domain);
    }
    
    // Check if domain is in valid domains list
    const domainValid = this.validDomains.has(domain);
    const status = domainValid && mxCheck.hasMxRecords ? 'valid' : 'unknown';
    
    // FIXED: Use proper bounce status mapping
    const umBounceStatus = (domainValid && mxCheck.hasMxRecords) ? 'Unlikely to bounce' : 'Likely to bounce';
    
    return this.buildValidationResult(originalEmail, {
      currentEmail: correctedEmail,
      formatValid: true,
      wasCorrected,
      domainValid,
      mxRecordsFound: mxCheck.hasMxRecords,
      status,
      recheckNeeded: !domainValid,
      um_bounce_status: umBounceStatus,
      suggestions,
      mxInfo: {
        checked: !mxCheck.skipped,
        hasMxRecords: mxCheck.hasMxRecords,
        primaryMx: mxCheck.primaryMx,
        recordCount: mxCheck.mxRecords?.length || 0
      }
    }, clientId, accountType);
  }
  
  // Build result with ZeroBounce data
  buildZeroBounceResult(originalEmail, finalEmail, wasCorrected, zbResult, mxCheck, suggestions, clientId, accountType = 'basic') {
    const now = new Date();
    const epochMs = now.getTime();
    const umCheckId = this.generateUmCheckId(clientId);
    
    // Determine final status based on ZeroBounce
    let status = 'unknown';
    let subStatus = zbResult.subStatus;
    
    // FIXED: Use the new mapping function
    const umBounceStatus = this.mapBounceStatus(zbResult.status, zbResult.subStatus);
    
    if (zbResult.status === 'valid') {
      status = 'valid';
    } else if (zbResult.status === 'invalid' || zbResult.status === 'catch-all' || 
               zbResult.status === 'spamtrap' || zbResult.status === 'abuse' || 
               zbResult.status === 'do_not_mail') {
      status = 'invalid';
    }
    
    // Build validation steps
    const validationSteps = [
      { step: '1_database_check', passed: false, note: 'Email not found in database' },
      { step: '2_format_check', passed: true, validator: 'validator.js' },
      { step: '3_typo_correction', applied: wasCorrected, corrected: finalEmail },
      { step: '4_invalid_domain_check', passed: true },
      { 
        step: '5_mx_record_check', 
        performed: mxCheck ? !mxCheck.skipped : true, 
        hasMxRecords: mxCheck ? mxCheck.hasMxRecords : (zbResult.mxFound === 'true' || zbResult.mxFound === true),
        primaryMx: mxCheck ? mxCheck.primaryMx : zbResult.mxRecord
      },
      { 
        step: '6_zerobounce_validation', 
        performed: true, 
        status: zbResult.status,
        subStatus: zbResult.subStatus,
        freeEmail: zbResult.freeEmail,
        didYouMean: zbResult.didYouMean,
        mxFound: zbResult.mxFound
      }
    ];
    
    const result = {
      originalEmail,
      currentEmail: finalEmail,
      formatValid: true,
      wasCorrected,
      status,
      subStatus,
      recheckNeeded: false,
      domainValid: true,
      
      // Unmessy fields
      um_email: finalEmail,
      um_email_status: wasCorrected ? 'Changed' : 'Unchanged',
      date_last_um_check: now.toISOString(),
      date_last_um_check_epoch: epochMs,
      um_check_id: umCheckId,
      
      // Suggestions
      suggestions,
      
      // MX info
      mxInfo: mxCheck ? {
        checked: !mxCheck.skipped,
        hasMxRecords: mxCheck.hasMxRecords,
        primaryMx: mxCheck.primaryMx,
        recordCount: mxCheck.mxRecords?.length || 0
      } : {
        // If no MX check was done but ZeroBounce found MX records
        checked: true,
        hasMxRecords: zbResult.mxFound === 'true' || zbResult.mxFound === true,
        primaryMx: zbResult.mxRecord || null,
        recordCount: zbResult.mxFound === 'true' || zbResult.mxFound === true ? 1 : 0
      },
      
      // ZeroBounce data
      zeroBounce: {
        status: zbResult.status,
        subStatus: zbResult.subStatus,
        account: zbResult.account,
        domain: zbResult.domain,
        didYouMean: zbResult.didYouMean,
        domainAgeDays: zbResult.domainAgeDays,
        freeEmail: zbResult.freeEmail,
        mxFound: zbResult.mxFound,
        mxRecord: zbResult.mxRecord,
        smtpProvider: zbResult.smtpProvider,
        firstname: zbResult.firstname,
        lastname: zbResult.lastname,
        gender: zbResult.gender,
        country: zbResult.country,
        region: zbResult.region,
        city: zbResult.city,
        zipcode: zbResult.zipcode,
        processedAt: zbResult.processedAt
      },
      
      // Validation steps
      validationSteps
    };
    
    // Add um_bounce_status only if account type is not 'basic'
    if (accountType !== 'basic') {
      result.um_bounce_status = umBounceStatus;
    }
    
    return result;
  }
  
  // Build validation result
  buildValidationResult(originalEmail, validationData, clientId, accountType = 'basic') {
    const now = new Date();
    const epochMs = now.getTime();
    const umCheckId = this.generateUmCheckId(clientId);
    
    // Determine um_email_status
    const umEmailStatus = validationData.wasCorrected ? 'Changed' : 'Unchanged';
    
    // Determine um_bounce_status (already set in validation data)
    const umBounceStatus = validationData.um_bounce_status || 'Likely to bounce';
    
    // Build validation steps
    const validationSteps = [
      { step: '1_database_check', passed: false, note: 'Email not found in database' },
      { step: '2_format_check', passed: validationData.formatValid !== false },
      { step: '3_typo_correction', applied: validationData.wasCorrected, corrected: validationData.currentEmail }
    ];
    
    if (validationData.formatValid) {
      validationSteps.push({ 
        step: '4_invalid_domain_check', 
        passed: !validationData.isInvalidDomain,
        isInvalidDomain: validationData.isInvalidDomain 
      });
    }
    
    // Add MX check step if it was performed
    if (validationData.mxInfo && validationData.mxInfo.checked) {
      validationSteps.push({
        step: '5_mx_record_check',
        passed: validationData.mxRecordsFound,
        hasMxRecords: validationData.mxRecordsFound,
        primaryMx: validationData.mxInfo.primaryMx
      });
    }
    
    const result = {
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
      date_last_um_check: now.toISOString(),
      date_last_um_check_epoch: epochMs,
      um_check_id: umCheckId,
      
      // Suggestions if any
      ...(validationData.suggestions && { suggestions: validationData.suggestions }),
      
      // MX info if available
      ...(validationData.mxInfo && { mxInfo: validationData.mxInfo }),
      
      // Validation steps
      validationSteps: validationData.validationSteps || validationSteps
    };
    
    // Add um_bounce_status only if account type is not 'basic'
    if (accountType !== 'basic') {
      result.um_bounce_status = umBounceStatus;
    }
    
    return result;
  }
  
  // Check if email exists in valid emails database
  async checkEmailCache(email) {
    try {
      // Check if this email has been previously validated as "Unlikely to bounce"
      const result = await db.select('email_validations', 
        { email: email.toLowerCase() }, 
        { 
          limit: 1,
          order: { column: 'date_last_um_check_epoch', ascending: false }
        }
      );
      
      if (!result || !result.rows || result.rows.length === 0) {
        return null;
      }
      
      const data = result.rows[0];
      
      // This email was previously validated as "Unlikely to bounce"
      // Trust it forever - no revalidation needed
      const validationAge = Date.now() - (data.date_last_um_check_epoch || 0);
      
      this.logger.debug('Email found in valid emails database', { 
        email,
        daysSinceValidation: Math.floor(validationAge / (24 * 60 * 60 * 1000))
      });
      
      // Return the valid email data
      return {
        originalEmail: email,
        currentEmail: data.um_email || email,
        formatValid: true,
        wasCorrected: data.um_email !== email,
        status: 'valid', // We only store valid emails
        recheckNeeded: false,
        um_email: data.um_email,
        um_email_status: data.um_email_status,
        um_bounce_status: 'Unlikely to bounce', // Always this value in the database
        date_last_um_check: data.date_last_um_check,
        date_last_um_check_epoch: data.date_last_um_check_epoch,
        um_check_id: data.um_check_id,
        isFromDatabase: true,
        daysSinceValidation: Math.floor(validationAge / (24 * 60 * 60 * 1000)),
        // Add validation steps to show it came from database
        validationSteps: [
          { step: '1_database_check', passed: true, note: 'Email found in database as valid' }
        ]
      };
    } catch (error) {
      this.logger.error('Failed to check email in database', error, { email });
      return null;
    }
  }
  
  // Save valid emails to database (only "Unlikely to bounce" emails)
  async saveEmailCache(email, validationResult, clientId) {
    // Only save emails that are "Unlikely to bounce"
    if (validationResult.status !== 'valid' || validationResult.um_bounce_status !== 'Unlikely to bounce') {
      this.logger.debug('Skipping database save - email is not valid', { 
        email, 
        status: validationResult.status,
        um_bounce_status: validationResult.um_bounce_status 
      });
      return;
    }
    
    try {
      const cacheData = {
        email: email.toLowerCase(),
        um_email: validationResult.um_email || validationResult.currentEmail,
        um_email_status: validationResult.um_email_status,
        um_bounce_status: validationResult.um_bounce_status,
        date_last_um_check: validationResult.date_last_um_check,
        date_last_um_check_epoch: validationResult.date_last_um_check_epoch,
        um_check_id: validationResult.um_check_id,
        updated_at: new Date().toISOString()
      };
      
      // First try to update existing record
      const updateResult = await db.update(
        'email_validations',
        cacheData,
        { email: email.toLowerCase() },
        { returning: false }
      );
      
      // If no record was updated, insert new one
      if (!updateResult || updateResult.rows.length === 0) {
        await db.insert('email_validations', cacheData, { returning: false });
      }
      
      this.logger.debug('Valid email saved to database', { email, clientId });
    } catch (error) {
      // Handle duplicate key errors gracefully
      if (error.code !== '23505') { // PostgreSQL unique violation
        this.logger.error('Failed to save valid email to database', error, { email });
      }
    }
  }
}

// Create singleton instance
const emailValidationService = new EmailValidationService();

// Set up periodic MX cache cleanup (every hour)
setInterval(() => {
  emailValidationService.cleanupMxCache();
}, 3600000);

// Export the class and instance
export { emailValidationService, EmailValidationService };
export default emailValidationService;