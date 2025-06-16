// src/services/validation/name-validation-service.js
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { ValidationError, DatabaseError } from '../../core/errors.js';

const logger = createServiceLogger('name-validation-service');

class NameValidationService {
  constructor() {
    this.logger = logger;
    
    // Load initial data
    this.honorifics = new Set();
    this.suffixes = new Set();
    this.suffixFormatting = new Map();
    this.nameParticles = new Set();
    this.suspiciousNames = new Set();
    this.securityPatterns = new Set();
    this.specialCaseCorrections = new Map();
    
    // Load normalization data on startup
    this.loadNormalizationData();
  }
  
  async loadNormalizationData() {
    try {
      // Load all normalization data in parallel
      const [
        honorificsData,
        suffixesData,
        particlesData,
        suspiciousData,
        securityData,
        specialCasesData
      ] = await Promise.all([
        db.query('SELECT honorific FROM honorifics'),
        db.query('SELECT suffix FROM suffixes'),
        db.query('SELECT particle FROM name_particles'),
        db.query('SELECT name FROM suspicious_names'),
        db.query('SELECT pattern FROM security_patterns'),
        db.query('SELECT name_typo, name_correction FROM special_case_names')
      ]);
      
      // Populate sets and maps
      honorificsData.rows.forEach(row => this.honorifics.add(row.honorific.toLowerCase()));
      suffixesData.rows.forEach(row => this.suffixes.add(row.suffix.toLowerCase()));
      particlesData.rows.forEach(row => this.nameParticles.add(row.particle.toLowerCase()));
      suspiciousData.rows.forEach(row => this.suspiciousNames.add(row.name.toLowerCase()));
      securityData.rows.forEach(row => this.securityPatterns.add(row.pattern.toLowerCase()));
      specialCasesData.rows.forEach(row => {
        this.specialCaseCorrections.set(row.name_typo.toLowerCase(), row.name_correction);
      });
      
      // Initialize suffix formatting
      this.initializeSuffixFormatting();
      
      this.logger.info('Normalization data loaded', {
        honorifics: this.honorifics.size,
        suffixes: this.suffixes.size,
        particles: this.nameParticles.size,
        suspiciousNames: this.suspiciousNames.size,
        securityPatterns: this.securityPatterns.size,
        specialCases: this.specialCaseCorrections.size
      });
    } catch (error) {
      this.logger.error('Failed to load normalization data', error);
      // Initialize with defaults if DB fails
      this.initializeDefaults();
    }
  }
  
  initializeDefaults() {
    // Default honorifics
    ['mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'rev'].forEach(h => this.honorifics.add(h));
    
    // Default suffixes
    ['jr', 'sr', 'i', 'ii', 'iii', 'iv', 'v', 'phd', 'md'].forEach(s => this.suffixes.add(s));
    
    // Default particles
    ['von', 'van', 'de', 'del', 'della', 'di', 'da'].forEach(p => this.nameParticles.add(p));
    
    // Default suspicious names
    ['test', 'user', 'admin', 'sample', 'demo', 'fake'].forEach(n => this.suspiciousNames.add(n));
    
    // Default security patterns
    [');', '--', '/*', '*/', 'drop', 'select', 'insert'].forEach(p => this.securityPatterns.add(p));
    
    this.initializeSuffixFormatting();
  }
  
  initializeSuffixFormatting() {
    this.suffixFormatting.set('jr', 'Jr.');
    this.suffixFormatting.set('sr', 'Sr.');
    this.suffixFormatting.set('i', 'I');
    this.suffixFormatting.set('ii', 'II');
    this.suffixFormatting.set('iii', 'III');
    this.suffixFormatting.set('iv', 'IV');
    this.suffixFormatting.set('v', 'V');
    this.suffixFormatting.set('phd', 'Ph.D.');
    this.suffixFormatting.set('md', 'M.D.');
    this.suffixFormatting.set('dds', 'D.D.S.');
    this.suffixFormatting.set('esq', 'Esq.');
  }
  
  // Check if a word is a name particle
  isNameParticle(word) {
    return this.nameParticles.has(word.toLowerCase());
  }
  
  // Detect script/language
  detectScript(text) {
    if (!text || typeof text !== 'string') return 'unknown';
    
    if (text.includes('�') || /\uFFFD/.test(text)) {
      return 'encoding-issue';
    }
    
    const scripts = {
      cyrillic: /[\u0400-\u04FF]/,
      devanagari: /[\u0900-\u097F]/,
      arabic: /[\u0600-\u06FF\u0750-\u077F]/,
      han: /[\u4E00-\u9FFF\u3400-\u4DBF]/,
      hiragana: /[\u3040-\u309F]/,
      katakana: /[\u30A0-\u30FF]/,
      hangul: /[\uAC00-\uD7AF\u1100-\u11FF]/,
      thai: /[\u0E00-\u0E7F]/
    };
    
    for (const [script, regex] of Object.entries(scripts)) {
      if (regex.test(text)) {
        return script;
      }
    }
    
    if (/[^\u0000-\u007F]/.test(text)) {
      return 'non-latin';
    }
    
    return 'latin';
  }
  
  // Check for security threats
  containsSecurityThreat(text) {
    if (!text || typeof text !== 'string') return false;
    
    const lowered = text.toLowerCase();
    for (const pattern of this.securityPatterns) {
      if (lowered.includes(pattern)) {
        return true;
      }
    }
    
    return false;
  }
  
  // Format suffix correctly
  formatSuffix(suffix) {
    if (!suffix) return '';
    
    const cleanSuffix = suffix.toLowerCase().replace(/\.$/, '').replace(/,/g, '');
    
    if (this.suffixFormatting.has(cleanSuffix)) {
      return this.suffixFormatting.get(cleanSuffix);
    }
    
    if (cleanSuffix.startsWith('jr')) return 'Jr.';
    if (cleanSuffix.startsWith('sr')) return 'Sr.';
    
    return cleanSuffix.charAt(0).toUpperCase() + cleanSuffix.slice(1).toLowerCase() + '.';
  }
  
  // Proper capitalization
  properCapitalize(name, isLastName = false) {
    if (!name) return '';
    
    const script = this.detectScript(name);
    if (script !== 'latin' && script !== 'unknown') {
      return name;
    }
    
    if (name.includes('-')) {
      return name.split('-')
        .map(part => this.properCapitalize(part, isLastName))
        .join('-');
    }
    
    const loweredName = name.toLowerCase();
    
    // Check special cases
    if (this.specialCaseCorrections.has(loweredName)) {
      return this.specialCaseCorrections.get(loweredName);
    }
    
    // Handle McSomething and MacSomething
    if ((loweredName.startsWith('mc') || loweredName.startsWith('mac')) && name.length > 3) {
      const prefix = name.substring(0, loweredName.startsWith('mac') ? 3 : 2);
      const rest = name.substring(loweredName.startsWith('mac') ? 3 : 2);
      return prefix.charAt(0).toUpperCase() + prefix.slice(1).toLowerCase() + 
             rest.charAt(0).toUpperCase() + rest.slice(1).toLowerCase();
    }
    
    // Handle O'Something
    if (loweredName.startsWith("o'") && name.length > 2) {
      return "O'" + name.charAt(2).toUpperCase() + name.slice(3).toLowerCase();
    }
    
    // Handle apostrophes in the middle
    if (name.includes("'") && !loweredName.startsWith("o'")) {
      const parts = name.split("'");
      if (parts.length >= 2) {
        return parts[0].charAt(0).toUpperCase() + parts[0].slice(1).toLowerCase() + 
               "'" + (parts[1].charAt(0).toUpperCase() + parts[1].slice(1).toLowerCase());
      }
    }
    
    // Handle name particles
    for (const particle of this.nameParticles) {
      if (loweredName === particle) {
        return isLastName ? 
          particle.charAt(0).toUpperCase() + particle.slice(1).toLowerCase() : 
          particle.toLowerCase();
      }
      
      if (loweredName.startsWith(particle.toLowerCase() + ' ')) {
        const particlePart = isLastName ? 
          particle.charAt(0).toUpperCase() + particle.slice(1).toLowerCase() : 
          particle.toLowerCase();
        
        const remainingPart = name.slice(particle.length + 1);
        return `${particlePart} ${this.properCapitalize(remainingPart, isLastName)}`;
      }
    }
    
    // Default capitalization
    return name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
  }
  
  // Validate full name
  async validateFullName(name, options = {}) {
    const { useCache = true, clientId = null } = options;
    
    this.logger.debug('Validating full name', { name, useCache, clientId });
    
    // Check cache first
    if (useCache) {
      const cached = await this.checkNameCache(name);
      if (cached) {
        this.logger.debug('Name found in cache', { name });
        return { ...cached, isFromCache: true };
      }
    }
    
    // Perform validation
    const result = this.validateName(name);
    
    // Save to cache if valid
    if (useCache && result.status === 'valid') {
      await this.saveNameCache(name, result, clientId);
    }
    
    return result;
  }
  
  // Validate separate names
  async validateSeparateNames(firstName, lastName) {
    this.logger.debug('Validating separate names', { firstName, lastName });
    
    // Handle null/empty
    if ((!firstName && !lastName) || (firstName === '' && lastName === '')) {
      return {
        originalName: '',
        currentName: '',
        firstName: '',
        lastName: '',
        middleName: '',
        honorific: '',
        suffix: '',
        script: 'unknown',
        formatValid: false,
        status: 'invalid',
        subStatus: 'empty_name',
        potentialIssues: ['Null or empty name components'],
        confidenceLevel: 'low'
      };
    }
    
    // Sanitize inputs
    const sanitizedFirst = firstName ? String(firstName).trim().replace(/\s+/g, ' ') : '';
    const sanitizedLast = lastName ? String(lastName).trim().replace(/\s+/g, ' ') : '';
    
    const fullName = [sanitizedFirst, sanitizedLast].filter(Boolean).join(' ');
    
    // Check scripts
    const firstNameScript = this.detectScript(sanitizedFirst);
    const lastNameScript = this.detectScript(sanitizedLast);
    
    const result = {
      originalName: fullName,
      currentName: fullName,
      firstName: '',
      lastName: '',
      middleName: '',
      honorific: '',
      suffix: '',
      script: 'latin',
      formatValid: true,
      status: 'valid',
      subStatus: 'valid_format',
      potentialIssues: [],
      confidenceLevel: 'high',
      isCommaFormat: false
    };
    
    // Set script
    if (firstNameScript !== 'latin' && firstNameScript !== 'unknown') {
      result.script = firstNameScript;
    } else if (lastNameScript !== 'latin' && lastNameScript !== 'unknown') {
      result.script = lastNameScript;
    }
    
    // Process first name
    if (sanitizedFirst) {
      if (this.containsSecurityThreat(sanitizedFirst)) {
        result.potentialIssues.push('First name may contain code or SQL patterns');
        result.confidenceLevel = 'low';
        result.status = 'invalid';
        result.subStatus = 'security_risk';
        return result;
      }
      
      // Check for suspicious names
      for (const fake of this.suspiciousNames) {
        if (sanitizedFirst.toLowerCase().includes(fake)) {
          result.potentialIssues.push('First name may be a test or placeholder');
          result.confidenceLevel = 'low';
          break;
        }
      }
      
      // Check for honorific
      const firstNameParts = sanitizedFirst.split(' ');
      if (firstNameParts.length > 1) {
        const firstComponent = firstNameParts[0].toLowerCase().replace(/\.$/, '');
        if (this.honorifics.has(firstComponent)) {
          result.honorific = this.properCapitalize(firstNameParts[0]);
          result.firstName = firstNameParts.slice(1).join(' ');
        } else {
          result.firstName = firstNameParts[0];
          if (firstNameParts.length > 1) {
            result.middleName = firstNameParts.slice(1).join(' ');
          }
        }
      } else {
        result.firstName = sanitizedFirst;
      }
    }
    
    // Process last name
    if (sanitizedLast) {
      if (this.containsSecurityThreat(sanitizedLast)) {
        result.potentialIssues.push('Last name may contain code or SQL patterns');
        result.confidenceLevel = 'low';
        result.status = 'invalid';
        result.subStatus = 'security_risk';
        return result;
      }
      
      // Check for suspicious names
      for (const fake of this.suspiciousNames) {
        if (sanitizedLast.toLowerCase().includes(fake)) {
          result.potentialIssues.push('Last name may be a test or placeholder');
          result.confidenceLevel = 'low';
          break;
        }
      }
      
      // Check for suffix
      const lastNameParts = sanitizedLast.split(' ');
      if (lastNameParts.length > 1) {
        const lastComponent = lastNameParts[lastNameParts.length - 1].toLowerCase().replace(/\.$/, '').replace(/,/g, '');
        
        if (this.suffixes.has(lastComponent) || lastComponent.startsWith('jr') || lastComponent.startsWith('sr')) {
          result.suffix = this.formatSuffix(lastComponent);
          result.lastName = lastNameParts.slice(0, -1).join(' ');
        } else {
          result.lastName = sanitizedLast;
        }
      } else {
        result.lastName = sanitizedLast;
      }
    }
    
    // Apply capitalization
    result.firstName = this.properCapitalize(result.firstName);
    result.lastName = this.properCapitalize(result.lastName, true);
    
    if (result.middleName) {
      result.middleName = this.properCapitalize(result.middleName);
    }
    
    // Validation checks
    if (!result.firstName && !result.lastName) {
      result.formatValid = false;
      result.status = 'invalid';
      result.subStatus = 'invalid_format';
      result.potentialIssues.push('Missing both first and last name');
      result.confidenceLevel = 'low';
    } else if (!result.firstName) {
      result.potentialIssues.push('Missing first name');
      result.confidenceLevel = 'medium';
    } else if (!result.lastName) {
      result.potentialIssues.push('Missing last name');
      result.confidenceLevel = 'medium';
    }
    
    return result;
  }
  
  // Main validation function
  validateName(name) {
    this.logger.debug('Starting name validation', { name });
    
    // Handle null/empty
    if (name === null || name === undefined || name === '') {
      return {
        originalName: name || '',
        currentName: '',
        firstName: '',
        lastName: '',
        middleName: '',
        honorific: '',
        suffix: '',
        script: 'unknown',
        formatValid: false,
        status: 'invalid',
        subStatus: 'empty_name',
        potentialIssues: ['Null or empty name'],
        confidenceLevel: 'low'
      };
    }
    
    // Sanitize
    let inputStr = String(name).trim();
    const sanitizedName = inputStr.replace(/\s+/g, ' ');
    
    // Check format
    const formatValid = this.isValidNameFormat(sanitizedName);
    
    const result = {
      originalName: name,
      currentName: sanitizedName,
      firstName: '',
      lastName: '',
      middleName: '',
      honorific: '',
      suffix: '',
      script: this.detectScript(sanitizedName),
      formatValid: formatValid,
      status: formatValid ? 'valid' : 'invalid',
      subStatus: formatValid ? 'valid_format' : 'invalid_format',
      potentialIssues: [],
      confidenceLevel: 'high',
      isCommaFormat: false,
      wasCorrected: false
    };
    
    if (!formatValid) {
      result.potentialIssues.push('Invalid name format');
      result.confidenceLevel = 'low';
      return result;
    }
    
    // Check for suspicious names
    for (const fake of this.suspiciousNames) {
      if (sanitizedName.toLowerCase().includes(fake)) {
        result.potentialIssues.push('Name may be a test or placeholder');
        result.confidenceLevel = 'low';
        break;
      }
    }
    
    // Check for security threats
    if (this.containsSecurityThreat(sanitizedName)) {
      result.potentialIssues.push('Name may contain code or SQL patterns');
      result.confidenceLevel = 'low';
      result.status = 'invalid';
      result.subStatus = 'security_risk';
      return result;
    }
    
    // Process the name
    let nameToProcess = result.currentName;
    
    // Handle comma format
    if (nameToProcess.includes(',')) {
      result.isCommaFormat = true;
      const parts = nameToProcess.split(',').map(p => p.trim());
      
      result.lastName = parts[0];
      
      if (parts.length > 1) {
        const remainingParts = parts[1].split(' ').filter(Boolean);
        if (remainingParts.length === 1) {
          result.firstName = remainingParts[0];
        } else if (remainingParts.length > 1) {
          result.firstName = remainingParts[0];
          result.middleName = remainingParts.slice(1).join(' ');
        }
      }
      
      result.lastName = this.properCapitalize(result.lastName, true);
      result.firstName = this.properCapitalize(result.firstName);
      result.middleName = this.properCapitalize(result.middleName);
      
      result.wasCorrected = true;
      return result;
    }
    
    // Split into components
    const components = nameToProcess.split(' ').filter(Boolean);
    let remainingComponents = [...components];
    
    // Check for honorific
    if (components.length > 1) {
      const firstComponent = components[0].toLowerCase().replace(/\.$/, '');
      if (this.honorifics.has(firstComponent)) {
        result.honorific = this.properCapitalize(components[0]);
        remainingComponents.shift();
        result.wasCorrected = true;
      }
    }
    
    // Check for suffix
    if (remainingComponents.length > 1) {
      const lastComponent = remainingComponents[remainingComponents.length - 1].toLowerCase().replace(/\.$/, '').replace(/,/g, '');
      if (this.suffixes.has(lastComponent) || lastComponent.startsWith('jr') || lastComponent.startsWith('sr')) {
        result.suffix = this.formatSuffix(lastComponent);
        remainingComponents.pop();
        result.wasCorrected = true;
      }
    }
    
    // Process remaining components
    if (remainingComponents.length === 0) {
      result.potentialIssues.push('Name consists of only honorifics/suffixes');
      result.confidenceLevel = 'low';
      return result;
    }
    
    if (remainingComponents.length === 1) {
      result.firstName = this.properCapitalize(remainingComponents[0]);
      result.potentialIssues.push('Only a single name was provided');
      result.confidenceLevel = 'medium';
    } else if (remainingComponents.length === 2) {
      result.firstName = this.properCapitalize(remainingComponents[0]);
      result.lastName = this.properCapitalize(remainingComponents[1], true);
    } else {
      // Handle particles
      result.firstName = this.properCapitalize(remainingComponents[0]);
      
      const middleIndex = 1;
      if (middleIndex < remainingComponents.length - 1 && 
          this.isNameParticle(remainingComponents[middleIndex])) {
        const particle = remainingComponents[middleIndex].toLowerCase();
        const actualLastName = remainingComponents.slice(middleIndex + 1).join(' ');
        const capitalizedLastName = this.properCapitalize(actualLastName, true);
        result.lastName = `${particle} ${capitalizedLastName}`;
      } else {
        result.lastName = this.properCapitalize(remainingComponents[remainingComponents.length - 1], true);
        if (remainingComponents.length > 2) {
          result.middleName = remainingComponents
            .slice(1, remainingComponents.length - 1)
            .map(name => this.properCapitalize(name))
            .join(' ');
        }
      }
    }
    
    // Check if any capitalization was changed
    if (result.originalName !== result.currentName) {
      result.wasCorrected = true;
    }
    
    return result;
  }
  
  // Check name format
  isValidNameFormat(name) {
    if (typeof name !== 'string') return false;
    
    const trimmedName = name.trim();
    if (trimmedName.length === 0) return false;
    if (trimmedName.length < 2) return false;
    
    const validNameRegex = /^[\p{L}\p{M}'\-\s.]+$/u;
    return validNameRegex.test(trimmedName);
  }
  
  // Cache operations
  async checkNameCache(name) {
    try {
      const result = await db.query(
        'SELECT * FROM name_validations WHERE original_name = $1',
        [name]
      );
      
      if (result.rows.length > 0) {
        const cached = result.rows[0];
        return {
          originalName: cached.original_name,
          firstName: cached.first_name,
          lastName: cached.last_name,
          middleName: cached.middle_name,
          honorific: cached.honorific,
          suffix: cached.suffix,
          script: cached.script,
          formatValid: cached.format_valid,
          status: cached.validation_status,
          subStatus: cached.validation_sub_status,
          potentialIssues: cached.potential_issues ? JSON.parse(cached.potential_issues) : [],
          confidenceLevel: cached.confidence_level,
          cacheDate: cached.date_validated,
          cacheDateEpochMs: cached.date_validated_epoch_ms
        };
      }
      
      return null;
    } catch (error) {
      this.logger.error('Failed to check name cache', error, { name });
      return null;
    }
  }
  
  async saveNameCache(name, validationResult, clientId) {
    try {
      const now = new Date();
      const epochMs = now.getTime();
      
      await db.query(`
        INSERT INTO name_validations (
          original_name, first_name, last_name, middle_name,
          honorific, suffix, validation_status, validation_sub_status,
          format_valid, confidence_level, script, potential_issues,
          date_validated, date_validated_epoch_ms, client_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (original_name) DO UPDATE SET
          first_name = $2, last_name = $3, middle_name = $4,
          honorific = $5, suffix = $6, validation_status = $7,
          validation_sub_status = $8, format_valid = $9,
          confidence_level = $10, script = $11, potential_issues = $12,
          date_validated = $13, date_validated_epoch_ms = $14,
          updated_at = CURRENT_TIMESTAMP
      `, [
        name,
        validationResult.firstName,
        validationResult.lastName,
        validationResult.middleName || null,
        validationResult.honorific || null,
        validationResult.suffix || null,
        validationResult.status,
        validationResult.subStatus || null,
        validationResult.formatValid,
        validationResult.confidenceLevel,
        validationResult.script,
        validationResult.potentialIssues.length > 0 ? JSON.stringify(validationResult.potentialIssues) : null,
        now,
        epochMs,
        clientId
      ]);
      
      this.logger.debug('Name validation saved to cache', { name, clientId });
    } catch (error) {
      this.logger.error('Failed to save name validation', error, { name });
    }
  }
}

// Create singleton instance
const nameValidationService = new NameValidationService();

export { nameValidationService, NameValidationService };