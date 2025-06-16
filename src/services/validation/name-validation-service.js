// src/services/validation/name-validation-service.js
import { db } from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { ValidationError, DatabaseError } from '../../core/errors.js';

const logger = createServiceLogger('name-validation-service');

class NameValidationService {
  constructor() {
    this.logger = logger;
    
    // Initialize reference data
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
        db.query('SELECT honorific FROM honorifics').catch(() => ({ rows: [] })),
        db.query('SELECT suffix, formatted FROM suffixes').catch(() => ({ rows: [] })),
        db.query('SELECT particle FROM name_particles').catch(() => ({ rows: [] })),
        db.query('SELECT name FROM suspicious_names').catch(() => ({ rows: [] })),
        db.query('SELECT pattern FROM security_patterns').catch(() => ({ rows: [] })),
        db.query('SELECT name_typo, name_correction FROM special_case_names').catch(() => ({ rows: [] }))
      ]);
      
      // Populate sets and maps
      if (honorificsData?.rows) {
        honorificsData.rows.forEach(row => this.honorifics.add(row.honorific.toLowerCase()));
      }
      if (suffixesData?.rows) {
        suffixesData.rows.forEach(row => {
          this.suffixes.add(row.suffix.toLowerCase());
          if (row.formatted) {
            this.suffixFormatting.set(row.suffix.toLowerCase(), row.formatted);
          }
        });
      }
      if (particlesData?.rows) {
        particlesData.rows.forEach(row => this.nameParticles.add(row.particle.toLowerCase()));
      }
      if (suspiciousData?.rows) {
        suspiciousData.rows.forEach(row => this.suspiciousNames.add(row.name.toLowerCase()));
      }
      if (securityData?.rows) {
        securityData.rows.forEach(row => this.securityPatterns.add(row.pattern.toLowerCase()));
      }
      if (specialCasesData?.rows) {
        specialCasesData.rows.forEach(row => {
          this.specialCaseCorrections.set(row.name_typo.toLowerCase(), row.name_correction);
        });
      }
      
      // Initialize default data if database is empty
      this.initializeDefaultData();
      
      this.logger.info('Name normalization data loaded', {
        honorifics: this.honorifics.size,
        suffixes: this.suffixes.size,
        particles: this.nameParticles.size,
        suspicious: this.suspiciousNames.size,
        security: this.securityPatterns.size,
        specialCases: this.specialCaseCorrections.size
      });
    } catch (error) {
      this.logger.error('Failed to load normalization data', error);
      // Initialize with defaults
      this.initializeDefaultData();
    }
  }
  
  initializeDefaultData() {
    // Default honorifics if not loaded from DB
    if (this.honorifics.size === 0) {
      ['mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'rev', 'hon', 'sir', 'madam', 
       'lord', 'lady', 'capt', 'major', 'col', 'lt', 'cmdr', 'sgt'].forEach(h => 
        this.honorifics.add(h)
      );
    }
    
    // Default suffixes
    if (this.suffixes.size === 0) {
      ['jr', 'sr', 'i', 'ii', 'iii', 'iv', 'v', 'phd', 'md', 'dds', 'esq'].forEach(s => 
        this.suffixes.add(s)
      );
    }
    
    // Default suffix formatting
    if (this.suffixFormatting.size === 0) {
      this.suffixFormatting = new Map([
        ['jr', 'Jr.'], ['sr', 'Sr.'], ['i', 'I'], ['ii', 'II'], 
        ['iii', 'III'], ['iv', 'IV'], ['v', 'V'], ['phd', 'Ph.D.'],
        ['md', 'M.D.'], ['dds', 'D.D.S.'], ['esq', 'Esq.']
      ]);
    }
    
    // Default name particles
    if (this.nameParticles.size === 0) {
      ['von', 'van', 'de', 'del', 'della', 'di', 'da', 'do', 'dos', 'das', 'du', 
       'la', 'le', 'el', 'les', 'lo', 'mac', 'mc', "o'", 'al', 'bin', 'ibn', 
       'ap', 'ben', 'bat', 'bint'].forEach(p => 
        this.nameParticles.add(p)
      );
    }
    
    // Default suspicious names
    if (this.suspiciousNames.size === 0) {
      ['test', 'user', 'admin', 'sample', 'demo', 'fake', 'anonymous', 'unknown', 
       'noreply', 'example', 'null', 'undefined', 'n/a', 'na', 'none', 'blank'].forEach(s => 
        this.suspiciousNames.add(s)
      );
    }
    
    // Default security patterns
    if (this.securityPatterns.size === 0) {
      [');', '--', '/*', '*/', ';', 'drop', 'select', 'insert', 'update', 'delete', 
       'union', 'script', '<>'].forEach(p => 
        this.securityPatterns.add(p)
      );
    }
    
    // Default special cases
    if (this.specialCaseCorrections.size === 0) {
      this.specialCaseCorrections = new Map([
        ['obrien', "O'Brien"], ['oneill', "O'Neill"], ['odonnell', "O'Donnell"],
        ['mcdonald', 'McDonald'], ['macleod', 'MacLeod'], ['vanhalen', 'Van Halen'],
        ['desouza', 'De Souza'], ['delafuente', 'De la Fuente']
      ]);
    }
  }
  
  // Basic name format validation
  isValidNameFormat(name) {
    if (typeof name !== 'string') return false;
    
    const trimmedName = name.trim();
    if (trimmedName.length === 0) return false;
    if (trimmedName.length < 2) return false;
    
    // Allow letters, spaces, hyphens, apostrophes, periods
    const validNameRegex = /^[\p{L}\p{M}'\-\s.]+$/u;
    return validNameRegex.test(trimmedName);
  }
  
  // Detect script/language
  detectScript(text) {
    if (!text) return 'unknown';
    
    const scripts = {
      cyrillic: /[\u0400-\u04FF]/,
      greek: /[\u0370-\u03FF]/,
      hebrew: /[\u0590-\u05FF]/,
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
    
    // Check for non-Latin
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
  
  // Check if component is a name particle
  isNameParticle(component) {
    return this.nameParticles.has(component.toLowerCase());
  }
  
  // Proper capitalization with special cases
  properCapitalize(name, isLastName = false) {
    if (!name) return '';
    
    // Skip for non-Latin scripts
    const script = this.detectScript(name);
    if (script !== 'latin' && script !== 'unknown') {
      return name;
    }
    
    // Handle hyphenated names
    if (name.includes('-')) {
      return name.split('-')
        .map(part => this.properCapitalize(part, isLastName))
        .join('-');
    }
    
    // Check special case corrections
    const loweredName = name.toLowerCase();
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
  
  // Parse name components
  parseNameComponents(name) {
    const result = {
      firstName: '',
      lastName: '',
      middleName: '',
      honorific: '',
      suffix: '',
      isCommaFormat: false,
      wasCorrected: false
    };
    
    // Handle comma format (Last, First)
    if (name.includes(',')) {
      result.isCommaFormat = true;
      const parts = name.split(',').map(p => p.trim());
      
      if (parts.length !== 2) {
        return result;
      }
      
      const [lastPart, firstPart] = parts;
      const lastComponents = lastPart.split(' ').filter(Boolean);
      const firstComponents = firstPart.split(' ').filter(Boolean);
      
      // Check for suffix in last part
      if (lastComponents.length > 1) {
        const lastComponent = lastComponents[lastComponents.length - 1].toLowerCase().replace(/\.$/, '');
        if (this.suffixes.has(lastComponent)) {
          result.suffix = this.formatSuffix(lastComponent);
          result.lastName = lastComponents.slice(0, -1).join(' ');
        } else {
          result.lastName = lastPart;
        }
      } else {
        result.lastName = lastPart;
      }
      
      // Process first part
      if (firstComponents.length === 0) {
        return result;
      }
      
      // Check for honorific
      const firstComponent = firstComponents[0].toLowerCase().replace(/\.$/, '');
      if (this.honorifics.has(firstComponent)) {
        result.honorific = this.properCapitalize(firstComponents[0]);
        firstComponents.shift();
      }
      
      if (firstComponents.length === 1) {
        result.firstName = firstComponents[0];
      } else if (firstComponents.length > 1) {
        result.firstName = firstComponents[0];
        result.middleName = firstComponents.slice(1).join(' ');
      }
      
      result.wasCorrected = true;
      return result;
    }
    
    // Process regular format
    const components = name.split(' ').filter(Boolean);
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
    if (remainingComponents.length === 1) {
      result.firstName = remainingComponents[0];
    } else if (remainingComponents.length === 2) {
      result.firstName = remainingComponents[0];
      result.lastName = remainingComponents[1];
    } else if (remainingComponents.length >= 3) {
      result.firstName = remainingComponents[0];
      
      // Check for name particle in middle position
      const middleIndex = 1;
      if (middleIndex < remainingComponents.length - 1 && 
          this.isNameParticle(remainingComponents[middleIndex])) {
        // Particle is part of last name
        const particle = remainingComponents[middleIndex].toLowerCase();
        const actualLastName = remainingComponents.slice(middleIndex + 1).join(' ');
        const capitalizedLastName = this.properCapitalize(actualLastName, true);
        result.lastName = `${particle} ${capitalizedLastName}`;
      } else {
        // Standard case
        result.lastName = remainingComponents[remainingComponents.length - 1];
        if (remainingComponents.length > 2) {
          result.middleName = remainingComponents.slice(1, -1).join(' ');
        }
      }
    }
    
    return result;
  }
  
  // Main validation function
  validateName(name) {
    this.logger.debug('Starting name validation', { name });
    
    // Handle null/empty
    if (!name || name === '') {
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
    const sanitizedName = String(name).trim().replace(/\s+/g, ' ');
    
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
      return result;
    }
    
    // Security check
    if (this.containsSecurityThreat(sanitizedName)) {
      result.status = 'invalid';
      result.subStatus = 'security_risk';
      result.potentialIssues.push('Potential security threat detected');
      result.confidenceLevel = 'low';
      return result;
    }
    
    // Check for suspicious names
    const lowerName = sanitizedName.toLowerCase();
    for (const suspicious of this.suspiciousNames) {
      if (lowerName.includes(suspicious)) {
        result.potentialIssues.push('May be a test or placeholder name');
        result.confidenceLevel = 'low';
        break;
      }
    }
    
    // Parse components
    const parsed = this.parseNameComponents(sanitizedName);
    Object.assign(result, parsed);
    
    // Apply proper capitalization
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
    
    // Generate unmessy fields
    result.um_first_name = result.honorific ? 
      `${result.honorific} ${result.firstName}`.trim() : result.firstName;
    result.um_last_name = result.suffix ?
      `${result.lastName} ${result.suffix}`.trim() : result.lastName;
    result.um_name = `${result.um_first_name} ${result.um_last_name}`.trim();
    result.um_name_status = result.wasCorrected ? 'Changed' : 'Unchanged';
    result.um_name_format = result.formatValid ? 'Valid' : 'Invalid';
    
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
        confidenceLevel: 'low',
        um_first_name: '',
        um_last_name: '',
        um_name: '',
        um_name_status: 'Unchanged',
        um_name_format: 'Invalid'
      };
    }
    
    // Sanitize inputs
    const sanitizedFirst = firstName ? String(firstName).trim().replace(/\s+/g, ' ') : '';
    const sanitizedLast = lastName ? String(lastName).trim().replace(/\s+/g, ' ') : '';
    
    // Reconstruct full name
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
      isCommaFormat: false,
      wasCorrected: false
    };
    
    // Set script
    if (firstNameScript !== 'latin' && firstNameScript !== 'unknown') {
      result.script = firstNameScript;
    } else if (lastNameScript !== 'latin' && lastNameScript !== 'unknown') {
      result.script = lastNameScript;
    }
    
    // Process first name
    if (sanitizedFirst) {
      // Security check
      if (this.containsSecurityThreat(sanitizedFirst)) {
        result.potentialIssues.push('First name may contain code or SQL patterns');
        result.confidenceLevel = 'low';
        result.status = 'invalid';
        result.subStatus = 'security_risk';
        return result;
      }
      
      // Check suspicious names
      for (const fake of this.suspiciousNames) {
        if (sanitizedFirst.toLowerCase().includes(fake)) {
          result.potentialIssues.push('First name may be a test or placeholder');
          result.confidenceLevel = 'low';
          break;
        }
      }
      
      // Parse first name for honorific
      const firstNameParts = sanitizedFirst.split(' ');
      if (firstNameParts.length > 1) {
        const firstComponent = firstNameParts[0].toLowerCase().replace(/\.$/, '');
        if (this.honorifics.has(firstComponent)) {
          result.honorific = this.properCapitalize(firstNameParts[0]);
          result.firstName = firstNameParts.slice(1).join(' ');
          result.wasCorrected = true;
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
      // Security check
      if (this.containsSecurityThreat(sanitizedLast)) {
        result.potentialIssues.push('Last name may contain code or SQL patterns');
        result.confidenceLevel = 'low';
        result.status = 'invalid';
        result.subStatus = 'security_risk';
        return result;
      }
      
      // Check suspicious names
      for (const fake of this.suspiciousNames) {
        if (sanitizedLast.toLowerCase().includes(fake)) {
          result.potentialIssues.push('Last name may be a test or placeholder');
          result.confidenceLevel = 'low';
          break;
        }
      }
      
      // Check for suffix in last name
      const lastNameParts = sanitizedLast.split(' ');
      if (lastNameParts.length > 1) {
        const lastComponent = lastNameParts[lastNameParts.length - 1].toLowerCase().replace(/\.$/, '').replace(/,/g, '');
        if (this.suffixes.has(lastComponent) || lastComponent.startsWith('jr') || lastComponent.startsWith('sr')) {
          result.suffix = this.formatSuffix(lastComponent);
          result.lastName = lastNameParts.slice(0, -1).join(' ');
          result.wasCorrected = true;
        } else {
          result.lastName = sanitizedLast;
        }
      } else {
        result.lastName = sanitizedLast;
      }
    }
    
    // Apply proper capitalization
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
    
    // Generate unmessy fields
    result.um_first_name = result.honorific ? 
      `${result.honorific} ${result.firstName}`.trim() : result.firstName;
    result.um_last_name = result.suffix ?
      `${result.lastName} ${result.suffix}`.trim() : result.lastName;
    result.um_name = `${result.um_first_name} ${result.um_last_name}`.trim();
    result.um_name_status = result.wasCorrected ? 'Changed' : 'Unchanged';
    result.um_name_format = result.formatValid ? 'Valid' : 'Invalid';
    
    return result;
  }
  
  // Validate full name (wrapper for consistency)
  async validateFullName(name, options = {}) {
    const { useCache = true, clientId = null } = options;
    
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
  
  // Cache operations
  async checkNameCache(name) {
    try {
      const { data, error } = await db.getNameValidation(name);
      
      if (data) {
        return {
          originalName: data.original_name,
          currentName: data.original_name,
          firstName: data.first_name || '',
          lastName: data.last_name || '',
          middleName: data.middle_name || '',
          honorific: data.honorific || '',
          suffix: data.suffix || '',
          script: data.script || 'latin',
          formatValid: data.format_valid !== false,
          status: data.validation_status || 'valid',
          subStatus: data.validation_sub_status,
          potentialIssues: data.potential_issues ? 
            JSON.parse(data.potential_issues) : [],
          confidenceLevel: data.confidence_level,
          um_first_name: data.honorific ? 
            `${data.honorific} ${data.first_name}`.trim() : data.first_name,
          um_last_name: data.suffix ?
            `${data.last_name} ${data.suffix}`.trim() : data.last_name,
          um_name: `${data.first_name} ${data.last_name}`.trim(),
          um_name_status: 'Unchanged',
          um_name_format: data.format_valid ? 'Valid' : 'Invalid',
          cacheDate: data.date_validated,
          cacheDateEpochMs: data.date_validated_epoch_ms
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
      
      await db.saveNameValidation({
        original_name: name,
        first_name: validationResult.firstName,
        last_name: validationResult.lastName,
        middle_name: validationResult.middleName || null,
        honorific: validationResult.honorific || null,
        suffix: validationResult.suffix || null,
        validation_status: validationResult.status,
        validation_sub_status: validationResult.subStatus || null,
        format_valid: validationResult.formatValid,
        confidence_level: validationResult.confidenceLevel,
        script: validationResult.script,
        potential_issues: validationResult.potentialIssues.length > 0 ? 
          JSON.stringify(validationResult.potentialIssues) : null,
        date_validated: now,
        date_validated_epoch_ms: epochMs,
        client_id: clientId
      });
      
      this.logger.debug('Name validation saved to cache', { name, clientId });
    } catch (error) {
      this.logger.error('Failed to save name validation', error, { name });
    }
  }
}

// Export the class for use in validation-service.js
export { NameValidationService };