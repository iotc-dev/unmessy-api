// src/services/validation/name-validation-service.js
import db from '../../core/db.js';
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
      // Load all normalization data in parallel using proper Supabase methods
      const [
        honorificsData,
        suffixesData,
        particlesData,
        suspiciousData,
        securityData,
        specialCasesData
      ] = await Promise.all([
        db.select('honorifics', {}, { columns: 'honorific' }).catch(() => ({ rows: [] })),
        db.select('suffixes', {}, { columns: 'suffix, formatted' }).catch(() => ({ rows: [] })),
        db.select('name_particles', {}, { columns: 'particle' }).catch(() => ({ rows: [] })),
        db.select('suspicious_names', {}, { columns: 'name' }).catch(() => ({ rows: [] })),
        db.select('security_patterns', {}, { columns: 'pattern' }).catch(() => ({ rows: [] })),
        db.select('special_case_names', {}, { columns: 'name_typo, name_correction' }).catch(() => ({ rows: [] }))
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
       'ap', 'ben', 'bat', 'bint', 'ter', 'ten', 'den', 'der'].forEach(p => 
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
        ['desouza', 'De Souza'], ['delafuente', 'De la Fuente'], ['macassi', 'Macassi']
      ]);
    }
  }
  
  // Basic name format validation - UPDATED TO INCLUDE COMMA
  isValidNameFormat(name) {
    if (typeof name !== 'string') return false;
    
    const trimmedName = name.trim();
    if (trimmedName.length === 0) return false;
    if (trimmedName.length < 2) return false;
    
    // Allow letters, spaces, hyphens, apostrophes, periods, and COMMAS
    const validNameRegex = /^[\p{L}\p{M}'\-\s.,]+$/u;
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
  
  // FIXED: Proper capitalization with special cases
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
    
    // FIXED: Handle McSomething and MacSomething with better logic
    if (loweredName.startsWith('mac') && name.length > 3) {
      // Check if the next letter after 'mac' is uppercase in the original
      // If it is, preserve the original casing pattern
      if (name.length > 3 && name[3] === name[3].toUpperCase()) {
        // Keep original Mac casing (like MacAssi)
        return 'Mac' + name.substring(3);
      } else {
        // Standard Mac capitalization
        return 'Mac' + name.charAt(3).toUpperCase() + name.slice(4).toLowerCase();
      }
    }
    
    if (loweredName.startsWith('mc') && name.length > 2) {
      // Same logic for Mc names
      if (name.length > 2 && name[2] === name[2].toUpperCase()) {
        return 'Mc' + name.substring(2);
      } else {
        return 'Mc' + name.charAt(2).toUpperCase() + name.slice(3).toLowerCase();
      }
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
  
  // FIXED: Parse name components with better particle handling
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
    
    // Handle comma format (Last, First Middle)
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
        // FIXED: Join all remaining parts as middle name
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
    
    // FIXED: Process remaining components with better particle handling
    if (remainingComponents.length === 1) {
      // Only one name component - treat as first name
      result.firstName = remainingComponents[0];
    } else if (remainingComponents.length === 2) {
      // Two components - first and last
      result.firstName = remainingComponents[0];
      result.lastName = remainingComponents[1];
    } else if (remainingComponents.length >= 3) {
      // Three or more components - need to identify where last name starts
      result.firstName = remainingComponents[0];
      
      // Look for particles to determine last name boundary
      let lastNameStartIndex = -1;
      
      // Check each component starting from position 1
      for (let i = 1; i < remainingComponents.length; i++) {
        if (this.isNameParticle(remainingComponents[i])) {
          // Found a particle - this starts the last name
          lastNameStartIndex = i;
          break;
        }
      }
      
      if (lastNameStartIndex > 0) {
        // Particle found - everything before it is middle name(s)
        if (lastNameStartIndex > 1) {
          result.middleName = remainingComponents.slice(1, lastNameStartIndex).join(' ');
        }
        // Everything from particle onwards is last name
        result.lastName = remainingComponents.slice(lastNameStartIndex).join(' ');
      } else {
        // No particles found - treat all middle components as middle name
        // and last component as last name
        result.lastName = remainingComponents[remainingComponents.length - 1];
        if (remainingComponents.length > 2) {
          result.middleName = remainingComponents.slice(1, -1).join(' ');
        }
      }
    }
    
    return result;
  }
  
  // FIXED: Main validation function with proper change tracking
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
        confidenceLevel: 'low',
        wasCorrected: false,
        um_first_name: '',
        um_last_name: '',
        um_middle_name: '',
        um_name: '',
        um_name_status: 'Unchanged',
        um_name_format: 'Invalid',
        um_honorific: '',
        um_suffix: ''
      };
    }
    
    // Sanitize
    const sanitizedName = String(name).trim().replace(/\s+/g, ' ');
    
    // Check format
    const formatValid = this.isValidNameFormat(sanitizedName);
    
    // Track original for comparison
    const originalName = sanitizedName;
    
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
    
    // Store original parsed values for comparison
    const originalFirst = result.firstName;
    const originalMiddle = result.middleName;
    const originalLast = result.lastName;
    
    // Apply proper capitalization
    result.firstName = this.properCapitalize(result.firstName);
    result.lastName = this.properCapitalize(result.lastName, true);
    if (result.middleName) {
      result.middleName = this.properCapitalize(result.middleName);
    }
    
    // FIXED: Check if capitalization or parsing changed anything
    if (result.firstName !== originalFirst || 
        result.lastName !== originalLast || 
        result.middleName !== originalMiddle ||
        result.wasCorrected ||
        result.isCommaFormat) {
      result.wasCorrected = true;
    }
    
    // Check if the original was all uppercase or all lowercase
    if (originalName === originalName.toUpperCase() || originalName === originalName.toLowerCase()) {
      result.wasCorrected = true;
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
    
    // FIXED: Generate unmessy fields including middle name in um_name when present
    result.um_first_name = result.honorific ? 
      `${result.honorific} ${result.firstName}`.trim() : result.firstName;
    result.um_last_name = result.suffix ?
      `${result.lastName} ${result.suffix}`.trim() : result.lastName;
    
    // FIXED: Include middle name in um_name
    const nameComponents = [result.um_first_name];
    if (result.middleName) {
      nameComponents.push(result.middleName);
    }
    if (result.um_last_name) {
      nameComponents.push(result.um_last_name);
    }
    result.um_name = nameComponents.filter(Boolean).join(' ');
    
    result.um_name_status = result.wasCorrected ? 'Changed' : 'Unchanged';
    result.um_name_format = result.formatValid ? 'Valid' : 'Invalid';
    result.um_honorific = result.honorific;
    result.um_suffix = result.suffix;
    result.um_middle_name = result.middleName;
    
    return result;
  }
  
  // FIXED: Validate separate names with proper tracking
  async validateSeparateNames(firstName, lastName, options = {}) {
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
        wasCorrected: false,
        um_first_name: '',
        um_last_name: '',
        um_name: '',
        um_name_status: 'Unchanged',
        um_name_format: 'Invalid',
        um_honorific: '',
        um_suffix: '',
        um_middle_name: ''
      };
    }
    
    // Sanitize inputs
    const sanitizedFirst = firstName ? String(firstName).trim().replace(/\s+/g, ' ') : '';
    const sanitizedLast = lastName ? String(lastName).trim().replace(/\s+/g, ' ') : '';
    
    // Track originals for comparison
    const originalFirst = sanitizedFirst;
    const originalLast = sanitizedLast;
    
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
      
      // Parse first name for honorific and middle name
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
    
    // Store pre-capitalization values
    const processedFirst = result.firstName;
    const processedMiddle = result.middleName;
    const processedLast = result.lastName;
    
    // Apply proper capitalization
    result.firstName = this.properCapitalize(result.firstName);
    result.lastName = this.properCapitalize(result.lastName, true);
    if (result.middleName) {
      result.middleName = this.properCapitalize(result.middleName);
    }
    
    // FIXED: Check if anything changed
    if (result.firstName !== processedFirst || 
        result.lastName !== processedLast || 
        result.middleName !== processedMiddle ||
        result.wasCorrected) {
      result.wasCorrected = true;
    }
    
    // Check if originals were all uppercase or lowercase
    if ((originalFirst && (originalFirst === originalFirst.toUpperCase() || originalFirst === originalFirst.toLowerCase())) ||
        (originalLast && (originalLast === originalLast.toUpperCase() || originalLast === originalLast.toLowerCase()))) {
      result.wasCorrected = true;
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
    
    // Generate unmessy fields including middle name
    result.um_first_name = result.honorific ? 
      `${result.honorific} ${result.firstName}`.trim() : result.firstName;
    result.um_last_name = result.suffix ?
      `${result.lastName} ${result.suffix}`.trim() : result.lastName;
    
    // FIXED: Include middle name in um_name
    const nameComponents = [result.um_first_name];
    if (result.middleName) {
      nameComponents.push(result.middleName);
    }
    if (result.um_last_name) {
      nameComponents.push(result.um_last_name);
    }
    result.um_name = nameComponents.filter(Boolean).join(' ');
    
    result.um_name_status = result.wasCorrected ? 'Changed' : 'Unchanged';
    result.um_name_format = result.formatValid ? 'Valid' : 'Invalid';
    result.um_honorific = result.honorific;
    result.um_suffix = result.suffix;
    result.um_middle_name = result.middleName;
    
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
      const { rows } = await db.select(
        'name_validations',
        { original_name: name },
        { limit: 1 }
      );
      
      const data = rows[0];
      
      if (data) {
        // FIXED: Include middle name in cached um_name
        const nameComponents = [];
        if (data.honorific) {
          nameComponents.push(`${data.honorific} ${data.first_name}`.trim());
        } else {
          nameComponents.push(data.first_name);
        }
        
        if (data.middle_name) {
          nameComponents.push(data.middle_name);
        }
        
        if (data.suffix) {
          nameComponents.push(`${data.last_name} ${data.suffix}`.trim());
        } else {
          nameComponents.push(data.last_name);
        }
        
        const umName = nameComponents.filter(Boolean).join(' ');
        
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
          wasCorrected: false, // Cached data is already processed
          um_first_name: data.honorific ? 
            `${data.honorific} ${data.first_name}`.trim() : data.first_name,
          um_last_name: data.suffix ?
            `${data.last_name} ${data.suffix}`.trim() : data.last_name,
          um_name: umName,
          um_name_status: 'Unchanged',
          um_name_format: data.format_valid ? 'Valid' : 'Invalid',
          um_honorific: data.honorific || '',
          um_suffix: data.suffix || '',
          um_middle_name: data.middle_name || '',
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
      
      await db.insert('name_validations', {
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
      // Handle duplicate key errors gracefully
      if (error.code !== '23505') { // PostgreSQL unique violation
        this.logger.error('Failed to save name validation', error, { name });
      }
    }
  }
}

// Create singleton instance
const nameValidationService = new NameValidationService();

// Export the class and instance
export { nameValidationService, NameValidationService };
export default nameValidationService;