// src/services/validation/phone-validation-service.js
import db from '../../core/db.js';
import { config } from '../../core/config.js';
import { createServiceLogger } from '../../core/logger.js';
import { ValidationError } from '../../core/errors.js';
import { 
  parsePhoneNumber, 
  parsePhoneNumberFromString,
  getCountryCallingCode, 
  getCountries, 
  ParseError 
} from 'libphonenumber-js';

const logger = createServiceLogger('phone-validation-service');

class PhoneValidationService {
  constructor() {
    this.logger = logger;
    this.logger.info('Phone validation service initialized using libphonenumber-js');
    
    // Initialize country mappings
    this.initializeCountryMappings();
  }
  
  // Initialize comprehensive country mappings
  initializeCountryMappings() {
    // Map of all possible country identifiers to ISO codes
    this.countryMappings = new Map([
      // ISO codes (already correct)
      ['US', 'US'], ['USA', 'US'], ['UNITED STATES', 'US'], ['UNITED STATES OF AMERICA', 'US'], 
      ['AMERICA', 'US'], ['ESTADOS UNIDOS', 'US'], ['EEUU', 'US'], ['E.E.U.U.', 'US'],
      
      ['GB', 'GB'], ['UK', 'GB'], ['UNITED KINGDOM', 'GB'], ['GREAT BRITAIN', 'GB'], 
      ['ENGLAND', 'GB'], ['SCOTLAND', 'GB'], ['WALES', 'GB'], ['NORTHERN IRELAND', 'GB'],
      ['BRITAIN', 'GB'], ['REINO UNIDO', 'GB'],
      
      ['AU', 'AU'], ['AUSTRALIA', 'AU'], ['AUS', 'AU'], ['STRAYA', 'AU'], ['OZ', 'AU'],
      
      ['NZ', 'NZ'], ['NEW ZEALAND', 'NZ'], ['AOTEAROA', 'NZ'], ['NUEVA ZELANDA', 'NZ'],
      ['KIWI', 'NZ'], ['KIWILAND', 'NZ'],
      
      ['CA', 'CA'], ['CANADA', 'CA'], ['CAN', 'CA'], ['CANADÁ', 'CA'],
      
      ['IN', 'IN'], ['INDIA', 'IN'], ['IND', 'IN'], ['BHARAT', 'IN'], ['HINDUSTAN', 'IN'],
      ['भारत', 'IN'], ['BHARATH', 'IN'],
      
      ['PH', 'PH'], ['PHILIPPINES', 'PH'], ['PHIL', 'PH'], ['PILIPINAS', 'PH'], ['FILIPINAS', 'PH'],
      ['PINAS', 'PH'], ['RP', 'PH'],
      
      ['CN', 'CN'], ['CHINA', 'CN'], ['CHN', 'CN'], ['中国', 'CN'], ['ZHONGGUO', 'CN'],
      ['PEOPLE\'S REPUBLIC OF CHINA', 'CN'], ['PRC', 'CN'], ['MAINLAND CHINA', 'CN'],
      
      ['JP', 'JP'], ['JAPAN', 'JP'], ['JPN', 'JP'], ['日本', 'JP'], ['NIPPON', 'JP'], ['NIHON', 'JP'],
      
      ['KR', 'KR'], ['KOREA', 'KR'], ['SOUTH KOREA', 'KR'], ['REPUBLIC OF KOREA', 'KR'],
      ['한국', 'KR'], ['HANGUK', 'KR'], ['대한민국', 'KR'], ['DAEHAN MINGUK', 'KR'],
      
      ['DE', 'DE'], ['GERMANY', 'DE'], ['DEU', 'DE'], ['DEUTSCHLAND', 'DE'], ['ALEMANIA', 'DE'],
      ['ALLEMAGNE', 'DE'], ['BRD', 'DE'],
      
      ['FR', 'FR'], ['FRANCE', 'FR'], ['FRA', 'FR'], ['FRANCIA', 'FR'], ['FRANKREICH', 'FR'],
      
      ['ES', 'ES'], ['SPAIN', 'ES'], ['ESP', 'ES'], ['ESPAÑA', 'ES'], ['ESPANA', 'ES'], ['ESPAGNE', 'ES'],
      
      ['IT', 'IT'], ['ITALY', 'IT'], ['ITA', 'IT'], ['ITALIA', 'IT'], ['ITALIE', 'IT'],
      
      ['NL', 'NL'], ['NETHERLANDS', 'NL'], ['HOLLAND', 'NL'], ['THE NETHERLANDS', 'NL'],
      ['NEDERLAND', 'NL'], ['PAÍSES BAJOS', 'NL'], ['PAYS-BAS', 'NL'],
      
      ['BE', 'BE'], ['BELGIUM', 'BE'], ['BEL', 'BE'], ['BELGIQUE', 'BE'], ['BELGIË', 'BE'],
      ['BELGIEN', 'BE'], ['BÉLGICA', 'BE'],
      
      ['CH', 'CH'], ['SWITZERLAND', 'CH'], ['CHE', 'CH'], ['SWISS', 'CH'], ['SUISSE', 'CH'],
      ['SCHWEIZ', 'CH'], ['SVIZZERA', 'CH'], ['SUIZA', 'CH'],
      
      ['AT', 'AT'], ['AUSTRIA', 'AT'], ['AUT', 'AT'], ['ÖSTERREICH', 'AT'], ['OESTERREICH', 'AT'],
      ['AUTRICHE', 'AT'],
      
      ['PT', 'PT'], ['PORTUGAL', 'PT'], ['PRT', 'PT'], ['PORTUGALIA', 'PT'],
      
      ['BR', 'BR'], ['BRAZIL', 'BR'], ['BRA', 'BR'], ['BRASIL', 'BR'], ['BRÉSIL', 'BR'],
      
      ['MX', 'MX'], ['MEXICO', 'MX'], ['MEX', 'MX'], ['MÉXICO', 'MX'], ['MEJICO', 'MX'],
      ['MEXIQUE', 'MX'],
      
      ['AR', 'AR'], ['ARGENTINA', 'AR'], ['ARG', 'AR'], ['ARGENTINE', 'AR'],
      
      ['RU', 'RU'], ['RUSSIA', 'RU'], ['RUS', 'RU'], ['РОССИЙСКАЯ ФЕДЕРАЦИЯ', 'RU'],
      ['РОССИЯ', 'RU'], ['RUSSIAN FEDERATION', 'RU'], ['RUSSIE', 'RU'], ['RUSIA', 'RU'],
      
      ['ZA', 'ZA'], ['SOUTH AFRICA', 'ZA'], ['RSA', 'ZA'], ['SUID-AFRIKA', 'ZA'],
      ['SÜDAFRIKA', 'ZA'], ['AFRIQUE DU SUD', 'ZA'], ['SA', 'ZA'],
      
      ['EG', 'EG'], ['EGYPT', 'EG'], ['مصر', 'EG'], ['MISR', 'EG'], ['MASR', 'EG'],
      
      ['SA', 'SA'], ['SAUDI ARABIA', 'SA'], ['KSA', 'SA'], ['KINGDOM OF SAUDI ARABIA', 'SA'],
      ['السعودية', 'SA'], ['AL-SAUDIYYAH', 'SA'],
      
      ['AE', 'AE'], ['UAE', 'AE'], ['UNITED ARAB EMIRATES', 'AE'], ['EMIRATES', 'AE'],
      ['الإمارات', 'AE'], ['AL-IMARAT', 'AE'],
      
      ['IL', 'IL'], ['ISRAEL', 'IL'], ['ISR', 'IL'], ['ישראל', 'IL'], ['YISRAEL', 'IL'],
      
      ['TH', 'TH'], ['THAILAND', 'TH'], ['THA', 'TH'], ['ประเทศไทย', 'TH'], ['PRATHET THAI', 'TH'],
      ['SIAM', 'TH'],
      
      ['MY', 'MY'], ['MALAYSIA', 'MY'], ['MYS', 'MY'], ['MAS', 'MY'],
      
      ['SG', 'SG'], ['SINGAPORE', 'SG'], ['SGP', 'SG'], ['新加坡', 'SG'], ['SINGAPURA', 'SG'],
      
      ['ID', 'ID'], ['INDONESIA', 'ID'], ['IDN', 'ID'], ['INDO', 'ID'],
      
      ['VN', 'VN'], ['VIETNAM', 'VN'], ['VIET NAM', 'VN'], ['VNM', 'VN'], ['VIỆT NAM', 'VN'],
      
      ['HK', 'HK'], ['HONG KONG', 'HK'], ['HKG', 'HK'], ['香港', 'HK'], ['XIANGGANG', 'HK'],
      
      ['TW', 'TW'], ['TAIWAN', 'TW'], ['TWN', 'TW'], ['台湾', 'TW'], ['臺灣', 'TW'],
      ['REPUBLIC OF CHINA', 'TW'], ['ROC', 'TW'], ['FORMOSA', 'TW'],
      
      ['TR', 'TR'], ['TURKEY', 'TR'], ['TUR', 'TR'], ['TÜRKİYE', 'TR'], ['TURKIYE', 'TR'],
      ['TURQUIE', 'TR'], ['TURQUÍA', 'TR'],
      
      ['GR', 'GR'], ['GREECE', 'GR'], ['GRC', 'GR'], ['ΕΛΛΆΔΑ', 'GR'], ['ELLADA', 'GR'],
      ['HELLAS', 'GR'], ['GRÈCE', 'GR'], ['GRECIA', 'GR'],
      
      ['NO', 'NO'], ['NORWAY', 'NO'], ['NOR', 'NO'], ['NORGE', 'NO'], ['NOREG', 'NO'],
      ['NORVÈGE', 'NO'], ['NORUEGA', 'NO'],
      
      ['SE', 'SE'], ['SWEDEN', 'SE'], ['SWE', 'SE'], ['SVERIGE', 'SE'], ['SUÈDE', 'SE'],
      ['SUECIA', 'SE'],
      
      ['DK', 'DK'], ['DENMARK', 'DK'], ['DNK', 'DK'], ['DANMARK', 'DK'], ['DANEMARK', 'DK'],
      ['DINAMARCA', 'DK'],
      
      ['FI', 'FI'], ['FINLAND', 'FI'], ['FIN', 'FI'], ['SUOMI', 'FI'], ['FINLANDE', 'FI'],
      ['FINLANDIA', 'FI'],
      
      ['PL', 'PL'], ['POLAND', 'PL'], ['POL', 'PL'], ['POLSKA', 'PL'], ['POLOGNE', 'PL'],
      ['POLONIA', 'PL'],
      
      ['IE', 'IE'], ['IRELAND', 'IE'], ['IRL', 'IE'], ['ÉIRE', 'IE'], ['EIRE', 'IE'],
      ['REPUBLIC OF IRELAND', 'IE'], ['IRLANDE', 'IE'], ['IRLANDA', 'IE'],
      
      ['CZ', 'CZ'], ['CZECH REPUBLIC', 'CZ'], ['CZE', 'CZ'], ['CZECHIA', 'CZ'], 
      ['ČESKÁ REPUBLIKA', 'CZ'], ['CESKA REPUBLIKA', 'CZ'], ['TCHÉQUIE', 'CZ'],
      
      ['HU', 'HU'], ['HUNGARY', 'HU'], ['HUN', 'HU'], ['MAGYARORSZÁG', 'HU'], 
      ['MAGYARORSZAG', 'HU'], ['HONGRIE', 'HU'], ['HUNGRÍA', 'HU'],
      
      ['RO', 'RO'], ['ROMANIA', 'RO'], ['ROU', 'RO'], ['ROMÂNIA', 'RO'], ['ROUMANIE', 'RO'],
      ['RUMANIA', 'RO'], ['RUMANÍA', 'RO'],
      
      ['BG', 'BG'], ['BULGARIA', 'BG'], ['BGR', 'BG'], ['БЪЛГАРИЯ', 'BG'], ['BALGARIYA', 'BG'],
      ['BULGARIE', 'BG'],
      
      ['HR', 'HR'], ['CROATIA', 'HR'], ['HRV', 'HR'], ['HRVATSKA', 'HR'], ['CROATIE', 'HR'],
      ['CROACIA', 'HR'],
      
      ['SK', 'SK'], ['SLOVAKIA', 'SK'], ['SVK', 'SK'], ['SLOVENSKO', 'SK'], ['SLOVAQUIE', 'SK'],
      ['ESLOVAQUIA', 'SK'],
      
      ['SI', 'SI'], ['SLOVENIA', 'SI'], ['SVN', 'SI'], ['SLOVENIJA', 'SI'], ['SLOVÉNIE', 'SI'],
      ['ESLOVENIA', 'SI'],
      
      ['LT', 'LT'], ['LITHUANIA', 'LT'], ['LTU', 'LT'], ['LIETUVA', 'LT'], ['LITUANIE', 'LT'],
      ['LITUANIA', 'LT'],
      
      ['LV', 'LV'], ['LATVIA', 'LV'], ['LVA', 'LV'], ['LATVIJA', 'LV'], ['LETTONIE', 'LV'],
      ['LETONIA', 'LV'],
      
      ['EE', 'EE'], ['ESTONIA', 'EE'], ['EST', 'EE'], ['EESTI', 'EE'], ['ESTONIE', 'EE'],
      
      ['CL', 'CL'], ['CHILE', 'CL'], ['CHL', 'CL'], ['CHILI', 'CL'],
      
      ['CO', 'CO'], ['COLOMBIA', 'CO'], ['COL', 'CO'], ['COLOMBIE', 'CO'],
      
      ['PE', 'PE'], ['PERU', 'PE'], ['PER', 'PE'], ['PERÚ', 'PE'], ['PÉROU', 'PE'],
      
      ['VE', 'VE'], ['VENEZUELA', 'VE'], ['VEN', 'VE'], ['VÉNÉZUÉLA', 'VE'],
      
      ['EC', 'EC'], ['ECUADOR', 'EC'], ['ECU', 'EC'], ['ÉQUATEUR', 'EC'],
      
      ['UY', 'UY'], ['URUGUAY', 'UY'], ['URY', 'UY'],
      
      ['PY', 'PY'], ['PARAGUAY', 'PY'], ['PRY', 'PY'],
      
      ['BO', 'BO'], ['BOLIVIA', 'BO'], ['BOL', 'BO'], ['BOLIVIE', 'BO'],
      
      ['CR', 'CR'], ['COSTA RICA', 'CR'], ['CRI', 'CR'],
      
      ['PA', 'PA'], ['PANAMA', 'PA'], ['PAN', 'PA'], ['PANAMÁ', 'PA'],
      
      ['GT', 'GT'], ['GUATEMALA', 'GT'], ['GTM', 'GT'],
      
      ['HN', 'HN'], ['HONDURAS', 'HN'], ['HND', 'HN'],
      
      ['SV', 'SV'], ['EL SALVADOR', 'SV'], ['SLV', 'SV'], ['SALVADOR', 'SV'],
      
      ['NI', 'NI'], ['NICARAGUA', 'NI'], ['NIC', 'NI'],
      
      ['DO', 'DO'], ['DOMINICAN REPUBLIC', 'DO'], ['DOM', 'DO'], ['REPÚBLICA DOMINICANA', 'DO'],
      ['REPUBLICA DOMINICANA', 'DO'], ['DOMINICAN', 'DO'],
      
      ['CU', 'CU'], ['CUBA', 'CU'], ['CUB', 'CU'],
      
      ['JM', 'JM'], ['JAMAICA', 'JM'], ['JAM', 'JM'], ['JAMAÏQUE', 'JM'],
      
      ['HT', 'HT'], ['HAITI', 'HT'], ['HTI', 'HT'], ['HAÏTI', 'HT'], ['AYITI', 'HT'],
      
      ['PR', 'PR'], ['PUERTO RICO', 'PR'], ['PRI', 'PR'], ['PORTO RICO', 'PR'],
      
      ['MA', 'MA'], ['MOROCCO', 'MA'], ['MAR', 'MA'], ['MAROC', 'MA'], ['المغرب', 'MA'],
      ['AL-MAGHRIB', 'MA'], ['MARRUECOS', 'MA'],
      
      ['DZ', 'DZ'], ['ALGERIA', 'DZ'], ['DZA', 'DZ'], ['ALGÉRIE', 'DZ'], ['الجزائر', 'DZ'],
      ['AL-JAZAIR', 'DZ'], ['ARGELIA', 'DZ'],
      
      ['TN', 'TN'], ['TUNISIA', 'TN'], ['TUN', 'TN'], ['TUNISIE', 'TN'], ['تونس', 'TN'],
      ['TUNIS', 'TN'], ['TÚNEZ', 'TN'],
      
      ['LY', 'LY'], ['LIBYA', 'LY'], ['LBY', 'LY'], ['LIBYE', 'LY'], ['ليبيا', 'LY'],
      ['LIBIA', 'LY'],
      
      ['NG', 'NG'], ['NIGERIA', 'NG'], ['NGA', 'NG'], ['NIGÉRIA', 'NG'],
      
      ['KE', 'KE'], ['KENYA', 'KE'], ['KEN', 'KE'], ['KENIA', 'KE'],
      
      ['GH', 'GH'], ['GHANA', 'GH'], ['GHA', 'GH'],
      
      ['ET', 'ET'], ['ETHIOPIA', 'ET'], ['ETH', 'ET'], ['ÉTHIOPIE', 'ET'], ['ETIOPÍA', 'ET'],
      
      ['UG', 'UG'], ['UGANDA', 'UG'], ['UGA', 'UG'], ['OUGANDA', 'UG'],
      
      ['TZ', 'TZ'], ['TANZANIA', 'TZ'], ['TZA', 'TZ'], ['TANZANIE', 'TZ'],
      
      ['ZW', 'ZW'], ['ZIMBABWE', 'ZW'], ['ZWE', 'ZW'], ['ZIMBABUÉ', 'ZW'],
      
      ['ZM', 'ZM'], ['ZAMBIA', 'ZM'], ['ZMB', 'ZM'], ['ZAMBIE', 'ZM'],
      
      ['BW', 'BW'], ['BOTSWANA', 'BW'], ['BWA', 'BW'],
      
      ['MZ', 'MZ'], ['MOZAMBIQUE', 'MZ'], ['MOZ', 'MZ'],
      
      ['NA', 'NA'], ['NAMIBIA', 'NA'], ['NAM', 'NA'], ['NAMIBIE', 'NA'],
      
      ['AO', 'AO'], ['ANGOLA', 'AO'], ['AGO', 'AO'],
      
      ['SN', 'SN'], ['SENEGAL', 'SN'], ['SEN', 'SN'], ['SÉNÉGAL', 'SN'],
      
      ['CI', 'CI'], ['IVORY COAST', 'CI'], ['CIV', 'CI'], ['CÔTE D\'IVOIRE', 'CI'],
      ['COTE D\'IVOIRE', 'CI'], ['COSTA DE MARFIL', 'CI'],
      
      ['CM', 'CM'], ['CAMEROON', 'CM'], ['CMR', 'CM'], ['CAMEROUN', 'CM'], ['CAMERÚN', 'CM'],
      
      ['PK', 'PK'], ['PAKISTAN', 'PK'], ['PAK', 'PK'], ['پاکستان', 'PK'],
      
      ['BD', 'BD'], ['BANGLADESH', 'BD'], ['BGD', 'BD'], ['বাংলাদেশ', 'BD'],
      
      ['LK', 'LK'], ['SRI LANKA', 'LK'], ['LKA', 'LK'], ['ශ්‍රී ලංකාව', 'LK'], 
      ['இலங்கை', 'LK'], ['CEYLON', 'LK'],
      
      ['MM', 'MM'], ['MYANMAR', 'MM'], ['MMR', 'MM'], ['BURMA', 'MM'], ['BIRMANIE', 'MM'],
      ['BIRMANIA', 'MM'],
      
      ['KH', 'KH'], ['CAMBODIA', 'KH'], ['KHM', 'KH'], ['KAMPUCHEA', 'KH'], ['CAMBODGE', 'KH'],
      ['កម្ពុជា', 'KH'], ['CAMBOYA', 'KH'],
      
      ['LA', 'LA'], ['LAOS', 'LA'], ['LAO', 'LA'], ['ລາວ', 'LA'], ['LAO PDR', 'LA'],
      
      ['NP', 'NP'], ['NEPAL', 'NP'], ['NPL', 'NP'], ['नेपाल', 'NP'], ['NÉPAL', 'NP'],
      
      ['AF', 'AF'], ['AFGHANISTAN', 'AF'], ['AFG', 'AF'], ['افغانستان', 'AF'], ['AFGANISTÁN', 'AF'],
      
      ['IQ', 'IQ'], ['IRAQ', 'IQ'], ['IRQ', 'IQ'], ['العراق', 'IQ'], ['AL-IRAQ', 'IQ'], ['IRAK', 'IQ'],
      
      ['IR', 'IR'], ['IRAN', 'IR'], ['IRN', 'IR'], ['ایران', 'IR'], ['PERSIA', 'IR'],
      
      ['JO', 'JO'], ['JORDAN', 'JO'], ['JOR', 'JO'], ['الأردن', 'JO'], ['AL-URDUN', 'JO'],
      ['JORDANIE', 'JO'], ['JORDANIA', 'JO'],
      
      ['LB', 'LB'], ['LEBANON', 'LB'], ['LBN', 'LB'], ['لبنان', 'LB'], ['LUBNAN', 'LB'],
      ['LIBAN', 'LB'], ['LÍBANO', 'LB'],
      
      ['SY', 'SY'], ['SYRIA', 'SY'], ['SYR', 'SY'], ['سوريا', 'SY'], ['SURIYA', 'SY'],
      ['SYRIE', 'SY'], ['SIRIA', 'SY'],
      
      ['UA', 'UA'], ['UKRAINE', 'UA'], ['UKR', 'UA'], ['УКРАЇНА', 'UA'], ['UKRAYINA', 'UA'],
      ['UCRANIA', 'UA'],
      
      ['BY', 'BY'], ['BELARUS', 'BY'], ['BLR', 'BY'], ['БЕЛАРУСЬ', 'BY'], ['BYELORUSSIA', 'BY'],
      ['BIÉLORUSSIE', 'BY'], ['BIELORRUSIA', 'BY'],
      
      ['MD', 'MD'], ['MOLDOVA', 'MD'], ['MDA', 'MD'], ['MOLDAVIE', 'MD'], ['MOLDAVIA', 'MD'],
      
      ['GE', 'GE'], ['GEORGIA', 'GE'], ['GEO', 'GE'], ['საქართველო', 'GE'], ['SAKARTVELO', 'GE'],
      ['GÉORGIE', 'GE'],
      
      ['AM', 'AM'], ['ARMENIA', 'AM'], ['ARM', 'AM'], ['ՀԱՅԱՍՏԱՆ', 'AM'], ['HAYASTAN', 'AM'],
      ['ARMÉNIE', 'AM'],
      
      ['AZ', 'AZ'], ['AZERBAIJAN', 'AZ'], ['AZE', 'AZ'], ['AZƏRBAYCAN', 'AZ'], ['AZERBAÏDJAN', 'AZ'],
      ['AZERBAIYÁN', 'AZ'],
      
      ['KZ', 'KZ'], ['KAZAKHSTAN', 'KZ'], ['KAZ', 'KZ'], ['ҚАЗАҚСТАН', 'KZ'], ['QAZAQSTAN', 'KZ'],
      ['KAZAJSTÁN', 'KZ'],
      
      ['UZ', 'UZ'], ['UZBEKISTAN', 'UZ'], ['UZB', 'UZ'], ['OʻZBEKISTON', 'UZ'], ['OUZBÉKISTAN', 'UZ'],
      ['UZBEKISTÁN', 'UZ'],
      
      ['TM', 'TM'], ['TURKMENISTAN', 'TM'], ['TKM', 'TM'], ['TÜRKMENISTAN', 'TM'], 
      ['TURKMÉNISTAN', 'TM'], ['TURKMENISTÁN', 'TM'],
      
      ['KG', 'KG'], ['KYRGYZSTAN', 'KG'], ['KGZ', 'KG'], ['КЫРГЫЗСТАН', 'KG'], ['KIRGHIZISTAN', 'KG'],
      ['KIRGUISTÁN', 'KG'],
      
      ['TJ', 'TJ'], ['TAJIKISTAN', 'TJ'], ['TJK', 'TJ'], ['ТОҶИКИСТОН', 'TJ'], ['TADJIKISTAN', 'TJ'],
      ['TAYIKISTÁN', 'TJ'],
      
      ['MN', 'MN'], ['MONGOLIA', 'MN'], ['MNG', 'MN'], ['МОНГОЛ УЛС', 'MN'], ['MONGOLIE', 'MN'],
      
      // Special territories and regions
      ['MO', 'MO'], ['MACAU', 'MO'], ['MACAO', 'MO'], ['MAC', 'MO'], ['澳門', 'MO'],
      
      ['PS', 'PS'], ['PALESTINE', 'PS'], ['PSE', 'PS'], ['فلسطين', 'PS'], ['FILASTIN', 'PS'],
      
      ['VA', 'VA'], ['VATICAN', 'VA'], ['VAT', 'VA'], ['VATICAN CITY', 'VA'], ['HOLY SEE', 'VA'],
      
      // Caribbean islands
      ['BB', 'BB'], ['BARBADOS', 'BB'], ['BRB', 'BB'], ['BARBADE', 'BB'],
      
      ['BS', 'BS'], ['BAHAMAS', 'BS'], ['BHS', 'BS'], ['THE BAHAMAS', 'BS'],
      
      ['TT', 'TT'], ['TRINIDAD AND TOBAGO', 'TT'], ['TTO', 'TT'], ['TRINIDAD & TOBAGO', 'TT'],
      ['TRINIDAD', 'TT'], ['T&T', 'TT'],
      
      ['BM', 'BM'], ['BERMUDA', 'BM'], ['BMU', 'BM'], ['BERMUDES', 'BM'],
      
      ['KY', 'KY'], ['CAYMAN ISLANDS', 'KY'], ['CYM', 'KY'], ['CAYMAN', 'KY'],
      
      ['VG', 'VG'], ['BRITISH VIRGIN ISLANDS', 'VG'], ['VGB', 'VG'], ['BVI', 'VG'],
      
      ['TC', 'TC'], ['TURKS AND CAICOS', 'TC'], ['TCA', 'TC'], ['TURKS & CAICOS', 'TC'],
      
      // Pacific islands
      ['FJ', 'FJ'], ['FIJI', 'FJ'], ['FJI', 'FJ'], ['FIDJI', 'FJ'], ['FIYI', 'FJ'],
      
      ['PG', 'PG'], ['PAPUA NEW GUINEA', 'PG'], ['PNG', 'PG'], ['PAPUA', 'PG'],
      
      ['SB', 'SB'], ['SOLOMON ISLANDS', 'SB'], ['SLB', 'SB'], ['SOLOMONS', 'SB'],
      
      ['VU', 'VU'], ['VANUATU', 'VU'], ['VUT', 'VU'],
      
      ['NC', 'NC'], ['NEW CALEDONIA', 'NC'], ['NCL', 'NC'], ['NOUVELLE-CALÉDONIE', 'NC'],
      
      ['PF', 'PF'], ['FRENCH POLYNESIA', 'PF'], ['PYF', 'PF'], ['TAHITI', 'PF'],
      
      ['GU', 'GU'], ['GUAM', 'GU'], ['GUM', 'GU'],
      
      ['MP', 'MP'], ['NORTHERN MARIANA ISLANDS', 'MP'], ['MNP', 'MP'], ['NORTHERN MARIANAS', 'MP'],
      
      ['PW', 'PW'], ['PALAU', 'PW'], ['PLW', 'PW'], ['BELAU', 'PW'],
      
      ['MH', 'MH'], ['MARSHALL ISLANDS', 'MH'], ['MHL', 'MH'], ['MARSHALLS', 'MH'],
      
      ['AS', 'AS'], ['AMERICAN SAMOA', 'AS'], ['ASM', 'AS'], ['AMERIKA SAMOA', 'AS'],
      
      ['WS', 'WS'], ['SAMOA', 'WS'], ['WSM', 'WS'], ['WESTERN SAMOA', 'WS'],
      
      ['TO', 'TO'], ['TONGA', 'TO'], ['TON', 'TO'],
      
      ['TV', 'TV'], ['TUVALU', 'TV'], ['TUV', 'TV'],
      
      ['NR', 'NR'], ['NAURU', 'NR'], ['NRU', 'NR'],
      
      ['KI', 'KI'], ['KIRIBATI', 'KI'], ['KIR', 'KI'],
      
      // Indian Ocean islands
      ['MV', 'MV'], ['MALDIVES', 'MV'], ['MDV', 'MV'], ['MALDIVAS', 'MV'],
      
      ['MU', 'MU'], ['MAURITIUS', 'MU'], ['MUS', 'MU'], ['MAURICE', 'MU'], ['MAURICIO', 'MU'],
      
      ['SC', 'SC'], ['SEYCHELLES', 'SC'], ['SYC', 'SC'],
      
      ['RE', 'RE'], ['REUNION', 'RE'], ['REU', 'RE'], ['RÉUNION', 'RE'], ['LA RÉUNION', 'RE'],
      
      // European micro-states
      ['AD', 'AD'], ['ANDORRA', 'AD'], ['AND', 'AD'], ['ANDORRE', 'AD'],
      
      ['MC', 'MC'], ['MONACO', 'MC'], ['MCO', 'MC'], ['MÓNACO', 'MC'],
      
      ['SM', 'SM'], ['SAN MARINO', 'SM'], ['SMR', 'SM'], ['SAINT-MARIN', 'SM'],
      
      ['LI', 'LI'], ['LIECHTENSTEIN', 'LI'], ['LIE', 'LI'],
      
      ['MT', 'MT'], ['MALTA', 'MT'], ['MLT', 'MT'], ['MALTE', 'MT'],
      
      ['CY', 'CY'], ['CYPRUS', 'CY'], ['CYP', 'CY'], ['ΚΎΠΡΟΣ', 'CY'], ['KYPROS', 'CY'],
      ['CHYPRE', 'CY'], ['CHIPRE', 'CY'],
      
      // Special cases
      ['EU', 'EU'], ['EUROPEAN UNION', 'EU'], ['EUROPE', 'EU'],
      
      // Common misspellings and variations
      ['ENGLAND', 'GB'], ['BRITAIN', 'GB'], ['GREAT BRITIAN', 'GB'], ['UNITED KINDOM', 'GB'],
      ['UNTIED KINGDOM', 'GB'], ['UNITED KINGDON', 'GB'], ['UNITED KIGDOM', 'GB'],
      
      ['UNITED STATE', 'US'], ['UNITED STATS', 'US'], ['UNITED SATES', 'US'], 
      ['UNITED STAES', 'US'], ['UNITES STATES', 'US'], ['U.S.A', 'US'], ['U.S.A.', 'US'],
      ['U.S', 'US'], ['U.S.', 'US'], ['THE US', 'US'], ['THE USA', 'US'],
      
      ['AUSTRAILIA', 'AU'], ['AUSTRAILA', 'AU'], ['AUSTRALA', 'AU'], ['AUSTRIALIA', 'AU'],
      
      ['NEW ZELAND', 'NZ'], ['NEW ZEELAND', 'NZ'], ['NEW ZEALND', 'NZ'], ['NEWZEALAND', 'NZ'],
      
      ['CANDADA', 'CA'], ['CANANDA', 'CA'], ['CANAD', 'CA'],
      
      ['PHILLIPINES', 'PH'], ['PHILIPINES', 'PH'], ['PHILLIPPINES', 'PH'], ['PHILIPPINS', 'PH'],
      ['PHILIPINS', 'PH'], ['FILLIPINES', 'PH'],
      
      ['INDONISIA', 'ID'], ['INDONSIA', 'ID'], ['INDONEISA', 'ID'], ['INDONESA', 'ID'],
      
      ['MALAYSAI', 'MY'], ['MALAYSA', 'MY'], ['MALASIA', 'MY'], ['MALAYISA', 'MY'],
      
      ['SINGAPOR', 'SG'], ['SINGAPOUR', 'SG'], ['SINGAPUR', 'SG'], ['SIGNAPORE', 'SG'],
      
      ['NETHERLAND', 'NL'], ['HOLAND', 'NL'], ['HOLLOND', 'NL'], ['THE NETHERLAND', 'NL']
    ]);
    
    // Create reverse mapping for country names to codes
    this.countryNameToCode = new Map();
    for (const [key, value] of this.countryMappings) {
      const normalizedKey = this.normalizeCountryInput(key);
      this.countryNameToCode.set(normalizedKey, value);
    }
  }
  
  // Normalize country input for matching
  normalizeCountryInput(input) {
    if (!input) return '';
    
    return input
      .toString()
      .toUpperCase()
      .trim()
      .replace(/[^A-Z0-9\u0080-\uFFFF\s]/g, '') // Keep unicode chars for non-Latin scripts
      .replace(/\s+/g, ' ')
      .trim();
  }
  
  // Resolve country input to ISO code
  resolveCountryCode(countryInput) {
    if (!countryInput) return null;
    
    const normalized = this.normalizeCountryInput(countryInput);
    
    // First, check if it's already a valid ISO code
    const upperInput = countryInput.toUpperCase().trim();
    if (upperInput.length === 2 && getCountries().includes(upperInput)) {
      return upperInput;
    }
    
    // Check our mappings
    const mappedCode = this.countryNameToCode.get(normalized);
    if (mappedCode) {
      return mappedCode;
    }
    
    // Try fuzzy matching for common variations
    for (const [key, value] of this.countryNameToCode) {
      if (key.includes(normalized) || normalized.includes(key)) {
        if (normalized.length > 3 && key.length > 3) { // Avoid false matches on short strings
          return value;
        }
      }
    }
    
    // Check if it's a phone prefix (like +1, +44, etc.)
    if (countryInput.startsWith('+') || /^\d+$/.test(countryInput)) {
      const prefix = countryInput.replace('+', '');
      // Map common prefixes to countries
      const prefixMap = {
        '1': 'US', '44': 'GB', '61': 'AU', '64': 'NZ', '91': 'IN',
        '86': 'CN', '81': 'JP', '82': 'KR', '49': 'DE', '33': 'FR',
        '39': 'IT', '34': 'ES', '31': 'NL', '32': 'BE', '41': 'CH',
        '43': 'AT', '45': 'DK', '46': 'SE', '47': 'NO', '358': 'FI',
        '48': 'PL', '420': 'CZ', '421': 'SK', '36': 'HU', '40': 'RO',
        '359': 'BG', '385': 'HR', '386': 'SI', '30': 'GR', '90': 'TR',
        '7': 'RU', '380': 'UA', '375': 'BY', '370': 'LT', '371': 'LV',
        '372': 'EE', '995': 'GE', '374': 'AM', '994': 'AZ', '7': 'KZ',
        '998': 'UZ', '993': 'TM', '996': 'KG', '992': 'TJ', '976': 'MN',
        '84': 'VN', '66': 'TH', '60': 'MY', '65': 'SG', '62': 'ID',
        '63': 'PH', '852': 'HK', '853': 'MO', '886': 'TW', '92': 'PK',
        '94': 'LK', '880': 'BD', '95': 'MM', '977': 'NP', '93': 'AF',
        '98': 'IR', '964': 'IQ', '962': 'JO', '961': 'LB', '963': 'SY',
        '966': 'SA', '971': 'AE', '968': 'OM', '967': 'YE', '965': 'KW',
        '973': 'BH', '974': 'QA', '972': 'IL', '970': 'PS', '20': 'EG',
        '212': 'MA', '213': 'DZ', '216': 'TN', '218': 'LY', '249': 'SD',
        '234': 'NG', '254': 'KE', '255': 'TZ', '256': 'UG', '251': 'ET',
        '233': 'GH', '237': 'CM', '225': 'CI', '221': 'SN', '27': 'ZA',
        '263': 'ZW', '260': 'ZM', '267': 'BW', '258': 'MZ', '264': 'NA',
        '244': 'AO', '55': 'BR', '54': 'AR', '56': 'CL', '57': 'CO',
        '58': 'VE', '593': 'EC', '595': 'PY', '598': 'UY', '591': 'BO',
        '51': 'PE', '52': 'MX', '53': 'CU', '504': 'HN', '503': 'SV',
        '502': 'GT', '507': 'PA', '506': 'CR', '505': 'NI', '509': 'HT',
        '1809': 'DO', '1876': 'JM', '1868': 'TT', '1246': 'BB', '1242': 'BS',
        '1441': 'BM', '1345': 'KY', '1284': 'VG', '1649': 'TC', '1787': 'PR',
        '679': 'FJ', '675': 'PG', '677': 'SB', '678': 'VU', '687': 'NC',
        '689': 'PF', '1671': 'GU', '1670': 'MP', '680': 'PW', '692': 'MH',
        '1684': 'AS', '685': 'WS', '676': 'TO', '688': 'TV', '674': 'NR',
        '686': 'KI', '960': 'MV', '230': 'MU', '248': 'SC', '262': 'RE',
        '376': 'AD', '377': 'MC', '378': 'SM', '423': 'LI', '356': 'MT',
        '357': 'CY'
      };
      
      if (prefixMap[prefix]) {
        return prefixMap[prefix];
      }
    }
    
    // Log unmatched country for debugging
    this.logger.debug('Could not resolve country code', { 
      input: countryInput, 
      normalized 
    });
    
    return null;
  }
  
  // External API placeholder - Numverify
  async validateWithNumverify(phone, country = null) {
    // TODO: Implement Numverify API call
    // This is a placeholder for the external validation service
    this.logger.info('Numverify validation placeholder called', { phone, country });
    
    // Placeholder response structure
    return {
      valid: false,
      number: phone,
      local_format: '',
      international_format: '',
      country_prefix: '',
      country_code: '',
      country_name: '',
      location: '',
      carrier: '',
      line_type: 'unknown',
      error: 'Numverify integration not implemented'
    };
  }
  
  // Predict possible countries for a phone number
  predictCountries(cleanedPhone) {
    const predictions = [];
    const allCountries = getCountries();
    
    // Try parsing with each country and collect valid matches
    for (const country of allCountries) {
      try {
        const phoneNumber = parsePhoneNumber(cleanedPhone, country);
        if (phoneNumber && phoneNumber.isPossible()) {
          const isValid = phoneNumber.isValid();
          const confidence = this.calculateCountryConfidence(phoneNumber, cleanedPhone, country);
          
          predictions.push({
            country,
            countryName: this.getCountryName(country),
            valid: isValid,
            possible: true,
            phoneType: phoneNumber.getType(),
            confidence: confidence.score,
            confidenceLevel: confidence.level,
            format: {
              e164: phoneNumber.format('E.164'),
              international: phoneNumber.format('INTERNATIONAL'),
              national: phoneNumber.format('NATIONAL')
            }
          });
        }
      } catch (e) {
        // Country doesn't match this number format
      }
    }
    
    // Sort by confidence score (highest first)
    predictions.sort((a, b) => b.confidence - a.confidence);
    
    return predictions;
  }
  
  // Calculate confidence for a specific country match
  calculateCountryConfidence(phoneNumber, originalPhone, country) {
    let score = 0;
    const factors = [];
    
    // Base score for valid numbers - the library already checked patterns!
    if (phoneNumber.isValid()) {
      score += 50;  // High score because libphonenumber-js validated it
      factors.push('valid_format');
    } else if (phoneNumber.isPossible()) {
      score += 25;
      factors.push('possible_format');
    }
    
    // Phone type clarity
    const phoneType = phoneNumber.getType();
    if (phoneType === 'MOBILE' || phoneType === 'FIXED_LINE') {
      score += 30;
      factors.push('definite_type');
    } else if (phoneType === 'FIXED_LINE_OR_MOBILE') {
      score += 15;
      factors.push('ambiguous_type');
    } else {
      score += 5;
      factors.push('unknown_type');
    }
    
    // National number completeness
    const nationalNumber = phoneNumber.nationalNumber;
    if (nationalNumber && nationalNumber.length >= 6) {
      score += 10;
      factors.push('complete_number');
    }
    
    // Convert to level
    let level;
    if (score >= 80) level = 'very_high';
    else if (score >= 60) level = 'high';
    else if (score >= 40) level = 'medium';
    else if (score >= 20) level = 'low';
    else level = 'very_low';
    
    return { score, level, factors };
  }
  
  // Find best matching country from predictions
  findBestCountryMatch(predictions, hintCountry = null) {
    if (!predictions || predictions.length === 0) {
      return null;
    }
    
    // If hint country provided, check if it's in predictions
    if (hintCountry) {
      const hintMatch = predictions.find(p => p.country === hintCountry);
      if (hintMatch && hintMatch.confidence >= 40) {
        // Use hint if confidence is reasonable
        return hintMatch;
      }
    }
    
    // Otherwise, return highest confidence match
    return predictions[0];
  }
  
  // Main validation method
  async validatePhoneNumber(phone, options = {}) {
    const {
      country = null,
      countryHint = null,
      useExternalApi = true,
      confidenceThreshold = 60, // Below this, use external API
      clientId = null,
      useCache = true
    } = options;
    
    // Resolve country input to ISO code
    const providedCountryRaw = country || countryHint;
    const providedCountry = this.resolveCountryCode(providedCountryRaw);
    
    if (providedCountryRaw && !providedCountry) {
      this.logger.warn('Could not resolve country input', { 
        input: providedCountryRaw 
      });
    }
    
    this.logger.debug('Starting phone validation', {
      phone,
      providedCountryRaw,
      providedCountry,
      clientId
    });
    
    // Handle null/empty
    if (!phone || phone === '') {
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Phone number is required',
        formatValid: false,
        confidence: { score: 0, level: 'none', factors: ['no_input'] }
      }, clientId);
    }
    
    // Clean phone number
    const cleanedPhone = this.cleanPhoneNumber(phone);
    const originalHasPlus = cleanedPhone.startsWith('+');
    
    // Check cache first if enabled
    if (useCache && cleanedPhone.startsWith('+')) {
      const cached = await this.checkPhoneCache(cleanedPhone);
      if (cached) {
        this.logger.debug('Phone found in cache', { phone: cleanedPhone });
        return cached;
      }
    }
    
    let phoneNumber = null;
    let successfulCountry = null;
    let validationMethod = null;
    let confidence = null;
    let predictions = [];
    
    try {
      // Step 1: If number has international format, try auto-detection first
      if (originalHasPlus) {
        try {
          phoneNumber = parsePhoneNumberFromString(cleanedPhone);
          if (phoneNumber && phoneNumber.isValid()) {
            successfulCountry = phoneNumber.country;
            validationMethod = 'international_format';
            
            // Calculate confidence
            confidence = this.calculateValidationConfidence(phoneNumber, {
              method: validationMethod,
              originalHasPlus: true,
              providedCountry,
              matchedCountry: successfulCountry
            });
            
            this.logger.debug('Validated with international format', {
              country: successfulCountry,
              confidence: confidence.score
            });
          }
        } catch (e) {
          this.logger.debug('International format parsing failed', { error: e.message });
        }
      }
      
      // Step 2: If not validated yet, predict possible countries
      if (!phoneNumber || !phoneNumber.isValid()) {
        predictions = this.predictCountries(cleanedPhone);
        
        this.logger.debug('Country predictions', {
          totalPredictions: predictions.length,
          topPredictions: predictions.slice(0, 5).map(p => ({
            country: p.country,
            confidence: p.confidence,
            valid: p.valid
          }))
        });
        
        // Find best match considering the hint
        const bestMatch = this.findBestCountryMatch(predictions, providedCountry);
        
        if (bestMatch) {
          try {
            phoneNumber = parsePhoneNumber(cleanedPhone, bestMatch.country);
            if (phoneNumber && phoneNumber.isValid()) {
              successfulCountry = bestMatch.country;
              validationMethod = providedCountry === bestMatch.country ? 'hint_match' : 'predicted';
              
              // Use the confidence from prediction
              confidence = {
                score: bestMatch.confidence,
                level: bestMatch.confidenceLevel,
                factors: ['predicted_country', ...(bestMatch.factors || [])]
              };
              
              this.logger.debug('Validated with predicted country', {
                country: successfulCountry,
                confidence: bestMatch.confidence,
                wasHintUsed: providedCountry === bestMatch.country
              });
            }
          } catch (e) {
            this.logger.debug('Validation with best match failed', { 
              country: bestMatch.country,
              error: e.message 
            });
          }
        }
      }
      
      // Step 3: Check if we need external validation
      const needsExternalValidation = useExternalApi && (
        !phoneNumber || 
        !phoneNumber.isValid() || 
        (confidence && confidence.score < confidenceThreshold) ||
        phoneNumber.getType() === 'UNKNOWN'
      );
      
      if (needsExternalValidation) {
        this.logger.info('Using external API due to low confidence or validation failure', {
          hasPhoneNumber: !!phoneNumber,
          isValid: phoneNumber?.isValid(),
          confidenceScore: confidence?.score,
          phoneType: phoneNumber?.getType()
        });
        
        // Call external API (Numverify)
        const externalResult = await this.validateWithNumverify(
          cleanedPhone,
          successfulCountry || providedCountry
        );
        
        // If external API provides better results, use them
        if (externalResult.valid) {
          // Build result from external API data
          return this.buildValidationResult(phone, {
            valid: true,
            formatValid: true,
            e164: externalResult.international_format,
            international: externalResult.international_format,
            national: externalResult.local_format,
            countryCode: externalResult.country_prefix,
            country: externalResult.country_code,
            type: externalResult.line_type?.toUpperCase() || 'UNKNOWN',
            isMobile: externalResult.line_type === 'mobile',
            isFixedLine: externalResult.line_type === 'fixed_line',
            carrier: externalResult.carrier,
            location: externalResult.location,
            confidence: {
              score: 95,
              level: 'very_high',
              factors: ['external_api_verified', 'numverify']
            },
            validationMethod: 'external_api',
            externalApiUsed: true
          }, clientId);
        }
      }
      
      // Step 4: Return validation result
      if (!phoneNumber || !phoneNumber.isValid()) {
        // Failed validation
        return this.buildValidationResult(phone, {
          valid: false,
          error: 'Invalid phone number format',
          formatValid: false,
          attemptedCountry: providedCountry,
          attemptedCountryInput: providedCountryRaw,
          predictions: predictions.slice(0, 3), // Top 3 predictions
          confidence: {
            score: 0,
            level: 'none',
            factors: ['validation_failed']
          }
        }, clientId);
      }
      
      // Successful validation
      const phoneDetails = {
        valid: true,
        formatValid: true,
        e164: phoneNumber.format('E.164'),
        international: phoneNumber.format('INTERNATIONAL'),
        national: phoneNumber.format('NATIONAL'),
        countryCode: phoneNumber.countryCallingCode,
        country: phoneNumber.country || successfulCountry,
        type: phoneNumber.getType() || 'UNKNOWN',
        isMobile: phoneNumber.getType() === 'MOBILE',
        isFixedLine: phoneNumber.getType() === 'FIXED_LINE',
        isFixedLineOrMobile: phoneNumber.getType() === 'FIXED_LINE_OR_MOBILE',
        isPossible: phoneNumber.isPossible(),
        uri: phoneNumber.getURI(),
        confidence,
        validationMethod,
        hintCountryUsed: providedCountry === successfulCountry,
        externalApiUsed: false
      };
      
      // For FIXED_LINE_OR_MOBILE, default to mobile for common mobile countries
      if (phoneDetails.isFixedLineOrMobile) {
        const mobileFirstCountries = ['US', 'CA', 'PH', 'IN', 'BR', 'MX', 'AU'];
        if (mobileFirstCountries.includes(phoneDetails.country)) {
          phoneDetails.isMobile = true;
        }
      }
      
      // Get country name
      phoneDetails.countryName = this.getCountryName(phoneDetails.country);
      
      const result = this.buildValidationResult(phone, phoneDetails, clientId);
      
      // Save to cache if valid
      if (useCache && result.valid) {
        await this.savePhoneCache(phone, result, clientId);
      }
      
      return result;
      
    } catch (error) {
      this.logger.error('Phone parsing failed', { phone, error: error.message });
      
      return this.buildValidationResult(phone, {
        valid: false,
        error: 'Failed to validate phone number',
        formatValid: false,
        confidence: {
          score: 0,
          level: 'none',
          factors: ['system_error']
        }
      }, clientId);
    }
  }
  
  // Calculate overall validation confidence
  calculateValidationConfidence(phoneNumber, context = {}) {
    const {
      method,
      originalHasPlus,
      providedCountry,
      matchedCountry
    } = context;
    
    let score = 0;
    const factors = [];
    
    // Method scoring
    if (method === 'international_format' && originalHasPlus) {
      score += 40;
      factors.push('international_format');
    } else if (method === 'hint_match' && providedCountry === matchedCountry) {
      score += 35;
      factors.push('country_hint_matched');
    } else if (method === 'predicted') {
      score += 25;
      factors.push('country_predicted');
    }
    
    // Validity scoring
    if (phoneNumber.isValid() && phoneNumber.isPossible()) {
      score += 30;
      factors.push('valid_and_possible');
    } else if (phoneNumber.isValid()) {
      score += 20;
      factors.push('valid_only');
    }
    
    // Type scoring
    const phoneType = phoneNumber.getType();
    if (phoneType === 'MOBILE' || phoneType === 'FIXED_LINE') {
      score += 20;
      factors.push('definite_line_type');
    } else if (phoneType === 'FIXED_LINE_OR_MOBILE') {
      score += 10;
      factors.push('ambiguous_line_type');
    } else {
      score += 0;
      factors.push('unknown_line_type');
    }
    
    // Additional factors
    if (providedCountry && providedCountry !== matchedCountry) {
      score -= 10;
      factors.push('country_hint_mismatch');
    }
    
    // Ensure score is between 0 and 100
    score = Math.max(0, Math.min(100, score));
    
    // Convert to level
    let level;
    if (score >= 85) level = 'very_high';
    else if (score >= 70) level = 'high';
    else if (score >= 50) level = 'medium';
    else if (score >= 30) level = 'low';
    else level = 'very_low';
    
    return { score, level, factors };
  }
  
  // Clean phone number
  cleanPhoneNumber(phone) {
    // Convert to string and trim
    let cleaned = String(phone).trim();
    
    // Remove common formatting characters but keep + for international
    cleaned = cleaned.replace(/[\s\-\(\)\.]/g, '');
    
    // Remove extension markers and everything after
    cleaned = cleaned.replace(/(?:ext|extension|x|ext\.|extn|extn\.|#)[\s\.\-:#]?[\d]+$/i, '');
    
    // Handle various international prefixes by converting to +
    if (cleaned.startsWith('00')) {
      cleaned = '+' + cleaned.substring(2);
    } else if (cleaned.startsWith('011')) {
      cleaned = '+' + cleaned.substring(3);
    } else if (cleaned.startsWith('0011')) {
      cleaned = '+' + cleaned.substring(4);
    }
    
    // Handle letters in phone numbers (like 1-800-FLOWERS)
    cleaned = cleaned.replace(/[A-Za-z]/g, (match) => {
      const letterMap = {
        'A': '2', 'B': '2', 'C': '2',
        'D': '3', 'E': '3', 'F': '3',
        'G': '4', 'H': '4', 'I': '4',
        'J': '5', 'K': '5', 'L': '5',
        'M': '6', 'N': '6', 'O': '6',
        'P': '7', 'Q': '7', 'R': '7', 'S': '7',
        'T': '8', 'U': '8', 'V': '8',
        'W': '9', 'X': '9', 'Y': '9', 'Z': '9'
      };
      return letterMap[match.toUpperCase()] || match;
    });
    
    // Remove any remaining non-digit characters except +
    cleaned = cleaned.replace(/[^\d+]/g, '');
    
    return cleaned;
  }
  
  // Get country name from code
  getCountryName(countryCode) {
    const countryNames = {
      'US': 'United States',
      'CA': 'Canada',
      'GB': 'United Kingdom',
      'AU': 'Australia',
      'DE': 'Germany',
      'FR': 'France',
      'PH': 'Philippines',
      'PG': 'Papua New Guinea',
      'IN': 'India',
      'JP': 'Japan',
      'CN': 'China',
      'BR': 'Brazil',
      'MX': 'Mexico',
      'ES': 'Spain',
      'IT': 'Italy',
      'NL': 'Netherlands',
      'SE': 'Sweden',
      'NO': 'Norway',
      'DK': 'Denmark',
      'FI': 'Finland',
      'PL': 'Poland',
      'RU': 'Russia',
      'ZA': 'South Africa',
      'SG': 'Singapore',
      'MY': 'Malaysia',
      'TH': 'Thailand',
      'VN': 'Vietnam',
      'ID': 'Indonesia',
      'KR': 'South Korea',
      'TW': 'Taiwan',
      'HK': 'Hong Kong',
      'NZ': 'New Zealand',
      'AR': 'Argentina',
      'CL': 'Chile',
      'CO': 'Colombia',
      'PE': 'Peru',
      'VE': 'Venezuela',
      'EC': 'Ecuador',
      'BO': 'Bolivia',
      'PY': 'Paraguay',
      'UY': 'Uruguay',
      'CR': 'Costa Rica',
      'PA': 'Panama',
      'DO': 'Dominican Republic',
      'GT': 'Guatemala',
      'HN': 'Honduras',
      'SV': 'El Salvador',
      'NI': 'Nicaragua',
      'PR': 'Puerto Rico',
      'JM': 'Jamaica',
      'TT': 'Trinidad and Tobago',
      'BB': 'Barbados',
      'BS': 'Bahamas',
      'BM': 'Bermuda',
      'KY': 'Cayman Islands',
      'VG': 'British Virgin Islands',
      'AG': 'Antigua and Barbuda',
      'DM': 'Dominica',
      'GD': 'Grenada',
      'KN': 'Saint Kitts and Nevis',
      'LC': 'Saint Lucia',
      'VC': 'Saint Vincent and the Grenadines',
      'MQ': 'Martinique',
      'GP': 'Guadeloupe',
      'AW': 'Aruba',
      'CW': 'Curaçao',
      'SX': 'Sint Maarten',
      'BQ': 'Caribbean Netherlands',
      'TC': 'Turks and Caicos Islands',
      'VI': 'U.S. Virgin Islands',
      'AI': 'Anguilla',
      'MS': 'Montserrat',
      'GU': 'Guam',
      'AS': 'American Samoa',
      'MP': 'Northern Mariana Islands',
      'PW': 'Palau',
      'MH': 'Marshall Islands',
      'BE': 'Belgium',
      'CH': 'Switzerland',
      'AT': 'Austria',
      'CZ': 'Czech Republic',
      'SK': 'Slovakia',
      'HU': 'Hungary',
      'RO': 'Romania',
      'BG': 'Bulgaria',
      'HR': 'Croatia',
     'SI': 'Slovenia',
     'LT': 'Lithuania',
     'LV': 'Latvia',
     'EE': 'Estonia',
     'MT': 'Malta',
     'CY': 'Cyprus',
     'LU': 'Luxembourg',
     'IS': 'Iceland',
     'AD': 'Andorra',
     'MC': 'Monaco',
     'LI': 'Liechtenstein',
     'SM': 'San Marino',
     'VA': 'Vatican City',
     'UA': 'Ukraine',
     'BY': 'Belarus',
     'MD': 'Moldova',
     'GE': 'Georgia',
     'AM': 'Armenia',
     'AZ': 'Azerbaijan',
     'KZ': 'Kazakhstan',
     'UZ': 'Uzbekistan',
     'TM': 'Turkmenistan',
     'KG': 'Kyrgyzstan',
     'TJ': 'Tajikistan',
     'MN': 'Mongolia',
     'AF': 'Afghanistan',
     'PK': 'Pakistan',
     'BD': 'Bangladesh',
     'LK': 'Sri Lanka',
     'MM': 'Myanmar',
     'NP': 'Nepal',
     'BT': 'Bhutan',
     'MV': 'Maldives',
     'KH': 'Cambodia',
     'LA': 'Laos',
     'BN': 'Brunei',
     'TL': 'Timor-Leste',
     'MO': 'Macau',
     'KP': 'North Korea',
     'IR': 'Iran',
     'IQ': 'Iraq',
     'SY': 'Syria',
     'LB': 'Lebanon',
     'JO': 'Jordan',
     'IL': 'Israel',
     'PS': 'Palestine',
     'SA': 'Saudi Arabia',
     'YE': 'Yemen',
     'OM': 'Oman',
     'AE': 'United Arab Emirates',
     'QA': 'Qatar',
     'BH': 'Bahrain',
     'KW': 'Kuwait',
     'EG': 'Egypt',
     'LY': 'Libya',
     'TN': 'Tunisia',
     'DZ': 'Algeria',
     'MA': 'Morocco',
     'EH': 'Western Sahara',
     'MR': 'Mauritania',
     'ML': 'Mali',
     'NE': 'Niger',
     'TD': 'Chad',
     'SD': 'Sudan',
     'SS': 'South Sudan',
     'ER': 'Eritrea',
     'DJ': 'Djibouti',
     'SO': 'Somalia',
     'ET': 'Ethiopia',
     'KE': 'Kenya',
     'UG': 'Uganda',
     'RW': 'Rwanda',
     'BI': 'Burundi',
     'TZ': 'Tanzania',
     'MW': 'Malawi',
     'ZM': 'Zambia',
     'ZW': 'Zimbabwe',
     'BW': 'Botswana',
     'NA': 'Namibia',
     'SZ': 'Eswatini',
     'LS': 'Lesotho',
     'MZ': 'Mozambique',
     'AO': 'Angola',
     'CD': 'Democratic Republic of Congo',
     'CG': 'Republic of Congo',
     'GA': 'Gabon',
     'GQ': 'Equatorial Guinea',
     'ST': 'São Tomé and Príncipe',
     'CM': 'Cameroon',
     'CF': 'Central African Republic',
     'NG': 'Nigeria',
     'BJ': 'Benin',
     'TG': 'Togo',
     'GH': 'Ghana',
     'CI': 'Ivory Coast',
     'BF': 'Burkina Faso',
     'LR': 'Liberia',
     'SL': 'Sierra Leone',
     'GN': 'Guinea',
     'GW': 'Guinea-Bissau',
     'SN': 'Senegal',
     'GM': 'Gambia',
     'CV': 'Cape Verde',
     'MU': 'Mauritius',
     'SC': 'Seychelles',
     'KM': 'Comoros',
     'RE': 'Réunion',
     'YT': 'Mayotte',
     'FJ': 'Fiji',
     'SB': 'Solomon Islands',
     'VU': 'Vanuatu',
     'NC': 'New Caledonia',
     'PF': 'French Polynesia',
     'WS': 'Samoa',
     'TO': 'Tonga',
     'TV': 'Tuvalu',
     'NR': 'Nauru',
     'KI': 'Kiribati',
     'FM': 'Micronesia',
     'PW': 'Palau',
     'CK': 'Cook Islands',
     'NU': 'Niue',
     'TK': 'Tokelau',
     'WF': 'Wallis and Futuna',
     'PM': 'Saint Pierre and Miquelon',
     'GL': 'Greenland',
     'FO': 'Faroe Islands',
     'GI': 'Gibraltar',
     'JE': 'Jersey',
     'GG': 'Guernsey',
     'IM': 'Isle of Man',
     'AX': 'Åland Islands',
     'SJ': 'Svalbard and Jan Mayen',
     'BV': 'Bouvet Island',
     'IO': 'British Indian Ocean Territory',
     'CX': 'Christmas Island',
     'CC': 'Cocos Islands',
     'HM': 'Heard Island and McDonald Islands',
     'NF': 'Norfolk Island',
     'PN': 'Pitcairn Islands',
     'GS': 'South Georgia and South Sandwich Islands',
     'UM': 'United States Minor Outlying Islands',
     'AQ': 'Antarctica',
     'HT': 'Haiti',
     'CU': 'Cuba',
     'FK': 'Falkland Islands',
     'GF': 'French Guiana',
     'SR': 'Suriname',
     'GY': 'Guyana'
   };
   
   return countryNames[countryCode] || countryCode;
 }
 
 // Build validation result
 buildValidationResult(originalPhone, validationData, clientId) {
   const isValid = validationData.valid === true;
   const formatValid = validationData.formatValid !== false;
   
   // Determine if phone was changed (formatted differently)
   const formattedPhone = validationData.international || validationData.e164 || this.cleanPhoneNumber(originalPhone);
   const wasChanged = originalPhone !== formattedPhone;
   
   // Get the country name properly
   const countryCode = validationData.country || null;
   const countryName = countryCode ? this.getCountryName(countryCode) : '';
   
   // Get confidence - either passed in or calculate a basic one
   const confidence = validationData.confidence || {
     score: isValid ? 50 : 0,
     level: isValid ? 'medium' : 'none',
     factors: isValid ? ['basic_valid'] : ['invalid']
   };
   
   const result = {
     originalPhone,
     currentPhone: validationData.e164 || this.cleanPhoneNumber(originalPhone),
     valid: isValid,
     possible: validationData.isPossible !== false,
     formatValid: formatValid,
     error: validationData.error || null,
     
     // Phone type
     type: validationData.type || 'UNKNOWN',
     
     // Location info
     location: validationData.location || countryName || 'Unknown',
     carrier: validationData.carrier || '',
     
     // Phone formats
     e164: validationData.e164 || null,
     internationalFormat: validationData.international || null,
     nationalFormat: validationData.national || null,
     uri: validationData.uri || null,
     
     // Country details
     countryCode: countryCode,
     countryCallingCode: validationData.countryCode || null,
     
     // Confidence details
     confidence: confidence.level,
     confidenceScore: confidence.score,
     confidenceFactors: confidence.factors,
     
     // Validation method
     validationMethod: validationData.validationMethod || 'unknown',
     externalApiUsed: validationData.externalApiUsed || false,
     
     // Unmessy fields
     um_phone: validationData.international || validationData.e164 || originalPhone,
     um_phone_status: wasChanged ? 'Changed' : 'Unchanged',
     um_phone_format: formatValid ? 'Valid' : 'Invalid',
     um_phone_country_code: countryCode || '',
     um_phone_country: countryName,
     um_phone_is_mobile: validationData.isMobile || false,
     
     // Debug info
     detectedCountry: validationData.country,
     parseError: validationData.parseError || null
   };
   
   // Add additional details if available
   if (validationData.predictions) {
     result.possibleCountries = validationData.predictions;
   }
   
   if (validationData.hintCountryUsed !== undefined) {
     result.hintCountryUsed = validationData.hintCountryUsed;
   }
   
   if (validationData.attemptedCountryInput) {
     result.attemptedCountryInput = validationData.attemptedCountryInput;
   }
   
   return result;
 }
 
 // Cache operations
 async checkPhoneCache(e164Phone) {
   try {
     const { rows } = await db.select(
       'phone_validations',
       { e164: e164Phone },
       { limit: 1 }
     );
     
     const data = rows[0];
     
     if (data) {
       return {
         originalPhone: data.original_phone,
         currentPhone: data.e164,
         valid: data.valid,
         possible: true,
         formatValid: true,
         type: data.phone_type,
         location: this.getCountryName(data.country),
         carrier: data.carrier || '',
         e164: data.e164,
         internationalFormat: data.international_format,
         nationalFormat: data.national_format,
         uri: `tel:${data.e164}`,
         countryCode: data.country,
         countryCallingCode: data.country_code,
         confidence: data.confidence_level || 'high',
         confidenceScore: data.confidence_score || 90,
         confidenceFactors: ['cached_result', 'previously_validated'],
         validationMethod: data.validation_method || 'cached',
         externalApiUsed: data.external_api_used || false,
         um_phone: data.international_format,
         um_phone_status: data.original_phone !== data.international_format ? 'Changed' : 'Unchanged',
         um_phone_format: 'Valid',
         um_phone_country_code: data.country,
         um_phone_country: this.getCountryName(data.country),
         um_phone_is_mobile: data.is_mobile,
         isFromCache: true
       };
     }
     
     return null;
   } catch (error) {
     this.logger.error('Failed to check phone cache', error, { e164Phone });
     return null;
   }
 }
 
 async savePhoneCache(phone, validationResult, clientId) {
   // Only save valid phones
   if (!validationResult.valid || !validationResult.e164) {
     return;
   }
   
   try {
     await db.insert('phone_validations', {
       original_phone: phone,
       e164: validationResult.e164,
       international_format: validationResult.internationalFormat,
       national_format: validationResult.nationalFormat,
       country_code: validationResult.countryCallingCode,
       country: validationResult.countryCode,
       phone_type: validationResult.type,
       is_mobile: validationResult.um_phone_is_mobile,
       valid: validationResult.valid,
       confidence_score: validationResult.confidenceScore,
       confidence_level: validationResult.confidence,
       validation_method: validationResult.validationMethod,
       external_api_used: validationResult.externalApiUsed,
       carrier: validationResult.carrier,
       client_id: clientId
     });
     
     this.logger.debug('Phone validation saved to cache', { 
       phone: validationResult.e164,
       confidence: validationResult.confidence,
       method: validationResult.validationMethod,
       clientId 
     });
   } catch (error) {
     // Handle duplicate key errors gracefully
     if (error.code !== '23505') { // PostgreSQL unique violation
       this.logger.error('Failed to save phone validation', error, { phone });
     }
   }
 }
 
 // Utility function to get all supported countries (for reference/UI)
 getAllSupportedCountries() {
   const countries = getCountries();
   return countries.map(country => ({
     code: country,
     name: this.getCountryName(country),
     callingCode: getCountryCallingCode(country)
   }));
 }
}

// Create singleton instance
const phoneValidationService = new PhoneValidationService();

// Export the class and instance
export { phoneValidationService, PhoneValidationService };