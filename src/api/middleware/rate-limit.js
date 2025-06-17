// In your route file
const rateLimit = require('../middleware/rate-limit');
const { authMiddleware } = require('../middleware/auth');
const { asyncHandler } = require('../middleware/error-handler');
const validationService = require('../../services/validation-service');

// Apply rate limiting to specific endpoints
router.post('/email',
  authMiddleware(),                  // First authenticate
  rateLimit.email(),                 // Then check rate limits
  asyncHandler(async (req, res) => { // Then handle the request
    const result = await validationService.validateEmail(req.body.email, {
      clientId: req.clientId
    });
    
    res.json(result);
  })
);

// Apply IP-only rate limiting to public endpoints
router.get('/public-data',
  rateLimit.ip({ ipLimit: 60 }),    // Higher limit for public endpoint
  asyncHandler(async (req, res) => {
    const data = await getPublicData();
    res.json(data);
  })
);