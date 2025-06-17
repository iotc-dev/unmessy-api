// api/index.js
// This file serves as the entry point for Vercel serverless functions
// It imports and re-exports the Express app from src/api/index.js

import app from '../src/api/index.js';

// Export the Express app as the default export for Vercel
export default app;