// api/index.js
// This file serves as the entry point for Vercel serverless functions
// It imports and re-exports the Express app from src/api/index.js

// Use dynamic import to handle ES modules from CommonJS
module.exports = async (req, res) => {
  const { default: app } = await import('../src/api/index.js');
  return app(req, res);
};