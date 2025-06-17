// api.js - Vercel entry point (CommonJS)
// This file uses CommonJS to import the ES module app

module.exports = async (req, res) => {
  // Dynamically import the ES module
  const { default: app } = await import('./src/api/index.js');
  
  // Execute the Express app
  return app(req, res);
};