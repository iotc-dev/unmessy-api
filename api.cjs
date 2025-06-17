// api.cjs - Vercel entry point (CommonJS)

module.exports = async (req, res) => {
  const { default: app } = await import('./src/api/vercel.js');
  return app(req, res);
};