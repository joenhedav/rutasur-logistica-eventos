/** @type {import('next').NextConfig} */
module.exports = {
  output: 'standalone',
  experimental: { serverComponentsExternalPackages: ['mongodb'] },
};
