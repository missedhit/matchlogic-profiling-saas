/** @type {import('next').NextConfig} */

const isProd = process.env.NODE_ENV === "production";

const nextConfig = {
	// Static export is only for production builds (deployed to S3+CloudFront).
	// In dev mode, omit it — keeping it on breaks Node-polyfill module resolution
	// for packages like amazon-cognito-identity-js (Buffer/SHA-256 SRP math).
	...(isProd ? { output: "export" } : {}),
	eslint: {
		ignoreDuringBuilds: false,
	},
	typescript: {
		ignoreBuildErrors: true,
	},
};

export default nextConfig;
