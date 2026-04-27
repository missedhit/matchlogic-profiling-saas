import pluginReact from "eslint-plugin-react";
import pluginReactHooks from "eslint-plugin-react-hooks";
import pluginNext from "@next/eslint-plugin-next";
import tsParser from "@typescript-eslint/parser";
// import tsPlugin from "@typescript-eslint/eslint-plugin";

const config = [
	// Base configuration for all files
	{
		ignores: [".next/*", "node_modules/*", "out/*", "build/*"],
	},
	// Language options
	// {
	// 	languageOptions: {
	// 		ecmaVersion: "latest",
	// 		sourceType: "module",
	// 		globals: {
	// 			...globals.browser,
	// 			...globals.node,
	// 		},
	// 	},
	// },
	// React configuration
	{
		files: ["**/*.{js,jsx,ts,tsx}"],
		plugins: {
			react: pluginReact,
			"react-hooks": pluginReactHooks,
		},
		rules: {
			...pluginReact.configs["jsx-runtime"].rules,
			...pluginReactHooks.configs.recommended.rules,
		},
		settings: {
			react: {
				version: "detect", // Automatically detect the React version
			},
		},
	},
	// TypeScript configuration (if applicable)
	{
		files: ["**/*.{ts,tsx}"],
		languageOptions: {
			parser: tsParser,
			parserOptions: {
				project: "./tsconfig.json",
			},
		},
		plugins: {
			// "@typescript-eslint": tsPlugin,
		},
		rules: {
			// ...tsPlugin.configs.recommended.rules,
			// Add or override specific TypeScript rules here
		},
	},
	// Next.js configuration
	{
		files: ["**/*.{js,jsx,ts,tsx}"],
		plugins: {
			"@next/next": pluginNext,
		},
		rules: {
			...pluginNext.configs.recommended.rules,
			// Use the 'core-web-vitals' configuration for stronger rules
			...pluginNext.configs["core-web-vitals"].rules,
		},
	},
];

export default config;
