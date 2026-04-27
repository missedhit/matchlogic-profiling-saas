import type { Config } from "tailwindcss";

const config: Config = {
	darkMode: ["class"],
	content: [
		"./pages/**/*.{ts,tsx}",
		"./components/**/*.{ts,tsx}",
		"./app/**/*.{ts,tsx}",
		"./src/**/*.{ts,tsx}",
		"*.{js,ts,jsx,tsx,mdx}",
	],
	theme: {
		container: {
			center: true,
			padding: "2rem",
			screens: {
				"2xl": "1400px",
			},
		},
		extend: {
			colors: {
				border: "var(--border)",
				input: "var(--input)",
				ring: "var(--ring)",
				background: "var(--background)",
				foreground: "var(--foreground)",
				primary: {
					DEFAULT: "var(--primary)",
					foreground: "var(--primary-foreground)",
				},
				secondary: {
					DEFAULT: "var(--secondary)",
					foreground: "var(--secondary-foreground)",
				},
				destructive: {
					DEFAULT: "var(--destructive)",
					foreground: "var(--destructive-foreground)",
				},
				muted: {
					DEFAULT: "var(--muted)",
					foreground: "var(--muted-foreground)",
				},
				accent: {
					DEFAULT: "var(--accent)",
					foreground: "var(--accent-foreground)",
				},
				popover: {
					DEFAULT: "var(--popover)",
					foreground: "var(--popover-foreground)",
				},
				card: {
					DEFAULT: "var(--card)",
					foreground: "var(--card-foreground)",
				},
				sidebar: {
					DEFAULT: "var(--sidebar)",
					foreground: "var(--sidebar-foreground)",
					accent: "var(--sidebar-accent)",
					hover: "var(--sidebar-hover)",
				},
				table: {
					background: "#fbf5ff",
					header: "#f6e5ff",
					row: "#ffffff",
					rowAlt: "#fbf5ff",
				},
				profile: "var(--profile-primary)",
				letters: "var(--letters)",
				lettersOnly: "var(--letters-only)",
				numbers: "var(--numbers)",
				numbersOnly: "var(--numbers-only)",
				lettersAndNumbers: "var(--letters-and-numbers)",
				punctuation: "var(--punctuation)",
				leadingSpaces: "var(--leading-spaces)",
				nonPrintable: "var(--non-printable)",
				tableCardBorder: "var(--table-card-border)",
				"node-preview": "var(--node-preview)",
				"node-preview-foreground": "var(--node-preview-foreground)",
				"node-background": "var(--node-background)",
				"node-handle-background": "var(--node-handle-background)",
				"node-handle-border": "var(--node-handle-border)",
				"start-node-background": "var(--start-node-background)",
				"start-node-foreground": "var(--start-node-foreground)",
				"end-node-background": "var(--end-node-background)",
				"end-node-foreground": "var(--end-node-foreground)",
				"abyss-purple": "var(--abyss-purple)",
				"iris-mist": "var(--iris-mist)",
				"soft-lilac": "var(--soft-lilac)",
				conf: {
					high: "#059669",
					"high-bg": "#ECFDF5",
					med: "#D97706",
					"med-bg": "#FFFBEB",
					low: "#DC2626",
					"low-bg": "#FEF2F2",
				},
				iris: {
					DEFAULT: "#5A189A",
					light: "#FBF5FF",
					mid: "#DBC9FF",
					hover: "#3E1068",
					dim: "#E8D5F5",
				},
					"badge-string": "var(--badge-string)",
				"badge-numeric": "var(--badge-numeric)",
				"badge-integer": "var(--badge-integer)",
				"badge-decimal": "var(--badge-decimal)",
				"badge-float": "var(--badge-float)",
				"badge-datetime": "var(--badge-datetime)",
				"badge-date": "var(--badge-date)",
				"badge-time": "var(--badge-time)",
				"info-bg": "var(--info-bg)",
				"info-text": "var(--info-text)",
				"success-bg": "var(--success-bg)",
				"success-text": "var(--success-text)",
				"warning-bg": "var(--warning-bg)",
				"warning-text": "var(--warning-text)",
				"error-bg": "var(--error-bg)",
				"error-text": "var(--error-text)",
			},
			boxShadow: {
				tooltip: "0px 0px 20px 0px hsla(0, 0%, 35%, 0.25)",
			},
			borderRadius: {
				lg: "var(--radius)",
				md: "calc(var(--radius) - 2px)",
				sm: "calc(var(--radius) - 4px)",
			},
			fontFamily: {
				sans: ["var(--font-sans)"],
				manrope: ["var(--font-manrope)"],
			},
			keyframes: {
				"accordion-down": {
					from: { height: "0" },
					to: { height: "var(--radix-accordion-content-height)" },
				},
				"accordion-up": {
					from: { height: "var(--radix-accordion-content-height)" },
					to: { height: "0" },
				},
				"toast-in": {
					from: { opacity: "0", transform: "translateY(100%)" },
					to: { opacity: "1", transform: "translateY(0)" },
				},
				"toast-out": {
					from: { opacity: "1", transform: "translateY(0)" },
					to: { opacity: "0", transform: "translateY(100%)" },
				},
			},
			animation: {
				"accordion-down": "accordion-down 0.2s ease-out",
				"accordion-up": "accordion-up 0.2s ease-out",
				"toast-in": "toast-in 0.3s ease-out",
				"toast-out": "toast-out 0.3s ease-in forwards",
			},
		},
	},
	plugins: [require("tailwindcss-animate")],
};

export default config;
