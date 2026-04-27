import type React from "react";
import type { Metadata } from "next";
import { Inter, Manrope } from "next/font/google";
import { Toaster as SonnerToaster } from "@/components/ui/sonner";
import "./globals.css";
import { Providers } from "./providers";
import { cn } from "@/lib/utils";
import { WebVitals } from "@/components/common/web-vitals";
import { InfoIcon, XIcon } from "lucide-react";

const inter = Inter({
	subsets: ["latin"],
	variable: "--font-inter",
});

const manrope = Manrope({
	subsets: ["latin"],
	variable: "--font-manrope",
});

const fontsList = [inter.variable, manrope.variable];

export const metadata: Metadata = {
	title: "MatchLogic Profiler",
	description: "Free data profiling for CSV and Excel files",
	icons: {
		icon: "/logos/ml_favicon.svg",
	},
};

export default function RootLayout({
	children,
}: {
	children: React.ReactNode;
}) {
	return (
		<html lang="en" className={cn(fontsList.join(" "), "overflow-hidden")}>
			<body className="custom-scrollbar">
				{process.env.NODE_ENV === "development" && <WebVitals />}
				<Providers>{children}</Providers>
				<SonnerToaster
					theme="light"
					closeButton
					position="bottom-right"
					dir="auto"
					duration={3000}
					icons={{
						close: <XIcon className="h-4 w-4" />,
						info: <InfoIcon className="h-4 w-4" />,
					}}
					toastOptions={{
						style: {
							"--toast-close-button-start": "93%",
							"--toast-close-button-transform": "translate(0%, 35%)",
							"--toast-icon-margin-start": "0px",
							"--toast-icon-margin-end": "0px",
						} as React.CSSProperties,
						classNames: {
							toast: "!border !shadow-[0_4px_20px_rgba(0,0,0,0.08)]",
							info: "!bg-amber-50 !border-amber-200 !text-amber-700 !shadow-[0_4px_20px_rgba(245,158,11,0.12)]",
							success: "!bg-emerald-50 !border-emerald-200 !text-emerald-700 !shadow-[0_4px_20px_rgba(16,185,129,0.12)]",
							warning: "!bg-amber-50 !border-amber-200 !text-amber-700 !shadow-[0_4px_20px_rgba(245,158,11,0.12)]",
							error: "!bg-red-50 !border-red-200 !text-red-700 !shadow-[0_4px_20px_rgba(239,68,68,0.12)]",
							title: "text-md font-medium",
							description: "mt-1 text-sm",
							icon: "self-start mt-1",
							actionButton: "!bg-primary !text-white",
							closeButton: "!border-none !bg-transparent !text-gray-400 hover:!text-gray-600",
						},
					}}
				/>
			</body>
		</html>
	);
}
