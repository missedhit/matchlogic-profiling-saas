"use client";
import React from "react";
import { toast as sonnerToast, type ToastT } from "sonner";

import {
	CircleCheckIcon,
	InfoIcon,
	Loader2Icon,
	OctagonXIcon,
	TriangleAlertIcon,
} from "lucide-react";
import { Toaster as Sonner, type ToasterProps } from "sonner";

const Toaster = ({ ...props }: ToasterProps) => {
	return (
		<Sonner
			theme="light"
			className="toaster group"
			icons={{
				success: <CircleCheckIcon className="size-4 text-emerald-600" />,
				info: <InfoIcon className="size-4 text-amber-600" />,
				warning: <TriangleAlertIcon className="size-4 text-amber-600" />,
				error: <OctagonXIcon className="size-4 text-red-600" />,
				loading: <Loader2Icon className="size-4 animate-spin text-gray-500" />,
			}}
			style={
				{
					"--normal-bg": "var(--popover)",
					"--normal-text": "var(--popover-foreground)",
					"--normal-border": "var(--border)",
					"--border-radius": "var(--radius)",
				} as React.CSSProperties
			}
			{...props}
		/>
	);
};

/** I recommend abstracting the toast function
 *  so that you can call it without having to use toast.custom everytime. */
function toast(toast: Omit<ToastProps, "id">) {
	if (toast.variant === "default") {
		return sonnerToast(toast.title, {
			description: toast.description,
			...(toast.duration && { duration: toast.duration }),
			...(toast.action && { action: toast.action }),
		});
	}
	if (toast.variant === "info") {
		return sonnerToast.info(toast.title, {
			description: toast.description,
			...(toast.duration && { duration: toast.duration }),
			...(toast.action && { action: toast.action }),
		});
	}
	if (toast.variant === "success") {
		return sonnerToast.success(toast.title, {
			description: toast.description,
			...(toast.duration && { duration: toast.duration }),
			...(toast.action && { action: toast.action }),
		});
	}
	if (toast.variant === "error") {
		return sonnerToast.error(toast.title, {
			description: toast.description,
			...(toast.duration && { duration: toast.duration }),
			...(toast.action && { action: toast.action }),
		});
	}
	if (toast.variant === "warning") {
		return sonnerToast.warning(toast.title, {
			description: toast.description,
			...(toast.duration && { duration: toast.duration }),
			...(toast.action && { action: toast.action }),
		});
	}
	return sonnerToast.info(toast.title, {
		description: toast.description,
		...(toast.duration && { duration: toast.duration }),
		...(toast.action && { action: toast.action }),
	});
	// return sonnerToast.custom((id) => (
	// 	<Toast id={id} title={toast.title} description={toast.description} />
	// ));
}

/** A fully custom toast that still maintains the animations and interactions. */
function Toast(props: ToastProps) {
	const { title, description, id } = props;

	return (
		<div className="flex rounded-lg bg-white shadow-lg ring-1 ring-black/5 w-full md:max-w-[364px] items-center p-4">
			<div className="flex flex-1 items-center">
				<div className="w-full">
					<p className="text-sm font-medium text-gray-900">{title}</p>
					<p className="mt-1 text-sm text-gray-500">{description}</p>
				</div>
			</div>
		</div>
	);
}

interface ToastProps extends Omit<ToastT, "id"> {
	id: string | number;
	variant?: "default" | "info" | "error" | "success" | "warning";
	title: string;
	description: string;
	duration?: number;
	action?: React.ReactNode;
}

export { Toaster, toast };
