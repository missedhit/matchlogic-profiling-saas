"use client";

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";
import { usePermission } from "@/hooks/use-permission";
import {
	Tooltip,
	TooltipTrigger,
	TooltipContent,
} from "@/components/ui/tooltip";

const buttonVariants = cva(
	"inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-all disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg:not([class*='size-'])]:size-4 shrink-0 [&_svg]:shrink-0 outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40 aria-invalid:border-destructive",
	{
		variants: {
			variant: {
				default:
					"bg-primary text-primary-foreground shadow-xs hover:bg-primary/90",
				destructive:
					"bg-destructive text-white shadow-xs hover:bg-destructive/90 focus-visible:ring-destructive/20 dark:focus-visible:ring-destructive/40 dark:bg-destructive/60",
				outline:
					"border bg-background shadow-xs hover:bg-accent hover:text-accent-foreground dark:bg-input/30 dark:border-input dark:hover:bg-input/50",
				secondary:
					"bg-secondary text-secondary-foreground shadow-xs hover:bg-secondary/80",
				iris: "bg-iris-mist text-primary border border-primary shadow-xs hover:bg-iris-mist/90 focus-visible:ring-iris-mist/20 dark:focus-visible:ring-iris-mist/40 dark:bg-iris-mist/60",
				ghost:
					"hover:bg-accent hover:text-accent-foreground dark:hover:bg-accent/50",
				link: "text-primary underline-offset-4 hover:underline",
			},
			size: {
				default: "h-9 px-4 py-2 has-[>svg]:px-3",
				sm: "h-8 rounded-md gap-1.5 px-3 has-[>svg]:px-2.5",
				lg: "h-10 rounded-md px-6 has-[>svg]:px-4",
				icon: "size-9",
			},
		},
		defaultVariants: {
			variant: "default",
			size: "default",
		},
	}
);

interface ButtonProps extends React.ComponentProps<"button"> {
	asChild?: boolean;
	/** Permission string to check. If the user lacks this permission, the button is disabled with a tooltip. */
	requiredPermission?: string;
	/** Tooltip text shown when the button is disabled for any non-permission reason. */
	disabledReason?: string;
	/** Custom tooltip text for permission denial. Defaults to the hook's reason message. */
	permissionDeniedMessage?: string;
}

function Button({
	className,
	variant,
	size,
	asChild = false,
	requiredPermission,
	disabledReason,
	permissionDeniedMessage,
	disabled,
	children,
	...props
}: ButtonProps &
	VariantProps<typeof buttonVariants> & {
		asChild?: boolean;
	}) {
	const permissionResult = usePermission(requiredPermission);

	const permissionDenied = !permissionResult.allowed && !permissionResult.loading;
	const effectiveDisabled = disabled || permissionDenied;

	// Determine tooltip text, in priority order:
	// 1. Permission denied → permissionDeniedMessage or hook reason
	// 2. Externally disabled with a reason → disabledReason
	// 3. No tooltip
	let tooltipMessage: string | undefined;
	if (permissionDenied) {
		tooltipMessage = permissionDeniedMessage ?? permissionResult.reason;
	} else if (disabled && disabledReason) {
		tooltipMessage = disabledReason;
	}

	const Comp = asChild ? Slot : "button";

	const buttonElement = (
		<Comp
			data-slot="button"
			className={cn(buttonVariants({ variant, size, className }))}
			disabled={effectiveDisabled}
			{...props}
		>
			{children}
		</Comp>
	);

	// Only wrap in Tooltip when there is a message and asChild is not used
	// (asChild composes into another element that manages its own rendering)
	if (tooltipMessage && !asChild) {
		return (
			<Tooltip>
				<TooltipTrigger asChild>
					{/* span wrapper needed: disabled buttons don't fire mouse events for tooltips */}
					<span tabIndex={0} className="inline-flex">
						{buttonElement}
					</span>
				</TooltipTrigger>
				<TooltipContent side="top">{tooltipMessage}</TooltipContent>
			</Tooltip>
		);
	}

	return buttonElement;
}

export { Button, buttonVariants };
export type { ButtonProps };
