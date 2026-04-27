import { type LucideIcon } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ModuleEmptyStateProps {
	icon: LucideIcon;
	title: string;
	description: string;
	actionLabel?: string;
	onAction?: () => void;
	secondaryLabel?: string;
	onSecondary?: () => void;
}

export function ModuleEmptyState({
	icon: Icon,
	title,
	description,
	actionLabel,
	onAction,
	secondaryLabel,
	onSecondary,
}: ModuleEmptyStateProps) {
	return (
		<div className="flex items-center justify-center h-64">
			<div className="text-center max-w-md">
				<Icon className="h-12 w-12 mx-auto mb-4 text-muted-foreground/50" />
				<p className="text-lg font-semibold text-muted-foreground">{title}</p>
				<p className="text-sm text-muted-foreground mt-2">{description}</p>
				{(actionLabel || secondaryLabel) && (
					<div className="flex items-center justify-center gap-3 mt-6">
						{actionLabel && onAction && (
							<Button onClick={onAction}>{actionLabel}</Button>
						)}
						{secondaryLabel && onSecondary && (
							<Button variant="outline" onClick={onSecondary}>
								{secondaryLabel}
							</Button>
						)}
					</div>
				)}
			</div>
		</div>
	);
}
