import type React from "react";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";

interface AuthCardProps {
	title: string;
	description?: string;
	footer?: React.ReactNode;
	children: React.ReactNode;
}

export function AuthCard({ title, description, footer, children }: AuthCardProps) {
	return (
		<Card>
			<CardHeader className="text-center">
				<CardTitle className="text-2xl">{title}</CardTitle>
				{description && <CardDescription>{description}</CardDescription>}
			</CardHeader>
			<CardContent className="space-y-4">{children}</CardContent>
			{footer && (
				<CardContent className="border-t pt-4 text-center text-sm text-muted-foreground">
					{footer}
				</CardContent>
			)}
		</Card>
	);
}
