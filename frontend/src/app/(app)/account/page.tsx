"use client";

import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/hooks/use-auth";

export default function AccountPage() {
	const { email, username, logout } = useAuth();

	return (
		<div className="mx-auto max-w-2xl space-y-6">
			<div>
				<h1 className="text-2xl font-semibold">Account</h1>
				<p className="text-sm text-muted-foreground">
					Manage your profiler account.
				</p>
			</div>

			<Card>
				<CardHeader>
					<CardTitle>Profile</CardTitle>
					<CardDescription>Signed in as {email ?? username ?? "—"}</CardDescription>
				</CardHeader>
				<CardContent className="flex justify-end">
					<Button variant="outline" onClick={logout}>
						Sign out
					</Button>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle>Quota usage</CardTitle>
					<CardDescription>1000 records lifetime per account.</CardDescription>
				</CardHeader>
				<CardContent>
					<p className="text-sm text-muted-foreground">
						Quota tracking lands in M4. Until then, no enforcement.
					</p>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle>Delete account</CardTitle>
					<CardDescription>
						Removes all data sources, profiles, and uploaded files.
					</CardDescription>
				</CardHeader>
				<CardContent>
					<Button variant="destructive" disabled>
						Delete account (M5)
					</Button>
				</CardContent>
			</Card>
		</div>
	);
}
