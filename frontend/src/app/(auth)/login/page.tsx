"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";

import { AuthCard } from "@/components/auth/auth-card";
import { toast } from "@/components/ui/sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useCognito } from "@/providers/cognito-provider";

const loginSchema = z.object({
	email: z.string().email("Enter a valid email address"),
	password: z.string().min(1, "Password is required"),
});

type LoginValues = z.infer<typeof loginSchema>;

export default function LoginPage() {
	const router = useRouter();
	const { signIn } = useCognito();
	const [submitting, setSubmitting] = useState(false);
	const {
		register,
		handleSubmit,
		formState: { errors },
	} = useForm<LoginValues>({ resolver: zodResolver(loginSchema) });

	const onSubmit = async (values: LoginValues) => {
		setSubmitting(true);
		try {
			await signIn(values.email, values.password);
			router.push("/project-management");
		} catch (err) {
			toast({
				variant: "error",
				title: "Sign-in failed",
				description: err instanceof Error ? err.message : "Unknown error",
			});
		} finally {
			setSubmitting(false);
		}
	};

	return (
		<AuthCard
			title="Sign in"
			description="Profile CSV and Excel files for free"
			footer={
				<>
					New to MatchLogic Profiler?{" "}
					<Link href="/signup" className="font-medium text-primary hover:underline">
						Create an account
					</Link>
				</>
			}
		>
			<form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
				<div className="space-y-1.5">
					<Label htmlFor="email">Email</Label>
					<Input id="email" type="email" autoComplete="email" {...register("email")} />
					{errors.email && (
						<p className="text-xs text-destructive">{errors.email.message}</p>
					)}
				</div>
				<div className="space-y-1.5">
					<Label htmlFor="password">Password</Label>
					<Input
						id="password"
						type="password"
						autoComplete="current-password"
						{...register("password")}
					/>
					{errors.password && (
						<p className="text-xs text-destructive">{errors.password.message}</p>
					)}
				</div>
				<Button type="submit" className="w-full" disabled={submitting}>
					{submitting ? "Signing in…" : "Sign in"}
				</Button>
			</form>
		</AuthCard>
	);
}
