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

// Matches the AWS Cognito default password policy (min 8, upper, lower, digit, symbol).
const signupSchema = z
	.object({
		email: z.string().email("Enter a valid email address"),
		password: z
			.string()
			.min(8, "At least 8 characters")
			.regex(/[A-Z]/, "Needs an uppercase letter")
			.regex(/[a-z]/, "Needs a lowercase letter")
			.regex(/\d/, "Needs a digit")
			.regex(/[^A-Za-z0-9]/, "Needs a symbol"),
		confirm: z.string(),
	})
	.refine((v) => v.password === v.confirm, {
		message: "Passwords don't match",
		path: ["confirm"],
	});

type SignupValues = z.infer<typeof signupSchema>;

export default function SignupPage() {
	const router = useRouter();
	const { signUp } = useCognito();
	const [submitting, setSubmitting] = useState(false);
	const {
		register,
		handleSubmit,
		formState: { errors },
	} = useForm<SignupValues>({ resolver: zodResolver(signupSchema) });

	const onSubmit = async (values: SignupValues) => {
		setSubmitting(true);
		try {
			await signUp(values.email, values.password);
			router.push(`/verify?email=${encodeURIComponent(values.email)}`);
		} catch (err) {
			toast({
				variant: "error",
				title: "Sign-up failed",
				description: err instanceof Error ? err.message : "Unknown error",
			});
		} finally {
			setSubmitting(false);
		}
	};

	return (
		<AuthCard
			title="Create your account"
			description="Free — up to 1000 records lifetime"
			footer={
				<>
					Already have an account?{" "}
					<Link href="/login" className="font-medium text-primary hover:underline">
						Sign in
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
						autoComplete="new-password"
						{...register("password")}
					/>
					{errors.password && (
						<p className="text-xs text-destructive">{errors.password.message}</p>
					)}
				</div>
				<div className="space-y-1.5">
					<Label htmlFor="confirm">Confirm password</Label>
					<Input
						id="confirm"
						type="password"
						autoComplete="new-password"
						{...register("confirm")}
					/>
					{errors.confirm && (
						<p className="text-xs text-destructive">{errors.confirm.message}</p>
					)}
				</div>
				<Button type="submit" className="w-full" disabled={submitting}>
					{submitting ? "Creating account…" : "Create account"}
				</Button>
			</form>
		</AuthCard>
	);
}
