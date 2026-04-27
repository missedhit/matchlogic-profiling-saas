"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";

import { AuthCard } from "@/components/auth/auth-card";
import { toast } from "@/components/ui/sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useCognito } from "@/providers/cognito-provider";

const verifySchema = z.object({
	code: z.string().regex(/^\d{6}$/, "Enter the 6-digit code from your email"),
});

type VerifyValues = z.infer<typeof verifySchema>;

function VerifyForm() {
	const router = useRouter();
	const searchParams = useSearchParams();
	const email = searchParams.get("email") ?? "";
	const { confirmSignUp, resendConfirmation } = useCognito();
	const [submitting, setSubmitting] = useState(false);
	const [resending, setResending] = useState(false);
	const {
		register,
		handleSubmit,
		formState: { errors },
	} = useForm<VerifyValues>({ resolver: zodResolver(verifySchema) });

	const onSubmit = async (values: VerifyValues) => {
		if (!email) {
			toast({
				variant: "error",
				title: "Missing email",
				description: "Open the verify link from the signup page.",
			});
			return;
		}
		setSubmitting(true);
		try {
			await confirmSignUp(email, values.code);
			toast({
				variant: "success",
				title: "Email verified",
				description: "Sign in to continue.",
			});
			router.push("/login");
		} catch (err) {
			toast({
				variant: "error",
				title: "Verification failed",
				description: err instanceof Error ? err.message : "Unknown error",
			});
		} finally {
			setSubmitting(false);
		}
	};

	const handleResend = async () => {
		if (!email) return;
		setResending(true);
		try {
			await resendConfirmation(email);
			toast({ variant: "info", title: "Code resent", description: `Sent to ${email}` });
		} catch (err) {
			toast({
				variant: "error",
				title: "Resend failed",
				description: err instanceof Error ? err.message : "Unknown error",
			});
		} finally {
			setResending(false);
		}
	};

	return (
		<AuthCard
			title="Verify your email"
			description={email ? `Enter the 6-digit code sent to ${email}` : "Enter the 6-digit code sent to your email"}
			footer={
				<>
					Wrong email?{" "}
					<Link href="/signup" className="font-medium text-primary hover:underline">
						Start over
					</Link>
				</>
			}
		>
			<form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
				<div className="space-y-1.5">
					<Label htmlFor="code">Verification code</Label>
					<Input
						id="code"
						type="text"
						inputMode="numeric"
						autoComplete="one-time-code"
						maxLength={6}
						{...register("code")}
					/>
					{errors.code && (
						<p className="text-xs text-destructive">{errors.code.message}</p>
					)}
				</div>
				<Button type="submit" className="w-full" disabled={submitting}>
					{submitting ? "Verifying…" : "Verify email"}
				</Button>
				<Button
					type="button"
					variant="ghost"
					className="w-full"
					disabled={resending || !email}
					onClick={handleResend}
				>
					{resending ? "Resending…" : "Resend code"}
				</Button>
			</form>
		</AuthCard>
	);
}

export default function VerifyPage() {
	return (
		<Suspense fallback={null}>
			<VerifyForm />
		</Suspense>
	);
}
