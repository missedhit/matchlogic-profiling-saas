"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/hooks/use-auth";

export function AuthRedirect() {
	const router = useRouter();
	const { isInitialized, isAuthenticated } = useAuth();

	useEffect(() => {
		if (!isInitialized) return;
		router.replace(isAuthenticated ? "/project-management" : "/login");
	}, [isInitialized, isAuthenticated, router]);

	return null;
}
