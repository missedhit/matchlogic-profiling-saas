"use client";

import { useRouter } from "next/navigation";
import { useAppDispatch, useAppSelector } from "@/hooks/use-store";
import {
	clearAuth,
	selectIsAuthenticated,
	selectIsInitialized,
	selectCurrentUser,
} from "@/store/authSlice";
import { signOut as cognitoSignOut } from "@/lib/cognito";

export function useAuth() {
	const router = useRouter();
	const dispatch = useAppDispatch();
	const isAuthenticated = useAppSelector(selectIsAuthenticated);
	const isInitialized = useAppSelector(selectIsInitialized);
	const currentUser = useAppSelector(selectCurrentUser);

	const authEnabled = true;

	const logout = () => {
		cognitoSignOut();
		dispatch(clearAuth());
		router.push("/login");
	};

	const goToProfile = () => {
		router.push("/account");
	};

	const hasRole = (role: string): boolean => currentUser.roles.includes(role);

	const hasAnyRole = (roles: string[]): boolean =>
		roles.some((r) => currentUser.roles.includes(r));

	const initials = (() => {
		if (!currentUser.username) return "?";
		const parts = currentUser.username.trim().split(/[\s._-]+/);
		if (parts.length >= 2) {
			return (parts[0][0] + parts[1][0]).toUpperCase();
		}
		return currentUser.username.slice(0, 2).toUpperCase();
	})();

	return {
		isAuthenticated,
		isInitialized,
		authEnabled,
		...currentUser,
		initials,
		accountUrl: "/account",
		logout,
		goToProfile,
		hasRole,
		hasAnyRole,
	};
}
