import { useAppSelector } from "@/hooks/use-store";
import {
	selectIsAuthenticated,
	selectIsInitialized,
	selectCurrentUser,
} from "@/store/authSlice";

// Profiling SaaS use-auth — slimmed from main-product version during saas-extract.
// Removed: Keycloak getKeycloak()/accountManagement()/logout() calls.
// TODO (M1 proper, in new SaaS repo): re-wire to Cognito sign-out + account page.
export function useAuth() {
	const isAuthenticated = useAppSelector(selectIsAuthenticated);
	const isInitialized = useAppSelector(selectIsInitialized);
	const currentUser = useAppSelector(selectCurrentUser);

	const authEnabled = false;

	const logout = () => {
		// no-op until Cognito wiring lands in M1
	};

	const goToProfile = () => {
		// no-op until /account page lands in M1
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
		accountUrl: "",
		logout,
		goToProfile,
		hasRole,
		hasAnyRole,
	};
}
