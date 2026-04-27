export interface UsePermissionResult {
	allowed: boolean;
	loading: boolean;
	reason: string | undefined;
}

// Profiling SaaS use-permission — slimmed from main-product version during saas-extract.
// SaaS variant has no RBAC layer (all signed-in users have the same permissions),
// so this always returns allowed. Soft-gates in lifted code now no-op cleanly.
export function usePermission(_permission?: string): UsePermissionResult {
	return { allowed: true, loading: false, reason: undefined };
}
