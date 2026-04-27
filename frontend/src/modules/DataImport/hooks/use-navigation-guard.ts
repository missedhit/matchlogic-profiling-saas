import { useAppSelector } from "@/hooks/use-store";
import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { usePermission } from "@/hooks/use-permission";

export function useNavigationGuard() {
	const { columnMappings } = useAppSelector((s) => s.dataImport);
	const router = useRouter();
	const { allowed: canImport } = usePermission("dataimport.execute");
	const [isOpen, setIsOpen] = useState(false);
	const [resolveCallback, setResolveCallback] = useState<((value: boolean) => void) | null>(null);

	const showConfirmation = (): Promise<boolean> => {
		return new Promise((resolve) => {
			setResolveCallback(() => resolve);
			setIsOpen(true);
		});
	};

	const onConfirm = () => {
		setIsOpen(false);
		resolveCallback?.(true);
		setResolveCallback(null);
	};

	const onCancel = () => {
		setIsOpen(false);
		resolveCallback?.(false);
		setResolveCallback(null);
	};
	// Only block if user has permission to import (can save) and has mappings
	const shouldBlock = canImport && Object.keys(columnMappings).length > 0;
	const onNavigationAttempt = useCallback(async () => {
		const shouldNavigate = await showConfirmation();
		if (shouldNavigate) {
			// Clear the mappings and navigate back
			// Navigate to select-table page
			setTimeout(() => {
				router.push("/data-import/select-table");
			}, 100);
		}
	}, [showConfirmation]);
	useEffect(() => {
		if (!shouldBlock) return;

		const handlePopState = async (e: PopStateEvent) => {
			// Block the navigation by pushing state again
			history.pushState(null, "", window.location.href);

			try {
				// Show confirmation and handle the result
				await onNavigationAttempt();
			} catch (error) {
				console.error("Navigation error:", error);
			}
		};

		// Push initial state to detect back button
		history.pushState(null, "", window.location.href);

		window.addEventListener("popstate", handlePopState);

		return () => {
			window.removeEventListener("popstate", handlePopState);
		};
	}, [shouldBlock, onNavigationAttempt]);

	// Handle page refresh/close
	useEffect(() => {
		if (!shouldBlock) return;

		const handleBeforeUnload = (e: BeforeUnloadEvent) => {
			e.preventDefault();
			e.returnValue = "";
			return "";
		};

		window.addEventListener("beforeunload", handleBeforeUnload);
		return () => window.removeEventListener("beforeunload", handleBeforeUnload);
	}, [shouldBlock]);
	return { showConfirmation, onConfirm, onCancel, isOpen };
}