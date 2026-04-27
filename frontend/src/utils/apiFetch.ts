// src/utils/apiFetch.ts

import {
	startLoading,
	finishLoading,
	setShowLoader,
	setError,
} from "../store/uiSlice";
import type { AppDispatch, RootState } from "../store";
import { toast } from "@/components/ui/sonner";
// Bearer token injection — re-wired to Cognito ID token in M1 proper.
// During saas-extract: no token attached; backend runs without auth locally.
const getAccessToken = (): string | undefined => undefined;

interface ToastConfig {
	successTitle?: string;
	successDescription?: string;
	errorTitle?: string;
	errorDescription?: string;
}

interface ApiFetchOptions extends RequestInit {
	toastConfig?: ToastConfig;
}

/**
 * Validation error messages that should NOT trigger an error toast/throw when
 * isSuccess=false (soft failures you want to ignore visually).
 */
const BYPASS_VALIDATION_ERROR_MESSAGES = new Set<string>([
	// "DataSource name with this name already exists.",
]);

/**
 * Normal error message prefixes that should be bypassed (prefix match).
 * If ANY error string in `errors[]` starts with one of these, it will be considered bypassable.
 * Add exact prefixes only (we match with startsWith).
 */
const BYPASS_ERROR_PREFIXES: string[] = [
	"No data sources found for project ID:",
	"No row reference data found for the specified document ID:",
	"No profile data found for DataSourceId:",
	"No data found for RunId:"
];

/** Helper: does this error string match any bypass prefix? */
function isErrorBypassedByPrefix(msg: unknown): boolean {
	if (typeof msg !== "string") return false;
	return BYPASS_ERROR_PREFIXES.some((p) => msg.startsWith(p));
}

/** Collects human-readable error messages from a typical API payload shape. */
function collectErrorMessages(payload: any): string[] {
	const msgs: string[] = [];

	// payload.errors may be an array of strings or objects
	if (Array.isArray(payload?.errors)) {
		for (const e of payload.errors) {
			if (!e) continue;
			if (typeof e === "string") msgs.push(e);
			else if (typeof (e as any)?.errorMessage === "string")
				msgs.push((e as any).errorMessage);
			else if (typeof (e as any)?.message === "string")
				msgs.push((e as any).message);
		}
	}

	// payload.validationErrors is usually array of { identifier, errorMessage, ... }
	if (Array.isArray(payload?.validationErrors)) {
		for (const ve of payload.validationErrors) {
			if (ve?.errorMessage) msgs.push(ve.errorMessage);
		}
	}

	// fallback
	if (msgs.length === 0 && typeof payload?.message === "string") {
		msgs.push(payload.message);
	}
	if (msgs.length === 0 && typeof payload?.error === "string") {
		msgs.push(payload.error);
	}

	return msgs.filter(Boolean);
}

/**
 * A wrapper around fetch that:
 *  • handles loader state
 *  • shows success/error toasts
 *  • NEW: handles 200 + isSuccess=false with bypass for validationErrors and normal errors (prefix match)
 */
export async function apiFetch<T = any>(
	dispatch: AppDispatch,
	getState: () => RootState,
	input: RequestInfo,
	init: ApiFetchOptions = {}
): Promise<T> {
	const { toastConfig, ...fetchInit } = init;

	// Skip loader?
	const skipLoader: boolean =
		fetchInit.headers instanceof Headers
			? fetchInit.headers.has("X-Skip-Loader")
			: (fetchInit.headers as any)?.["X-Skip-Loader"] === "true";

	// Skip ALL toasts?
	const skipToast: boolean =
		fetchInit.headers instanceof Headers
			? fetchInit.headers.has("X-Skip-Toast")
			: (fetchInit.headers as any)?.["X-Skip-Toast"] === "true";

	// Skip only success toast? (error/validation toasts still fire)
	const skipSuccessToast: boolean =
		fetchInit.headers instanceof Headers
			? fetchInit.headers.has("X-Skip-Success-Toast")
			: (fetchInit.headers as any)?.["X-Skip-Success-Toast"] === "true";

	if (!skipLoader) {
		dispatch(startLoading());
		setTimeout(() => {
			const { loadingCount } = getState().uiState;
			if (loadingCount > 0) {
				dispatch(setShowLoader(true));
			}
		}, 300);
	}

	// Build URL
	let url: string;
	const base = process.env.NEXT_PUBLIC_API_URL ?? "";
	if (typeof input === "string") {
		const hdrs =
			fetchInit.headers instanceof Headers
				? Object.fromEntries(fetchInit.headers.entries())
				: (fetchInit.headers as Record<string, any> | undefined);

		if (hdrs?.["external-call"] || hdrs?.["local-asset"]) {
			url = input;
		} else {
			url = base + input;
		}
	} else {
		url = input.url;
	}

	let response: Response | undefined;
	try {
		const mergedHeaders = new Headers(
			fetchInit.headers instanceof Headers
				? Array.from(fetchInit.headers.entries())
				: (fetchInit.headers as Record<string, any>) ?? {}
		);
		mergedHeaders.set("x-requested-with", "XMLHttpRequest");

		// Inject Bearer token — skip if caller already set Authorization explicitly
		const token = getAccessToken();
		if (token && !mergedHeaders.has("Authorization")) {
			mergedHeaders.set("Authorization", `Bearer ${token}`);
		}

		response = await fetch(url, {
			...fetchInit,
			headers: mergedHeaders,
		});

		let payload: any;
		const rawText = await response.text();
		try {
			payload = JSON.parse(rawText);
		} catch {
			payload = rawText;
		}

		// Non-2xx
		if (!response.ok) {
			const errMsg =
				response.status === 403
					? "You don't have permissions for this action"
					: (payload && (payload as any).message) ||
					  response.statusText ||
					  `HTTP ${response.status}`;
			dispatch(setError(errMsg));

			if (!skipToast) {
				toast({
					title: toastConfig?.errorTitle || "Error",
					description: response.status === 403 ? errMsg : (toastConfig?.errorDescription || errMsg),
					variant: "error",
				});
			}

			// throw new Error(errMsg);
			return {} as T
		}

		// 2xx but isSuccess=false
		const hasIsSuccessFlag = Object.prototype.hasOwnProperty.call(
			payload ?? {},
			"isSuccess"
		);
		if (hasIsSuccessFlag && payload?.isSuccess === false) {
			const allMessages = collectErrorMessages(payload);

			const validationMessages = Array.isArray(payload?.validationErrors)
				? payload.validationErrors
					.map((v: any) => v?.errorMessage)
					.filter((m: any) => typeof m === "string")
				: [];

			const nonBypassedValidation = validationMessages.filter(
				(m: string) => !BYPASS_VALIDATION_ERROR_MESSAGES.has(m)
			);

			// Separate "other errors" (anything that's not one of the validation messages)
			const otherErrors = allMessages.filter(
				(m) => !validationMessages.includes(m)
			);

			// Apply the new prefix-based bypass for normal errors
			const nonBypassedOtherErrors = otherErrors.filter(
				(m) => !isErrorBypassedByPrefix(m)
			);

			// Surface if any non-bypassed validation OR non-bypassed normal errors remain
			const shouldSurfaceError =
				nonBypassedValidation.length > 0 || nonBypassedOtherErrors.length > 0;
			if (shouldSurfaceError) {
				const errMsg =
					nonBypassedValidation[0] ||
					nonBypassedOtherErrors[0] ||
					toastConfig?.errorDescription ||
					"Request failed";

				dispatch(setError(errMsg));

				if (!skipToast) {
					toast({
						title: toastConfig?.errorTitle || "Error",
						description: errMsg,
						variant: "error",
					});
				}

				//   throw new Error(errMsg);
			}

			// If here: all validation errors are bypassed AND all normal errors are bypassed by prefix
			// Return quietly, no success toast since isSuccess=false.
			return payload as T;
		}

		// Success path (mutations only) — only show toast when an explicit message exists
		if (
			!skipToast &&
			!skipSuccessToast &&
			["POST", "PUT", "DELETE", "PATCH"].includes(
				(fetchInit.method || "").toUpperCase()
			)
		) {
			const successMessage =
				payload?.successMessage ||
				toastConfig?.successDescription;
			if (successMessage) {
				toast({
					title: toastConfig?.successTitle || "Success",
					description: successMessage,
					variant: "success",
				});
			}
		}

		return payload as T;
	} catch (err: any) {
		if (!response) {
			const msg = err?.message || "Network error";
			dispatch(setError(msg));

			if (!skipToast) {
				toast({
					title: toastConfig?.errorTitle || "Network Error",
					description: toastConfig?.errorDescription || msg,
					variant: "error",
				});
			}
		}
		throw err;
	} finally {
		if (!skipLoader) {
			dispatch(finishLoading());
		}
	}
}
