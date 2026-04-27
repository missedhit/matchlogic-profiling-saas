"use client";

import { createContext, useContext, useEffect, useRef, type ReactNode } from "react";
import { useAppDispatch } from "@/hooks/use-store";
import { clearAuth, setAuthState } from "@/store/authSlice";
import {
	confirmForgotPassword,
	confirmSignUp,
	forgotPassword,
	isCognitoConfigured,
	refreshSession,
	resendConfirmation,
	restoreSession,
	signIn,
	signOut,
	signUp,
	subscribeToTokens,
	type CognitoSession,
} from "@/lib/cognito";

interface CognitoContextValue {
	signUp: typeof signUp;
	confirmSignUp: typeof confirmSignUp;
	resendConfirmation: typeof resendConfirmation;
	signIn: (email: string, password: string) => Promise<CognitoSession>;
	signOut: () => void;
	forgotPassword: typeof forgotPassword;
	confirmForgotPassword: typeof confirmForgotPassword;
}

const CognitoContext = createContext<CognitoContextValue | null>(null);

export const useCognito = (): CognitoContextValue => {
	const ctx = useContext(CognitoContext);
	if (!ctx) throw new Error("useCognito must be used within CognitoProvider");
	return ctx;
};

// Refresh five minutes before id-token expiry. Cognito ID tokens default to 60 min,
// so this gives one cycle of headroom against clock drift + slow network.
const REFRESH_LEAD_MS = 5 * 60 * 1000;

export function CognitoProvider({ children }: { children: ReactNode }) {
	const dispatch = useAppDispatch();
	const refreshTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

	useEffect(() => {
		// Dev short-circuit: if pool isn't configured, mark initialised + unauthenticated
		// so the rest of the app doesn't hang on a forever-loading splash.
		if (!isCognitoConfigured()) {
			dispatch(setAuthState({ isInitialized: true, isAuthenticated: false }));
			return;
		}

		const applySession = (session: CognitoSession | null) => {
			if (session) {
				dispatch(
					setAuthState({
						isAuthenticated: true,
						isInitialized: true,
						userId: session.identity.sub,
						username: session.identity.username,
						email: session.identity.email,
					}),
				);
			} else {
				dispatch(setAuthState({ isAuthenticated: false, isInitialized: true }));
			}
		};

		restoreSession()
			.then(applySession)
			.catch(() => applySession(null));

		const unsubscribe = subscribeToTokens((next) => {
			if (refreshTimer.current) {
				clearTimeout(refreshTimer.current);
				refreshTimer.current = null;
			}
			if (!next) {
				dispatch(clearAuth());
				dispatch(setAuthState({ isInitialized: true }));
				return;
			}
			const delay = Math.max(0, next.expiresAt - Date.now() - REFRESH_LEAD_MS);
			refreshTimer.current = setTimeout(() => {
				refreshSession()
					.then((s) => applySession(s))
					.catch(() => applySession(null));
			}, delay);
		});

		return () => {
			unsubscribe();
			if (refreshTimer.current) clearTimeout(refreshTimer.current);
		};
	}, [dispatch]);

	const value: CognitoContextValue = {
		signUp,
		confirmSignUp,
		resendConfirmation,
		signIn,
		signOut,
		forgotPassword,
		confirmForgotPassword,
	};

	return <CognitoContext.Provider value={value}>{children}</CognitoContext.Provider>;
}
