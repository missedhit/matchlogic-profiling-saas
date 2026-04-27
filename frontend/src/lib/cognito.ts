import {
	AuthenticationDetails,
	CognitoUser,
	CognitoUserAttribute,
	CognitoUserPool,
	CognitoUserSession,
	type ISignUpResult,
} from "amazon-cognito-identity-js";

// Profiling SaaS Cognito shim — Keycloak-shaped surface so lifted code stays auth-agnostic.
// Backend M1a (CognitoJwtSetup) accepts both ID and access tokens; we send the ID token
// because it carries `email` / `cognito:username` claims that simplify request-log debugging.

const POOL_ID = process.env.NEXT_PUBLIC_COGNITO_USER_POOL_ID ?? "";
const CLIENT_ID = process.env.NEXT_PUBLIC_COGNITO_CLIENT_ID ?? "";

export interface CognitoTokens {
	idToken: string;
	accessToken: string;
	refreshToken: string;
	expiresAt: number;
}

export interface CognitoIdentity {
	sub: string;
	email: string;
	username: string;
}

export interface CognitoSession {
	tokens: CognitoTokens;
	identity: CognitoIdentity;
}

const NOT_CONFIGURED =
	"Cognito not configured. Set NEXT_PUBLIC_COGNITO_USER_POOL_ID + NEXT_PUBLIC_COGNITO_CLIENT_ID in .env.local.";

export const isCognitoConfigured = (): boolean =>
	POOL_ID.length > 0 && CLIENT_ID.length > 0;

let pool: CognitoUserPool | null = null;
const getPool = (): CognitoUserPool => {
	if (!isCognitoConfigured()) throw new Error(NOT_CONFIGURED);
	if (!pool) pool = new CognitoUserPool({ UserPoolId: POOL_ID, ClientId: CLIENT_ID });
	return pool;
};

const buildUser = (email: string): CognitoUser =>
	new CognitoUser({ Username: email, Pool: getPool() });

// Module-level token store. Sync read for apiFetch; CognitoProvider keeps it warm.
let tokens: CognitoTokens | null = null;
const subscribers = new Set<(t: CognitoTokens | null) => void>();

const notify = () => {
	for (const cb of subscribers) cb(tokens);
};

const setTokens = (next: CognitoTokens | null) => {
	tokens = next;
	notify();
};

export const subscribeToTokens = (cb: (t: CognitoTokens | null) => void): (() => void) => {
	subscribers.add(cb);
	cb(tokens);
	return () => {
		subscribers.delete(cb);
	};
};

export const getIdTokenSync = (): string | undefined => tokens?.idToken;

const sessionToTokens = (session: CognitoUserSession): CognitoTokens => ({
	idToken: session.getIdToken().getJwtToken(),
	accessToken: session.getAccessToken().getJwtToken(),
	refreshToken: session.getRefreshToken().getToken(),
	expiresAt: session.getIdToken().getExpiration() * 1000,
});

const sessionToIdentity = (session: CognitoUserSession): CognitoIdentity => {
	const payload = session.getIdToken().decodePayload();
	const email = (payload.email as string) ?? "";
	return {
		sub: (payload.sub as string) ?? "",
		email,
		username: ((payload["cognito:username"] as string) ?? email).split("@")[0],
	};
};

export const signUp = (email: string, password: string): Promise<{ userSub: string }> => {
	if (!isCognitoConfigured()) return Promise.reject(new Error(NOT_CONFIGURED));
	return new Promise((resolve, reject) => {
		const attrs = [new CognitoUserAttribute({ Name: "email", Value: email })];
		getPool().signUp(email, password, attrs, [], (err, result?: ISignUpResult) => {
			if (err || !result) return reject(err ?? new Error("SignUp returned no result"));
			resolve({ userSub: result.userSub });
		});
	});
};

export const confirmSignUp = (email: string, code: string): Promise<void> => {
	if (!isCognitoConfigured()) return Promise.reject(new Error(NOT_CONFIGURED));
	return new Promise((resolve, reject) => {
		buildUser(email).confirmRegistration(code, true, (err) => {
			if (err) return reject(err);
			resolve();
		});
	});
};

export const resendConfirmation = (email: string): Promise<void> => {
	if (!isCognitoConfigured()) return Promise.reject(new Error(NOT_CONFIGURED));
	return new Promise((resolve, reject) => {
		buildUser(email).resendConfirmationCode((err) => {
			if (err) return reject(err);
			resolve();
		});
	});
};

export const signIn = (email: string, password: string): Promise<CognitoSession> => {
	if (!isCognitoConfigured()) return Promise.reject(new Error(NOT_CONFIGURED));
	return new Promise((resolve, reject) => {
		const user = buildUser(email);
		const auth = new AuthenticationDetails({ Username: email, Password: password });
		user.authenticateUser(auth, {
			onSuccess: (session) => {
				const next: CognitoSession = {
					tokens: sessionToTokens(session),
					identity: sessionToIdentity(session),
				};
				setTokens(next.tokens);
				resolve(next);
			},
			onFailure: (err) => reject(err),
			newPasswordRequired: () =>
				reject(new Error("New password required. Reset via forgot-password flow.")),
		});
	});
};

export const signOut = (): void => {
	if (!isCognitoConfigured()) {
		setTokens(null);
		return;
	}
	getPool().getCurrentUser()?.signOut();
	setTokens(null);
};

const getCurrentUserSession = (): Promise<CognitoSession | null> =>
	new Promise((resolve, reject) => {
		const user = getPool().getCurrentUser();
		if (!user) return resolve(null);
		user.getSession((err: Error | null, session: CognitoUserSession | null) => {
			if (err || !session) return reject(err ?? new Error("No session"));
			if (!session.isValid()) return resolve(null);
			resolve({
				tokens: sessionToTokens(session),
				identity: sessionToIdentity(session),
			});
		});
	});

export const restoreSession = async (): Promise<CognitoSession | null> => {
	if (!isCognitoConfigured()) return null;
	try {
		const session = await getCurrentUserSession();
		if (session) setTokens(session.tokens);
		else setTokens(null);
		return session;
	} catch {
		setTokens(null);
		return null;
	}
};

export const refreshSession = (): Promise<CognitoSession | null> =>
	new Promise((resolve, reject) => {
		if (!isCognitoConfigured()) return resolve(null);
		const user = getPool().getCurrentUser();
		if (!user) return resolve(null);
		user.getSession((err: Error | null, current: CognitoUserSession | null) => {
			if (err || !current) return reject(err ?? new Error("No session"));
			user.refreshSession(current.getRefreshToken(), (refreshErr, fresh) => {
				if (refreshErr || !fresh) return reject(refreshErr ?? new Error("Refresh failed"));
				const next: CognitoSession = {
					tokens: sessionToTokens(fresh),
					identity: sessionToIdentity(fresh),
				};
				setTokens(next.tokens);
				resolve(next);
			});
		});
	});

export const forgotPassword = (email: string): Promise<void> => {
	if (!isCognitoConfigured()) return Promise.reject(new Error(NOT_CONFIGURED));
	return new Promise((resolve, reject) => {
		buildUser(email).forgotPassword({
			onSuccess: () => resolve(),
			onFailure: (err) => reject(err),
		});
	});
};

export const confirmForgotPassword = (
	email: string,
	code: string,
	newPassword: string,
): Promise<void> => {
	if (!isCognitoConfigured()) return Promise.reject(new Error(NOT_CONFIGURED));
	return new Promise((resolve, reject) => {
		buildUser(email).confirmPassword(code, newPassword, {
			onSuccess: () => resolve(),
			onFailure: (err) => reject(err),
		});
	});
};
