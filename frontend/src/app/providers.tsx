// app/providers.tsx
"use client";

import { store } from "@/store";
import { Provider } from "react-redux";
import { QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import { queryClient } from "@/store/query-client";
import { JobStateProvider } from "@/providers/job-state-provider";
import { RouteGuardProvider } from "@/providers/route-guard-provider";

// Profiling SaaS providers — slimmed from main-product providers during saas-extract.
// Removed: KeycloakProvider, LicenseStatusLoader, FullscreenBanner, UnsavedChangesContext.
// TODO (M1 proper, in new SaaS repo): wrap with CognitoProvider here.
export function Providers({ children }: { children: React.ReactNode }) {
	return (
		<Provider store={store}>
			<QueryClientProvider client={queryClient}>
				{process.env.NODE_ENV === "development" &&
					typeof window !== "undefined" &&
					localStorage.getItem("enableDevTools") === "true" && (
						<ReactQueryDevtools initialIsOpen={false} />
					)}
				<RouteGuardProvider>
					<JobStateProvider>{children}</JobStateProvider>
				</RouteGuardProvider>
			</QueryClientProvider>
		</Provider>
	);
}
