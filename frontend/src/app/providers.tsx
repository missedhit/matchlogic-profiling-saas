// app/providers.tsx
"use client";

import { store } from "@/store";
import { Provider } from "react-redux";
import { QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import { queryClient } from "@/store/query-client";
import { CognitoProvider } from "@/providers/cognito-provider";

export function Providers({ children }: { children: React.ReactNode }) {
	return (
		<Provider store={store}>
			<QueryClientProvider client={queryClient}>
				{process.env.NODE_ENV === "development" &&
					typeof window !== "undefined" &&
					localStorage.getItem("enableDevTools") === "true" && (
						<ReactQueryDevtools initialIsOpen={false} />
					)}
				<CognitoProvider>{children}</CognitoProvider>
			</QueryClientProvider>
		</Provider>
	);
}
