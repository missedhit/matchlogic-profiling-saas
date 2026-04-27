import type React from "react";
import Sidebar from "@/components/common/Sidebar";
import Header from "@/components/common/Header";
import { JobStateProvider } from "@/providers/job-state-provider";
import { RouteGuardProvider } from "@/providers/route-guard-provider";

export default function AppLayout({ children }: { children: React.ReactNode }) {
	return (
		<RouteGuardProvider>
			<JobStateProvider>
				<div className="flex h-screen overflow-hidden">
					<Sidebar />
					<div className="flex flex-col flex-1 overflow-hidden">
						<Header />
						<div className="relative flex-1 overflow-hidden">
							<main className="flex-1 overflow-auto bg-background p-8 h-full">
								{children}
							</main>
						</div>
					</div>
				</div>
			</JobStateProvider>
		</RouteGuardProvider>
	);
}
