"use client";

import { usePathname } from "next/navigation";
import { GeneralHelpIcon } from "@/assets/icons";
import { Button } from "../ui/button";
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuItem,
	DropdownMenuLabel,
	DropdownMenuSeparator,
	DropdownMenuTrigger,
} from "../ui/dropdown-menu";
import { JobStatusDialog } from "@/components/common/JobStatusDialog";
import ProfilingPageHeader from "@/modules/DataProfiling/components/page-header";
import Breadcrumbs from "./Breadcrumbs";
import { useAuth } from "@/hooks/use-auth";

// Profiling SaaS Header — slimmed from main-product version during saas-extract.
// Removed: CleansingPageHeader, RunNotificationBell, Keycloak admin console link,
// User Management button.
export default function Header() {
	const pathname = usePathname();
	const { username, email, initials, logout, goToProfile, authEnabled } = useAuth();

	return (
		<header className="border-b bg-background px-6 py-3 flex items-center">
			<div className="flex items-center justify-between gap-4 w-full">
				<div className="flex items-center gap-4 min-w-0 flex-1">
					<Breadcrumbs />
					{pathname.startsWith("/data-profiling") && <ProfilingPageHeader />}
				</div>
				<div className="flex items-center gap-2 shrink-0">
					<Button variant="ghost" size="sm" className="font-normal" asChild>
						<a
							href="https://help.matchlogic.io/"
							target="_blank"
							rel="noopener noreferrer"
						>
							<GeneralHelpIcon className="h-4 w-4 mr-1.5" />
							Help
						</a>
					</Button>
					<Button variant="outline" size="sm" className="font-normal" asChild>
						<a href="mailto:contact@matchlogic.io">Contact Us</a>
					</Button>

					<div className="relative">
						<JobStatusDialog />
					</div>

					<DropdownMenu>
						<Button
							asChild
							variant="ghost"
							size="sm"
							className="rounded-lg h-8 w-8 bg-accent text-accent-foreground"
						>
							<DropdownMenuTrigger aria-label="User menu">
								{initials}
							</DropdownMenuTrigger>
						</Button>
						<DropdownMenuContent align="end">
							<DropdownMenuLabel>
								{username ?? "My Account"}
								{email && (
									<span className="block text-xs font-normal text-muted-foreground">
										{email}
									</span>
								)}
							</DropdownMenuLabel>
							{authEnabled && (
								<>
									<DropdownMenuSeparator />
									<DropdownMenuItem
										onClick={goToProfile}
										className="cursor-pointer"
									>
										Account
									</DropdownMenuItem>
									<DropdownMenuItem onClick={logout} className="cursor-pointer">
										Logout
									</DropdownMenuItem>
								</>
							)}
						</DropdownMenuContent>
					</DropdownMenu>
				</div>
			</div>
		</header>
	);
}
