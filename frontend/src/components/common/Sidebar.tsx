"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/utils/apiFetch";
import { store } from "@/store";
import { cn } from "../../lib/utils";
import { ChevronLeft, ChevronRight } from "lucide-react";
import {
	SidebarProjectDashboardIcon,
	SidebarDataImportIcon,
	SidebarDataProfilingIcon,
} from "@/assets/icons";
import {
	Tooltip,
	TooltipTrigger,
	TooltipContent,
} from "@/components/ui/tooltip";
import { useRouteGuard } from "@/providers/route-guard-provider";
import { useAppDispatch, useAppSelector } from "@/hooks/use-store";
import { toggleSidebar, setSidebarExpanded } from "@/store/uiSlice";

// Profiling SaaS Sidebar — slimmed from main-product version during saas-extract.
// Removed: pipeline-status gating, unsaved-changes navigation guard, all
// non-profiling modules (Cleansing, MatchConfig, MatchDef, MatchResults,
// Survivorship, FinalExport, Scheduler), admin-role badge.
const Sidebar = () => {
	const pathname = usePathname();
	const { currentProject } = useRouteGuard();
	const dispatch = useAppDispatch();
	const isExpanded = useAppSelector((state) => state.uiState.sidebarExpanded);

	const { data: versionData } = useQuery({
		queryKey: ["app-version"],
		queryFn: async () => {
			try {
				const response = await apiFetch<any>(
					store.dispatch,
					store.getState,
					"/version",
					{
						method: "GET",
						headers: { "X-Skip-Loader": "true", "X-Skip-Toast": "true" },
					}
				);
				const version =
					response?.value?.version ?? response?.value ?? response?.version;
				if (!version || typeof version !== "string") return null;
				return version;
			} catch {
				return null;
			}
		},
		retry: false,
	});

	useEffect(() => {
		const stored = localStorage.getItem("sidebar-expanded");
		if (stored !== null) dispatch(setSidebarExpanded(stored === "true"));
	}, []);

	useEffect(() => {
		localStorage.setItem("sidebar-expanded", String(isExpanded));
	}, [isExpanded]);

	const projectId = currentProject?.id ?? "";
	const menuItems = [
		{
			name: "Projects",
			selector: "/project-management",
			path: "/project-management",
			Icon: SidebarProjectDashboardIcon,
		},
		{
			name: "Data Import",
			selector: "/data-import",
			path: projectId ? `/data-import/data-sources?projectId=${projectId}` : "/data-import",
			Icon: SidebarDataImportIcon,
		},
		{
			name: "Data Profiling",
			selector: "/data-profiling",
			path: projectId ? `/data-profiling?projectId=${projectId}` : "/data-profiling",
			Icon: SidebarDataProfilingIcon,
		},
	];

	return (
		<aside
			className={cn(
				"flex flex-col border-r bg-sidebar-default transition-all duration-200 ease-in-out",
				isExpanded ? "w-56" : "w-14"
			)}
		>
			<div className="flex items-center justify-between p-3 border-b">
				{isExpanded && (
					<span className="text-sm font-semibold">MatchLogic Profiler</span>
				)}
				<button
					onClick={() => dispatch(toggleSidebar())}
					className="p-1 hover:bg-sidebar-hover rounded"
					aria-label={isExpanded ? "Collapse sidebar" : "Expand sidebar"}
				>
					{isExpanded ? <ChevronLeft size={16} /> : <ChevronRight size={16} />}
				</button>
			</div>

			<nav className="flex-1 px-2 py-3 space-y-1">
				{menuItems.map((item) => {
					const isActive = pathname.startsWith(item.selector);
					return (
						<Tooltip key={item.selector}>
							<TooltipTrigger asChild>
								<Link
									href={item.path}
									className={cn(
										"flex items-center gap-3 px-2 py-2 rounded text-sm transition-colors",
										isActive
											? "bg-sidebar-accent text-sidebar-foreground font-medium"
											: "hover:bg-sidebar-hover text-sidebar-foreground/80"
									)}
								>
									<item.Icon className="h-5 w-5 shrink-0" />
									{isExpanded && <span>{item.name}</span>}
								</Link>
							</TooltipTrigger>
							{!isExpanded && (
								<TooltipContent side="right">{item.name}</TooltipContent>
							)}
						</Tooltip>
					);
				})}
			</nav>

			{isExpanded && versionData && (
				<div className="p-3 text-xs text-muted-foreground border-t">
					v{versionData}
				</div>
			)}
		</aside>
	);
};

export default Sidebar;
