import { useDataSourceQuery } from "@/hooks/use-data-source-query";
import { useAppDispatch, useAppSelector } from "@/hooks/use-store";
import { toast } from "@/components/ui/sonner";
import { Datasource, Project } from "@/models/api-responses";
import { dataImportActions } from "@/modules/DataImport/store/data-import-slice";
import { dataProfileActions } from "@/modules/DataProfiling/store/data-profile-slice";
import { useProjectsList } from "@/modules/ProjectManagement/hooks/use-projects-list";
import { projectActions } from "@/modules/ProjectManagement/store/projectSlice";
import { bindActionCreators } from "@reduxjs/toolkit";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import {
	createContext,
	ReactNode,
	useContext,
	useEffect,
	useLayoutEffect,
	useRef,
	useState,
} from "react";

// Profiling SaaS RouteGuard — slimmed from main-product version during saas-extract.
// Removed: license blocking, pipeline-stage redirects (Match*, Survivorship, FinalExport),
// remoteConnections clearing, purgeAllModuleState, oauth/settings/scheduler bypass.
// For the SaaS variant, only project + datasource context are gated.
// TODO (M1 proper, in new SaaS repo): replace with a single-page provider once
// auto-create-project on signup is wired up.

const LAST_ROUTE_KEY = "profiler_last_routes";

interface RouteGuardContextType {
	currentProject: Project | null;
	dataSources: Datasource[] | null | undefined;
	dataSourceId: string;
	importing: boolean;
	setDataSourceId: (id: string) => void;
	setImporting: (value: boolean) => void;
	file: File | null;
	setFile: (file: File | null) => void;
	oldRoute: string | null;
}

const RouteGuardContext = createContext<RouteGuardContextType>({
	currentProject: null,
	dataSources: undefined,
	dataSourceId: "",
	importing: false,
	setDataSourceId: () => {},
	setImporting: () => {},
	file: null,
	setFile: () => {},
	oldRoute: null,
});

export const useRouteGuard = () => useContext(RouteGuardContext);

interface RouteGuardProviderProps {
	children: ReactNode;
}

export const RouteGuardProvider = ({ children }: RouteGuardProviderProps) => {
	const router = useRouter();
	const pathname = usePathname();
	const searchParams = useSearchParams();
	const dispatch = useAppDispatch();
	const [dataSourceId, setDataSourceId] = useState<string>("");
	const [importing, setImporting] = useState<boolean>(false);
	const [oldRoute, setOldRoute] = useState<string | null>(null);
	const { selectedProject, viewMode } = useAppSelector(
		(state) => state.projects
	);
	const { data, isError, isSuccess, isLoading } = useProjectsList();
	const projects = data?.value || [];
	const { setViewMode, setSelectedProject, setStatus } = bindActionCreators(
		projectActions,
		dispatch
	);
	const { setUploadedFile } = bindActionCreators(dataImportActions, dispatch);
	const { setViewMode: setProfileViewmode, setSelectedTab: setProfileTab } =
		bindActionCreators(dataProfileActions, dispatch);
	const [file, setCurrentFile] = useState<File | null>(null);
	const setFile = (currentFile: File | null) => {
		setCurrentFile(currentFile);
		setUploadedFile(null);
	};
	const dataSourceQuery = useDataSourceQuery({
		projectId: selectedProject?.id,
	});

	const projectIdFromUrl = searchParams.get("projectId");
	const dataSourceIdFromUrl = searchParams.get("dataSourceId");
	const viewModeFromUrl = searchParams.get("viewMode");
	const params = new URLSearchParams(
		searchParams as unknown as URLSearchParams
	);

	useEffect(() => {
		if (
			!selectedProject &&
			!projectIdFromUrl &&
			pathname !== "/project-management" &&
			!isLoading
		) {
			router.push("/project-management");
		}
		if (
			projects &&
			projects.length > 0 &&
			projectIdFromUrl &&
			selectedProject?.id !== projectIdFromUrl
		) {
			const project =
				projects.find((proj) => proj.id === projectIdFromUrl) || null;
			setSelectedProject(project);
			if (project === null) {
				router.push("/project-management");
			}
		}
	}, [projects, projectIdFromUrl, pathname]);

	useEffect(() => {
		if (isLoading) setStatus("loading");
		if (isSuccess) setStatus("succeeded");
		if (isError) setStatus("failed");
	}, [isError, isLoading, isSuccess]);

	useEffect(() => {
		if (viewMode) {
			window.sessionStorage.setItem("projectViewMode", viewMode);
			params.set("viewMode", viewMode);
		} else {
			params.set(
				"viewMode",
				sessionStorage.getItem("projectViewMode") || "cards"
			);
		}
	}, [viewMode]);

	useEffect(() => {
		if (selectedProject?.id && pathname !== "/project-management") {
			const saved = JSON.parse(localStorage.getItem(LAST_ROUTE_KEY) || "{}");
			saved[selectedProject.id] = pathname;
			localStorage.setItem(LAST_ROUTE_KEY, JSON.stringify(saved));
		}
	}, [pathname, selectedProject?.id]);

	useEffect(() => {
		if (
			!pathname.startsWith("/project-management") &&
			!pathname.startsWith("/data-import") &&
			!importing
		) {
			const noDataSources =
				dataSourceQuery.data === null ||
				(Array.isArray(dataSourceQuery.data) && dataSourceQuery.data.length === 0);
			if (noDataSources && selectedProject && !dataSourceQuery.isLoading) {
				toast({
					variant: "info",
					title: "Import data first",
					description:
						"You need at least one data source before profiling.",
				});
				router.push(`/data-import?projectId=${selectedProject.id}`);
				return;
			}
			const targetSearch = params.toString();
			const currentSearch = searchParams.toString();
			if (targetSearch !== currentSearch) {
				router.replace(`${pathname}?${targetSearch}`);
			}
		}
	}, [viewMode, pathname, selectedProject?.id, projectIdFromUrl, router]);

	useEffect(() => {
		if (
			selectedProject &&
			selectedProject.id &&
			!dataSourceQuery.isLoading &&
			pathname === "/project-management"
		) {
			const hasDatasources =
				dataSourceQuery.data && dataSourceQuery.data.length > 0;
			const saved = JSON.parse(localStorage.getItem(LAST_ROUTE_KEY) || "{}");
			let lastRoute = saved[selectedProject.id];

			if (
				lastRoute === "/data-import/select-table" ||
				lastRoute === "/data-import/column-mapping"
			) {
				lastRoute = "/data-import/data-sources";
			}

			if (hasDatasources && lastRoute && lastRoute !== "/project-management") {
				router.push(lastRoute);
			} else if (hasDatasources) {
				router.push(`/data-import/data-sources`);
			} else {
				router.push(`/data-import`);
			}
		}
	}, [dataSourceQuery.isLoading, selectedProject]);

	useLayoutEffect(() => {
		if (isLoading) return;
		if (
			projects.length > 0 &&
			projectIdFromUrl &&
			selectedProject?.id !== projectIdFromUrl
		) {
			const project =
				projects.find((proj) => proj.id === projectIdFromUrl) || null;
			setSelectedProject(project);
			if (project === null) {
				router.push("/project-management");
			}
			return;
		}
		if (
			!selectedProject &&
			!projectIdFromUrl &&
			pathname !== "/project-management"
		) {
			toast({
				variant: "warning",
				title: "Please select a project first",
				description: "Click on a project card to get started",
			});
			router.push("/project-management");
		}
	}, [projects, projectIdFromUrl, pathname, isLoading]);

	useEffect(() => {
		if (dataSourceQuery.data && setDataSourceId) {
			const firstDataSourceId = dataSourceQuery.data?.[0]?.id;
			if (
				dataSourceIdFromUrl &&
				dataSourceId !== dataSourceIdFromUrl &&
				dataSourceQuery.data.find((item) => item.id === dataSourceIdFromUrl)
			) {
				setDataSourceId(dataSourceIdFromUrl);
			} else {
				if (
					dataSourceId === "" ||
					!dataSourceQuery.data.find((item) => item.id === dataSourceId)
				) {
					setDataSourceId(firstDataSourceId);
				}
			}
		}
	}, [dataSourceQuery.data, dataSourceIdFromUrl]);

	useEffect(() => {
		const viewModeSession = sessionStorage.getItem("projectViewMode");
		if (
			pathname === "/project-management" &&
			(viewModeFromUrl || viewModeSession)
		) {
			const migrateViewMode = (v: string | null): "cards" | "list" | null => {
				if (v === "cards" || v === "list") return v;
				if (v === "card") return "cards";
				if (v === "standard") return "list";
				return null;
			};
			const fromUrl = migrateViewMode(viewModeFromUrl);
			const fromSession = migrateViewMode(viewModeSession);
			if (fromUrl) setViewMode(fromUrl);
			else if (fromSession) setViewMode(fromSession);
		}
		if (pathname === "/data-profiling" && viewModeFromUrl) {
			if (viewModeFromUrl === "standard" || viewModeFromUrl === "numeric") {
				setProfileViewmode(viewModeFromUrl as "standard" | "numeric");
				setProfileTab("detailed");
			}
		}
	}, [viewModeFromUrl, pathname]);

	const previousPathnameRef = useRef<string | null>(null);
	useEffect(() => {
		const newPathname = pathname;
		const oldPathname = previousPathnameRef.current;
		if (oldPathname !== null && oldPathname !== newPathname) {
			setOldRoute(oldPathname);
		}
		previousPathnameRef.current = newPathname;
	}, [pathname]);

	const shouldBlock =
		!isLoading &&
		!selectedProject &&
		!projectIdFromUrl &&
		pathname !== "/project-management";

	const contextValue = {
		dataSourceId,
		setDataSourceId,
		dataSources: dataSourceQuery.data,
		currentProject: selectedProject,
		importing,
		setImporting,
		file,
		setFile,
		oldRoute,
	};

	if (shouldBlock) {
		return (
			<RouteGuardContext.Provider value={contextValue}>
				{null}
			</RouteGuardContext.Provider>
		);
	}

	return (
		<RouteGuardContext.Provider value={contextValue}>
			{children}
		</RouteGuardContext.Provider>
	);
};
