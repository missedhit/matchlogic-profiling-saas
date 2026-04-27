"use client";

import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import ProfileOverview from "@/modules/DataProfiling/components/profile-overview";
import { useAppSelector } from "@/hooks/use-store";
import { useRouter, useSearchParams } from "next/navigation";
import ProfileDetails from "@/modules/DataProfiling/components/profile-details";
import { dataProfileActions } from "@/modules/DataProfiling/store/data-profile-slice";
import { bindActionCreators } from "@reduxjs/toolkit";
import { useAppDispatch } from "@/hooks/use-store";
import { Button } from "@/components/ui/button";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import { useState } from "react";
import { useRouteGuard } from "@/providers/route-guard-provider";
import { PatternOptionsModal } from "@/modules/DataProfiling/components/pattern-options-modal";
import { useRegenerateProfile } from "@/modules/DataProfiling/hooks/use-regenerate-profile";
import { toast } from "@/components/ui/sonner";
import { ModuleEmptyState } from "@/components/common/ModuleEmptyState";
import { BarChart3 } from "lucide-react";
import { useAdvanceAnalyticsQuery } from "@/modules/DataProfiling/hooks/use-advance-analytics-query";
import { Skeleton } from "@/components/ui/skeleton";
import GenerateProfile from "@/modules/DataProfiling/components/generate-profile";
import { usePermission } from "@/hooks/use-permission";
import { ShieldAlert } from "lucide-react";

export default function ProfilePageTabs() {
	const [patternModalOpen, setPatternModalOpen] = useState(false);
	const router = useRouter();
	const searchParams = useSearchParams();
	const selectedTab = useAppSelector((state) => state.dataProfile.selectedTab);
	const viewMode = useAppSelector((state) => state.dataProfile.viewMode);
	const dispatch = useAppDispatch();
	const { setSelectedTab, setViewMode } = bindActionCreators(
		dataProfileActions,
		dispatch
	);
	const { dataSourceId, dataSources, currentProject } = useRouteGuard();
	const regenerateProfile = useRegenerateProfile();
	const { data: advanceAnalytics, isLoading: isProfileLoading } = useAdvanceAnalyticsQuery({
		dataSourceId: dataSourceId || "",
	});
	const profilingPermission = usePermission("profiling.execute");
	// Profile is being generated when query returned but value is null (triggers GenerateProfile)
	const isGenerating = !isProfileLoading && dataSourceId && advanceAnalytics && !advanceAnalytics.value;

	const handleGenerateProfile = () => {
		if (!currentProject || !dataSourceId) {
			toast({
				title: "Error",
				description: "Project or data source not found",
				variant: "error",
			});
			return;
		}

		regenerateProfile.mutate({
			projectId: currentProject.id,
			dataSourceId: dataSourceId,
			projectName: currentProject.name,
		});
	};

	const handleViewModeChange = (mode: "standard" | "numeric") => {
		// Update Redux state
		setViewMode(mode);
		// Update URL to persist view mode
		const params = new URLSearchParams(searchParams.toString());
		params.set("viewMode", mode);
		router.push(`/data-profiling?${params.toString()}`, { scroll: false });
	};

	const handleTabChange = (tab: string) => {
		setSelectedTab(tab);
		const params = new URLSearchParams(searchParams.toString());
		const currentViewMode = params.get("viewMode");

		if (tab === "detailed") {
			// When switching to detailed tab, ensure viewMode is in URL
			if (!currentViewMode) {
				params.set("viewMode", viewMode);
				router.push(`/data-profiling?${params.toString()}`, { scroll: false });
			}
		} else {
			// When switching to overview, remove viewMode from URL
			if (currentViewMode) {
				params.delete("viewMode");
				router.push(`/data-profiling?${params.toString()}`, { scroll: false });
			}
		}
	};

	if (isProfileLoading && dataSourceId) {
		return (
			<div className="space-y-6">
				<Skeleton className="h-10 w-64" />
				<div className="grid grid-cols-1 md:grid-cols-3 gap-6">
					<Skeleton className="h-96 w-full" />
					<Skeleton className="h-96 w-full" />
					<Skeleton className="h-96 w-full" />
				</div>
				<div className="grid grid-cols-1 md:grid-cols-2 gap-6">
					<Skeleton className="h-96 w-full" />
					<Skeleton className="h-96 w-full" />
				</div>
			</div>
		);
	}

	if (isGenerating && currentProject && dataSourceId) {
		if (!profilingPermission.allowed) {
			return (
				<div className="flex flex-col items-center justify-center h-[50vh] text-center px-4">
					<ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
					<h2 className="text-lg font-semibold mb-2">No profiling data available</h2>
					<p className="text-sm text-muted-foreground max-w-md">
						A data profile has not been generated for this data source yet.
						Contact your administrator to generate one.
					</p>
				</div>
			);
		}
		return (
			<GenerateProfile
				selectedProject={currentProject}
				dataSourceId={dataSourceId}
			/>
		);
	}

	return (
		<>
			<Tabs
				value={selectedTab}
				className="space-y-6"
				onValueChange={handleTabChange}
			>
				<div className="flex justify-between items-center">
					<div className="flex items-center gap-4">
						<TabsList className="grid w-full grid-cols-2 gap-2 bg-transparent shadow-none border-none p-0">
							<TabsTrigger
								value="overview"
								className="bg-transparent text-muted-foreground  border-transparent rounded-none  font-semibold  border-b-border transition-colors duration-200 data-[state=active]:text-primary data-[state=active]:border-b-primary "
							>
								Overview
							</TabsTrigger>
							<TabsTrigger
								value="detailed"
								className="bg-transparent text-muted-foreground border-transparent rounded-none font-semibold  border-b-border transition-colors duration-200 data-[state=active]:text-primary data-[state=active]:border-b-primary"
							>
								Detailed Analysis
							</TabsTrigger>
						</TabsList>
					</div>
					{selectedTab === "detailed" && (
						<div className="flex items-center gap-4 ml-auto">
							<Select value={viewMode} onValueChange={handleViewModeChange}>
								<SelectTrigger className="w-[250px]">
									<SelectValue placeholder="Select View" />
								</SelectTrigger>
								<SelectContent>
									<SelectItem value="standard">Standard View</SelectItem>
									<SelectItem value="numeric">Numeric View</SelectItem>
								</SelectContent>
							</Select>
							<Button
								variant="outline"
								onClick={() => setPatternModalOpen(true)}
							>
								Pattern Options
							</Button>
						</div>
					)}
				</div>
				{dataSources && dataSourceId ? (
					<>
						<ProfileOverview dataSourceId={dataSourceId} />
						<ProfileDetails dataSourceId={dataSourceId} activeView={viewMode} />
					</>
				) : (
					<ModuleEmptyState
						icon={BarChart3}
						title="No profiling data yet"
						description="Click Generate Profile to analyze your data quality, patterns, and statistics."
						actionLabel="Generate Profile"
						onAction={handleGenerateProfile}
					/>
				)}
			</Tabs>

			<PatternOptionsModal
				open={patternModalOpen}
				onOpenChange={setPatternModalOpen}
				onGenerate={handleGenerateProfile}
				isGenerating={regenerateProfile.isPending}
			/>
		</>
	);
}
