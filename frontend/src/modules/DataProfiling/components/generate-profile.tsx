import { useGenerateAdvanceAnalytics } from "../hooks/use-generate-advance-analytics";
import { useQueryClient } from "@tanstack/react-query";
import React, { useCallback, useEffect } from "react";
import { useRunStatus } from "@/hooks/use-run-status";
import { Skeleton } from "@/components/ui/skeleton";
import { Project } from "@/models/api-responses";
import { useJobState } from "@/providers/job-state-provider";
import { queryClient } from "@/store/query-client";
import { ProfilingLoader } from "./profiling-loader";
import { AdvanceAnalytics } from "../models/advance-analytics";
import { useRegenerateProfile } from "../hooks/use-regenerate-profile";
import { Button } from "@/components/ui/button";
import { XCircle, RotateCcw } from "lucide-react";

interface GenerateProfileProps {
	selectedProject: Project;
	dataSourceId: string;
	advanceAnalytics?: AdvanceAnalytics;
}

export default function GenerateProfile({
	selectedProject,
	dataSourceId,
	advanceAnalytics,
}: GenerateProfileProps) {
	const { startProcessing, activeProjectRuns } = useJobState();
	const regenerateProfile = useRegenerateProfile();
	const { data } = useGenerateAdvanceAnalytics({
		projectId: selectedProject.id,
		dataSourceId,
	});
	const [jobId, setJobId] = React.useState<string | undefined>(undefined);
	const [showLoader, setShowLoader] = React.useState(true);
	const [minDisplayTimeElapsed, setMinDisplayTimeElapsed] =
		React.useState(false);

	useEffect(() => {
		if (
			data &&
			data.value &&
			data.value.projectRun &&
			data.value.projectRun.id
		) {
			setJobId(data.value.projectRun.id);
			startProcessing(
				data.value.projectRun.id,
				"/data-profiling",
				{
					projectId: selectedProject.id,
					dataSourceId,
					queryKey: ["advance-analytics", dataSourceId],
				},
				selectedProject.name,
				"Generate Advance Analytics"
			);
		}
	}, [data]);

	// Ensure minimum display time of 2 seconds for smooth animation
	useEffect(() => {
		const timer = setTimeout(() => {
			setMinDisplayTimeElapsed(true);
		}, 2000);

		return () => clearTimeout(timer);
	}, []);

	// Check if job has reached a terminal state
	const projectRun = jobId ? activeProjectRuns.get(jobId) : null;
	const isCompleted = projectRun?.runStatus === "Completed";
	const isCancelledOrFailed =
		projectRun?.runStatus === "Cancelled" ||
		projectRun?.runStatus === "Failed";

	// Hide loader when job is completed (after min display time) OR immediately
	// when cancelled/failed so the parent shows the appropriate empty state
	useEffect(() => {
		if (isCancelledOrFailed && showLoader) {
			setShowLoader(false);
			return;
		}

		if (minDisplayTimeElapsed && isCompleted && showLoader) {
			// Add a small delay to show 100% completion
			const timer = setTimeout(() => {
				setShowLoader(false);
			}, 500);

			return () => clearTimeout(timer);
		}
	}, [minDisplayTimeElapsed, isCompleted, isCancelledOrFailed, showLoader]);

	// Show loader if not yet ready to hide
	if (showLoader) {
		return <ProfilingLoader jobId={jobId} />;
	}

	// Show cancelled/failed state with retry
	if (isCancelledOrFailed) {
		return (
			<div className="flex flex-col items-center justify-center h-[50vh] text-center px-4">
				<XCircle className="h-12 w-12 text-muted-foreground mb-4" />
				<h2 className="text-lg font-semibold mb-2">
					{projectRun?.runStatus === "Cancelled"
						? "Profiling was cancelled"
						: "Profiling failed"}
				</h2>
				<p className="text-sm text-muted-foreground max-w-md mb-6">
					{projectRun?.runStatus === "Cancelled"
						? "The data profiling job was cancelled before it could complete."
						: "Something went wrong while generating the profile. Please try again."}
				</p>
				<Button
					onClick={() => {
						setJobId(undefined);
						setShowLoader(true);
						setMinDisplayTimeElapsed(false);
						regenerateProfile.mutate({
							projectId: selectedProject.id,
							dataSourceId,
							projectName: selectedProject.name,
						});
					}}
					variant="outline"
					disabled={regenerateProfile.isPending}
				>
					<RotateCcw className="h-4 w-4 mr-2" />
					Retry
				</Button>
			</div>
		);
	}

	return null;
}
