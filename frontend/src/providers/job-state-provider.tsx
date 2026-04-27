import {
	useState,
	useEffect,
	useCallback,
	useRef,
	createContext,
	useContext,
	ReactNode,
} from "react";
import { useSearchParams } from "next/navigation";
import { apiFetch } from "@/utils/apiFetch";
import { store } from "@/store";
import { useAppDispatch, useAppSelector } from "../hooks/use-store";
import { queryClient } from "@/store/query-client";
import { useRouteGuard } from "./route-guard-provider";

// --- Type Definitions ---
// Based on the interfaces from the original Angular service

export interface ProjectRun {
	id: string;
	description: string;
	runStatus: string;
	startTime: Date; // Using ISO string for serialization
	endTime?: Date;
	statusUrl: string;
	jobs: JobStatus[];
	projectName: string;
	navigationRoute: string;
	queryParams: any; // Kept as 'any' to match original flexibility
}
export interface JobStatus {
	jobId: string;
	processedRecords?: number;
	totalRecords?: number;
	status:
		| "Processing"
		| "Completed"
		| "Failed"
		| "Queued"
		| "StepCompleted"
		| "StepFailed";
	currentStep?: JobStep;
	steps: Record<string, JobStep>;
	error?: string;
	startTime: Date;
	endTime?: Date;
	progressPercentage: number;
	description: string; // User-friendly description of the job
	statusUrl?: string;
	metadata?: Record<string, unknown>;
}

export interface JobStep {
	stepName: string;
	stepNumber: number;
	totalSteps: number;
	status: "Processing" | "Completed" | "Failed";
	processedItems?: number;
	totalItems?: number;
	startTime?: Date;
	endTime?: Date;
	error?: string;
	progressPercentage: number;
	message: string;
}

// Type for the context value
interface JobStateContextType {
	activeProjectRuns: Map<string, ProjectRun>;
	isLoading: boolean;
	error: string | null;
	isJobDialogOpen: boolean;
	setJobDialogOpen: (open: boolean) => void;
	startProcessing: (
		jobId: string,
		navigateTo: string,
		queryParam: any,
		projectName: string,
		description?: string,
		afterComplete?: () => void
	) => Promise<void>;
	cancelJob: (runId: string) => Promise<boolean>;
}

// --- Context Creation ---

// Create the context with a default value that matches the defined type
const JobStateContext = createContext<JobStateContextType>({
	activeProjectRuns: new Map(),
	isLoading: false,
	error: null,
	isJobDialogOpen: false,
	setJobDialogOpen: () => {},
	startProcessing: async () => {}, // Default no-op async function
	cancelJob: async () => false,
});

const storageKey = "job_states-1.0.6";
const baseUrl = "/run/status";
const pollInterval = 10000;

// Custom hook to easily consume the job state context
export const useJobState = () => useContext(JobStateContext);

// --- Provider Component ---

// Type the props for the provider component
interface JobStateProviderProps {
	children: ReactNode;
}

export const JobStateProvider = ({ children }: JobStateProviderProps) => {
	const { setImporting } = useRouteGuard();
	const [stateInitialized, setStateInitialized] = useState<boolean>(false);
	const [activeProjectRuns, setActiveProjectRuns] = useState<
		Map<string, ProjectRun>
	>(new Map());

	const [isLoading, setIsLoading] = useState<boolean>(false);
	const [error, setError] = useState<string | null>(null);
	const [isJobDialogOpen, setJobDialogOpen] = useState<boolean>(false);

	// CRIT-2: intervalIds lives inside the component so re-mounts don't orphan old intervals.
	const intervalIds = useRef<Map<string, NodeJS.Timeout>>(new Map());
	// Track consecutive null/missing status responses per run to avoid
	// immediately killing a job whose status record hasn't been created yet.
	const nullPollCount = useRef<Map<string, number>>(new Map());

	// CRIT-2: Clear all intervals on unmount.
	useEffect(() => {
		return () => {
			intervalIds.current.forEach((id) => clearInterval(id));
			intervalIds.current.clear();
		};
	}, []);

	// Load initial state from local storage on component mount
	useEffect(() => {
		try {
			const savedState = localStorage.getItem(storageKey);
			if (savedState && !stateInitialized) {
				const activeRuns: [string, ProjectRun][] = JSON.parse(savedState);
				const hydratedRuns: [string, ProjectRun][] = activeRuns.map(([id, run]) => [
					id,
					{
						...run,
						startTime: new Date(run.startTime),
						endTime: run.endTime ? new Date(run.endTime) : undefined,
					},
				]);
				setActiveProjectRuns(new Map(hydratedRuns));
			}
			setStateInitialized(true);
		} catch (err) {
			console.error("Failed to load job state from storage:", err);
		}
	}, []);

	useEffect(() => {
		if (!stateInitialized) return;
		try {
			const stateToSave = [...activeProjectRuns];
			localStorage.setItem(storageKey, JSON.stringify(stateToSave));
		} catch (err) {
			console.error("Failed to save job state to storage:", err);
		}
	}, [activeProjectRuns]);

	const updateProjectRunStatus = useCallback(
		(runId: string, jobStatuses: any[], runStatus: string): void => {
			setActiveProjectRuns((prev) => {
				const currentProjectRun = prev.get(runId);
				if (!currentProjectRun) return prev;

				// Don't overwrite terminal states (e.g. from an in-flight poll after cancel)
				if (
					currentProjectRun.runStatus === "Cancelled" ||
					currentProjectRun.runStatus === "Completed" ||
					currentProjectRun.runStatus === "Failed"
				) {
					return prev;
				}

				const updatedJobs = jobStatuses.map((apiJob) =>
					convertApiJobToJobStatus(apiJob)
				);

				return new Map(prev).set(runId, {
					...currentProjectRun,
					runStatus,
					jobs: updatedJobs,
				});
			});
		},
		[]
	);

	const completeProjectRun = useCallback(
		(
			runId: string,
			jobStatuses: any[],
			runStatus: string,
			hasFailedJobs: boolean
		): void => {
			clearInterval(intervalIds.current.get(runId));
			intervalIds.current.delete(runId);

			const projectRun = activeProjectRuns.get(runId);
			if (!projectRun) return;

			// Don't overwrite a locally-cancelled run with a stale poll response
			if (projectRun.runStatus === "Cancelled") return;

			// Convert API job statuses to our JobStatus format
			const completedJobs = jobStatuses.map((apiJob) =>
				convertApiJobToJobStatus(apiJob)
			);

			const completedProjectRun: ProjectRun = {
				...projectRun,
				runStatus: hasFailedJobs ? "Failed" : runStatus,
				endTime: new Date(),
				jobs: completedJobs,
			};

			// Update state first
			setActiveProjectRuns((prev) =>
				new Map(prev).set(runId, completedProjectRun)
			);

			const { queryKey } = completedProjectRun.queryParams;

			if (queryKey) {
				if (queryKey[0] === "data-source") {
					setImporting(false);
					queryClient.invalidateQueries({
						queryKey: ["license-status"],
						refetchType: "all",
					});
				}
				queryClient.invalidateQueries({
					queryKey,
				});
			}
		},
		[activeProjectRuns, setImporting]
	);

	const startProcessing = async (
		jobId: string,
		navigateTo: string,
		queryParam: any,
		projectName: string,
		description: string = "Processing Job"
	): Promise<void> => {
		setIsLoading(true);
		setError(null);
		const runId = activeProjectRuns.get(jobId);
		if (runId) {
			return;
		}

		const statusUrl = `${baseUrl}/${jobId}`;

		try {
			// This would be your API call to initiate the job on the backend

			const newProjectRun: ProjectRun = {
				id: jobId,
				description,
				runStatus: "Processing",
				navigationRoute: navigateTo,
				queryParams: queryParam,
				projectName,
				startTime: new Date(),
				statusUrl,
				jobs: [],
			};

			setActiveProjectRuns((prev) => new Map(prev).set(jobId, newProjectRun));
		} catch (err) {
			setError("Failed to start processing");
			console.error(err);
		} finally {
			setIsLoading(false);
		}
	};

	const cancelJob = useCallback(
		async (runId: string): Promise<boolean> => {
			// Stop polling IMMEDIATELY to prevent race conditions
			clearInterval(intervalIds.current.get(runId));
			intervalIds.current.delete(runId);

			// Optimistically update local state IMMEDIATELY
			setActiveProjectRuns((prev) => {
				const updated = new Map(prev);
				const run = updated.get(runId);
				if (run) {
					updated.set(runId, {
						...run,
						runStatus: "Cancelled",
						endTime: new Date(),
					});
				}
				return updated;
			});

			// Best-effort API call to cancel on the backend too
			try {
				await apiFetch(
					store.dispatch,
					store.getState,
					`/run/cancel/${runId}`,
					{
						method: "POST",
						headers: {
							"X-Skip-Loader": "true",
							"X-Skip-Toast": "true",
						},
					}
				);
			} catch (err) {
				console.error("Failed to cancel job on backend:", err);
			}

			return true;
		},
		[]
	);

	// CRIT-1: pollStatus is recreated each render because it closes over activeProjectRuns.
	// We store the latest version in a ref so the stable setInterval callback always
	// calls the most-recent copy without capturing a stale closure.
	const pollStatus = useCallback(
		async (run: ProjectRun): Promise<void> => {
			// Skip polling if this run was already cancelled/completed locally
			if (!intervalIds.current.has(run.id)) return;

			try {
				const response = await apiFetch(
					store.dispatch,
					store.getState,
					run.statusUrl,
					{
						method: "GET",
						headers: {
							"X-Skip-Loader": "true",
						},
					}
				);
				if (!response?.value) {
					// Status not available yet — the backend may not have created the
					// record yet (race between POST returning the run ID and the
					// background service registering the status). Allow up to 6
					// consecutive null polls (~60 s) before giving up.
					const count = (nullPollCount.current.get(run.id) || 0) + 1;
					nullPollCount.current.set(run.id, count);
					if (count >= 6) {
						nullPollCount.current.delete(run.id);
						completeProjectRun(run.id, [], "Failed", true);
					}
					return;
				}
				// Got a valid response — reset the null-poll counter
				nullPollCount.current.delete(run.id);
				const responseData = response.value;
				// Assuming the API response has a 'value' field like the original service
				const jobStatuses: JobStatus[] = responseData.jobStatuses || [
					responseData,
				];
				// Only use responseData.runStatus for terminal detection.
			// responseData.status may reflect a step-level status ("Completed" for a
			// finished sub-step while the overall run is still in progress). Falling
			// back to it caused premature completion on large imports.
			// Normalize: backend sends "InProgress"/"NotStarted", frontend expects "Processing".
			const rawRunStatus = responseData.runStatus ?? "Processing";
			const runStatus =
				rawRunStatus === "InProgress" || rawRunStatus === "NotStarted"
					? "Processing"
					: rawRunStatus;
				const hasFailedJobs = jobStatuses.some(
					(job) =>
						job.status === "Failed" ||
						Object.values(job.steps || {}).some(
							(step: any) => step.status === "Failed"
						)
				);

				// runStatus is the authoritative run-level status from the backend
				// (already normalized above). Trust it for terminal detection.
				const isRunTerminal =
					runStatus === "Completed" ||
					runStatus === "Failed" ||
					runStatus === "Cancelled";

				if (isRunTerminal) {
					// Backend says the run is done — complete it
					completeProjectRun(run.id, jobStatuses, runStatus, hasFailedJobs);
				} else if (hasFailedJobs) {
					// All jobs report failure but runStatus hasn't caught up yet
					const allJobsDone = jobStatuses.every(
						(job) =>
							job.status === "Completed" ||
							job.status === "Failed" ||
							job.status === "StepFailed"
					);
					if (allJobsDone) {
						completeProjectRun(run.id, jobStatuses, "Failed", true);
					} else {
						updateProjectRunStatus(run.id, jobStatuses, runStatus);
					}
				} else {
					// Run still in progress
					updateProjectRunStatus(run.id, jobStatuses, runStatus);
				}
			} catch (err) {
				console.error(`Failed to poll job status for ${run.id}:`, err);
			}
		},
		[completeProjectRun, updateProjectRunStatus]
	);

	// CRIT-1: Keep the ref pointing at the latest pollStatus so the interval
	// callback never holds a stale closure over activeProjectRuns.
	const pollStatusRef = useRef(pollStatus);
	useEffect(() => {
		pollStatusRef.current = pollStatus;
	}, [pollStatus]);

	// Effect for polling active jobs — immediate first poll, then every pollInterval
	useEffect(() => {
		activeProjectRuns.forEach((run: ProjectRun) => {
			if (run.runStatus === "Processing" && !intervalIds.current.has(run.id)) {
				// Fire immediately so steps appear without waiting a full interval
				pollStatusRef.current(run);
				const intervalId = setInterval(
					() => pollStatusRef.current(run),
					pollInterval
				);
				intervalIds.current.set(run.id, intervalId);
			}
		});
	}, [activeProjectRuns]);

	const value: JobStateContextType = {
		activeProjectRuns,
		isLoading,
		error,
		isJobDialogOpen,
		setJobDialogOpen,
		startProcessing,
		cancelJob,
	};

	return (
		<JobStateContext.Provider value={value}>
			{children}
		</JobStateContext.Provider>
	);
};

function convertApiJobToJobStatus(apiJob: any): JobStatus {
	const steps: Record<string, JobStep> = {};

	if (apiJob.steps) {
		Object.entries(apiJob.steps).forEach(([key, step]: [string, any]) => {
			steps[key] = {
				stepName: step.stepName,
				stepNumber: step.stepNumber,
				totalSteps: step.totalSteps,
				status: step.status,
				processedItems: step.processedItems,
				totalItems: step.totalItems,
				startTime: step.startTime ? new Date(step.startTime) : undefined,
				endTime: step.endTime ? new Date(step.endTime) : undefined,
				error: step.error,
				progressPercentage:
					step.totalItems > 0
						? Math.min(100, (step.processedItems / step.totalItems) * 100)
						: 0,
				message: step.message || "",
			};
		});
	}

	return {
		jobId: apiJob.jobId || apiJob.id,
		processedRecords: apiJob.processedRecords || 0,
		totalRecords: apiJob.totalRecords || 0,
		status: apiJob.status,
		steps: steps,
		error: apiJob.error,
		startTime: new Date(apiJob.startTime),
		endTime: apiJob.endTime ? new Date(apiJob.endTime) : undefined,
		progressPercentage: calculateJobProgress(steps),
		description: apiJob.dataSourceName || "Processing Job",
		statusUrl: apiJob.statusUrl,
		metadata: apiJob.metadata || {},
	};
}

function calculateJobProgress(steps: Record<string, JobStep>): number {
	const stepArray = Object.values(steps);
	if (stepArray.length === 0) return 0;

	const totalProgress = stepArray.reduce(
		(sum, step) => sum + step.progressPercentage,
		0
	);
	return Math.min(100, totalProgress / stepArray.length);
}
