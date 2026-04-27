import {
	JobStep,
	ProjectRun,
	useJobState,
} from "@/providers/job-state-provider";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import {
	Loader2,
	CheckCircle2,
	XCircle,
	Clock,
	Zap,
} from "lucide-react";
import {
	GeneralNotificationDefaultIcon,
	GeneralChevronDownIcon,
	GeneralChevronUpIcon,
} from "@/assets/icons";
import { useState, useRef, useCallback, useEffect } from "react";
import { Button } from "@/components/ui/button";
import {
	Dialog,
	DialogContent,
	DialogHeader,
	DialogTitle,
	DialogTrigger,
} from "@/components/ui/dialog";

// --- Status badge helper ---

function StatusBadge({ status }: { status: string }) {
	const s = status.toLowerCase();
	let className =
		"inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium border ";

	if (s === "completed" || s === "stepcompleted") {
		className += "bg-green-50 text-green-700 border-green-200";
	} else if (s === "failed" || s === "stepfailed") {
		className += "bg-red-50 text-red-700 border-red-200";
	} else if (s === "cancelled") {
		className += "bg-orange-50 text-orange-700 border-orange-200";
	} else if (s === "processing") {
		className += "bg-primary/10 text-primary border-primary/20";
	} else if (s === "queued") {
		className += "bg-gray-50 text-gray-600 border-gray-200";
	} else {
		className += "bg-gray-50 text-gray-600 border-gray-200";
	}

	return (
		<span className={className}>
			{s === "processing" && (
				<Loader2 className="h-3 w-3 animate-spin" />
			)}
			{(s === "completed" || s === "stepcompleted") && (
				<CheckCircle2 className="h-3 w-3" />
			)}
			{(s === "failed" || s === "stepfailed") && (
				<XCircle className="h-3 w-3" />
			)}
			{s === "cancelled" && <XCircle className="h-3 w-3" />}
			{s === "queued" && <Clock className="h-3 w-3" />}
			{status}
		</span>
	);
}

// --- Progress bar ---

function JobProgressBar({ value }: { value: number }) {
	return (
		<div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden">
			<div
				className="h-full rounded-full bg-primary transition-all duration-500"
				style={{ width: `${Math.min(value, 100)}%` }}
			/>
		</div>
	);
}

// --- Compute progress for a project run ---

export function computeProgress(projectRun: ProjectRun): number {
	if (projectRun.runStatus === "Completed") return 100;
	if (projectRun.runStatus === "Failed" || projectRun.runStatus === "Cancelled") return 0;
	if (!projectRun.jobs.length) return 0;

	const job = projectRun.jobs[0];
	const steps = Object.values(job.steps);
	if (!steps.length) return 0;

	const totalSteps = steps[0]?.totalSteps ?? steps.length;
	const completedSteps = steps.filter((s) => s.status === "Completed").length;
	return Math.round((completedSteps / totalSteps) * 100);
}

// --- Format timestamp ---

function formatTime(time: Date): string {
	if (!time) return "";
	return new Date(time).toLocaleString("en-GB", {
		day: "2-digit",
		month: "short",
		hour: "2-digit",
		minute: "2-digit",
	});
}

// --- Format elapsed duration ---

function formatElapsed(start: Date, end?: Date): string {
	if (!start) return "";
	const endTime = end ? new Date(end) : new Date();
	const seconds = Math.floor(
		(endTime.getTime() - new Date(start).getTime()) / 1000
	);
	if (seconds < 60) return `${seconds}s`;
	const minutes = Math.floor(seconds / 60);
	const secs = seconds % 60;
	if (minutes < 60) return `${minutes}m ${secs}s`;
	const hours = Math.floor(minutes / 60);
	return `${hours}h ${minutes % 60}m`;
}

// --- Active run card ---

function ActiveRunCard({ projectRun }: { projectRun: ProjectRun }) {
	const [expanded, setExpanded] = useState(true);
	const [cancelling, setCancelling] = useState(false);
	const { cancelJob } = useJobState();
	const progress = computeProgress(projectRun);
	const job = projectRun.jobs[0];
	const steps = job ? Object.values(job.steps) : [];
	const totalSteps = steps[0]?.totalSteps ?? steps.length;
	const completedSteps = steps.filter((s) => s.status === "Completed").length;
	const currentStep = steps.find((s) => s.status === "Processing");

	const handleCancel = async () => {
		setCancelling(true);
		try {
			await cancelJob(projectRun.id);
		} finally {
			setCancelling(false);
		}
	};

	return (
		<div className="rounded-lg border bg-white p-4 space-y-3">
			{/* Header */}
			<div className="flex items-start justify-between">
				<div className="space-y-1">
					<div className="flex items-center gap-2">
						<Loader2 className="h-4 w-4 animate-spin text-primary" />
						<span className="text-sm font-semibold">
							{projectRun.description || projectRun.projectName}
						</span>
					</div>
					<div className="flex items-center gap-3 text-xs text-muted-foreground">
						<span className="flex items-center gap-1">
							<Clock className="h-3 w-3" />
							Started {formatTime(projectRun.startTime)}
						</span>
					</div>
				</div>
				<div className="flex items-center gap-2">
					{/* Cancel button — disabled for now, will be enabled in a future release
					<Button
						variant="ghost"
						size="sm"
						className="h-7 px-2 text-xs text-red-600 hover:text-red-700 hover:bg-red-50"
						onClick={handleCancel}
						disabled={cancelling}
					>
						{cancelling ? <Loader2 className="h-3 w-3 animate-spin mr-1" /> : <XCircle className="h-3 w-3 mr-1" />}
						Cancel
					</Button>
					*/}
					<StatusBadge status={projectRun.runStatus} />
				</div>
			</div>

			{/* Elapsed time — prominent */}
			<div className="flex items-center gap-1.5 text-xs font-semibold text-primary">
				<Clock className="h-3.5 w-3.5" />
				{formatElapsed(projectRun.startTime)} elapsed
			</div>

			{/* Progress */}
			<div className="space-y-1.5">
				<div className="flex items-center justify-between text-xs">
					<span className="text-muted-foreground">
						{currentStep
							? `Step ${completedSteps + 1}: ${currentStep.stepName}`
							: `Step ${completedSteps} of ${totalSteps}`}
					</span>
					<span className="font-medium text-primary">{progress}%</span>
				</div>
				<JobProgressBar value={progress} />
			</div>

			{/* Steps (collapsible) */}
			{steps.length > 0 && (
				<>
					<button
						onClick={() => setExpanded(!expanded)}
						className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
					>
						{expanded ? (
							<GeneralChevronUpIcon className="h-3 w-3" />
						) : (
							<GeneralChevronDownIcon className="h-3 w-3" />
						)}
						{expanded ? "Hide" : "Show"} steps
					</button>
					{expanded && (
						<div className="space-y-2 pl-2 border-l-2 border-primary/20">
							{steps.map((step, idx) => (
								<div
									key={idx}
									className="flex items-center justify-between py-1"
								>
									<div className="flex items-center gap-2">
										{step.status === "Completed" && (
											<CheckCircle2 className="h-3.5 w-3.5 text-green-600" />
										)}
										{step.status === "Processing" && (
											<Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
										)}
										{step.status === "Failed" && (
											<XCircle className="h-3.5 w-3.5 text-red-500" />
										)}
										{!["Completed", "Processing", "Failed"].includes(
											step.status
										) && (
											<div className="h-3.5 w-3.5 rounded-full border-2 border-gray-300" />
										)}
										<span className="text-xs font-medium">{step.stepName}</span>
									</div>
									<div className="flex items-center gap-2">
										{step.startTime && (
											<span className="text-xs text-muted-foreground">
												{formatElapsed(step.startTime, step.endTime)}
											</span>
										)}
										{step.message && (
											<span className="text-xs text-muted-foreground max-w-[150px] truncate">
												{step.message}
											</span>
										)}
									</div>
								</div>
							))}
						</div>
					)}
				</>
			)}

			{/* Error */}
			{job?.error && (
				<div className="rounded-md bg-red-50 border border-red-200 px-3 py-2 text-xs text-red-700 break-words overflow-hidden">
					{job.error}
				</div>
			)}
		</div>
	);
}

// --- Completed run card ---

function CompletedRunCard({ projectRun }: { projectRun: ProjectRun }) {
	const [expanded, setExpanded] = useState(false);
	const isSuccess = projectRun.runStatus === "Completed";
	const job = projectRun.jobs[0];
	const steps = job ? Object.values(job.steps) : [];

	return (
		<div
			className={`rounded-lg border p-4 space-y-3 ${isSuccess ? "bg-green-50/50 border-green-200" : "bg-red-50/50 border-red-200"}`}
		>
			{/* Header */}
			<div className="flex items-start justify-between">
				<div className="space-y-1">
					<div className="flex items-center gap-2">
						{isSuccess ? (
							<CheckCircle2 className="h-4 w-4 text-green-600" />
						) : (
							<XCircle className="h-4 w-4 text-red-500" />
						)}
						<span className="text-sm font-semibold">
							{projectRun.projectName ? `${projectRun.projectName} - ` : ""}
							{projectRun.description}
						</span>
					</div>
					<div className="flex items-center gap-3 text-xs text-muted-foreground">
						<span>{formatTime(projectRun.startTime)}</span>
						{projectRun.endTime && (
							<>
								<span>→</span>
								<span>{formatTime(projectRun.endTime)}</span>
							</>
						)}
						<span className="font-medium">
							{formatElapsed(projectRun.startTime, projectRun.endTime)}
						</span>
					</div>
				</div>
				<StatusBadge status={projectRun.runStatus} />
			</div>

			{/* Steps summary chips */}
			{steps.length > 0 && (
				<>
					<button
						onClick={() => setExpanded(!expanded)}
						className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
					>
						{expanded ? (
							<GeneralChevronUpIcon className="h-3 w-3" />
						) : (
							<GeneralChevronDownIcon className="h-3 w-3" />
						)}
						{steps.length} step{steps.length !== 1 ? "s" : ""}
					</button>
					{expanded && (
						<div className="flex gap-1.5 flex-wrap">
							{steps.map((step, idx) => (
								<span
									key={idx}
									className={`inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium border ${
										step.status === "Completed"
											? "bg-green-50 text-green-700 border-green-200"
											: step.status === "Failed"
												? "bg-red-50 text-red-700 border-red-200"
												: "bg-gray-50 text-gray-600 border-gray-200"
									}`}
								>
									{step.status === "Completed" && (
										<CheckCircle2 className="h-3 w-3" />
									)}
									{step.status === "Failed" && <XCircle className="h-3 w-3" />}
									{step.stepName}
								</span>
							))}
						</div>
					)}
				</>
			)}

			{/* Error */}
			{job?.error && (
				<div className="rounded-md bg-red-50 border border-red-200 px-3 py-2 text-xs text-red-700 break-words overflow-hidden">
					{job.error}
				</div>
			)}
		</div>
	);
}

// --- Empty state ---

function EmptyState({ message }: { message: string }) {
	return (
		<div className="flex flex-col items-center justify-center py-12 text-center">
			<div className="h-10 w-10 rounded-full bg-gray-100 flex items-center justify-center mb-3">
				<Zap className="h-5 w-5 text-muted-foreground" />
			</div>
			<p className="text-sm text-muted-foreground">{message}</p>
		</div>
	);
}

// --- Main dialog ---

export const JobStatusDialog = () => {
	const { activeProjectRuns, isJobDialogOpen, setJobDialogOpen } =
		useJobState();
	const inProcessRun = [...activeProjectRuns.values()]
		.slice(-10)
		.filter(
			(run) => run.runStatus !== "Completed" && run.runStatus !== "Failed" && run.runStatus !== "Cancelled"
		)
		.reverse();
	const completedRun = [...activeProjectRuns.values()]
		.slice(-10)
		.filter(
			(run) => run.runStatus === "Completed" || run.runStatus === "Failed" || run.runStatus === "Cancelled"
		)
		.reverse();

	const hasActiveRuns = inProcessRun.length > 0;
	const defaultTab = hasActiveRuns ? "active" : "completed";

	const [tab, setTab] = useState(defaultTab);
	const [completedDotColor, setCompletedDotColor] = useState<"green" | "red" | null>(null);

	// Auto-switch tab when active state changes
	useEffect(() => {
		setTab(hasActiveRuns ? "active" : "completed");
	}, [hasActiveRuns]);

	// Track previous active IDs to detect which jobs just finished
	const prevActiveIds = useRef<Set<string>>(new Set(inProcessRun.map((r) => r.id)));
	useEffect(() => {
		const currentActiveIds = new Set(inProcessRun.map((r) => r.id));
		// Find runs that were active before but aren't now (just finished)
		const justFinished = [...prevActiveIds.current].filter((id) => !currentActiveIds.has(id));

		if (justFinished.length > 0) {
			const hasFailed = justFinished.some((id) => {
				const run = activeProjectRuns.get(id);
				return run && (run.runStatus === "Failed" || run.runStatus === "Cancelled");
			});
			setCompletedDotColor(hasFailed ? "red" : "green");
		}
		prevActiveIds.current = currentActiveIds;
	}, [inProcessRun, activeProjectRuns]);

	const handleOpenChange = useCallback(
		(isOpen: boolean) => {
			setJobDialogOpen(isOpen);
			if (isOpen) {
				setCompletedDotColor(null);
			}
		},
		[setJobDialogOpen]
	);

	return (
		<Dialog open={isJobDialogOpen} onOpenChange={handleOpenChange}>
			<DialogTrigger asChild>
				<Button variant="ghost" size="icon" className="relative h-8 w-8">
					<GeneralNotificationDefaultIcon className="h-5 w-5" />
					{hasActiveRuns && (
						<span className="absolute top-1.5 right-1.5 inline-flex h-2 w-2 rounded-full bg-primary animate-pulse" />
					)}
					{!hasActiveRuns && completedDotColor && (
						<span className={`absolute top-1.5 right-1.5 inline-flex h-2 w-2 rounded-full ${completedDotColor === "red" ? "bg-red-500" : "bg-green-500"}`} />
					)}
				</Button>
			</DialogTrigger>
			<DialogContent className="sm:max-w-lg p-0 gap-0">
				<DialogHeader className="px-6 pt-6 pb-4">
					<DialogTitle className="text-lg">Job Status</DialogTitle>
					<p className="text-sm text-muted-foreground">
						Monitor your running and completed project jobs
					</p>
				</DialogHeader>
				<Separator />
				<div className="px-6 pt-4 pb-6">
					<Tabs value={tab} onValueChange={setTab}>
						<TabsList className="w-full">
							<TabsTrigger value="active" className="flex-1 gap-1.5">
								{hasActiveRuns && <Loader2 className="h-3 w-3 animate-spin" />}
								Active
								{inProcessRun.length > 0 && (
									<span className="ml-1 inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-primary/10 px-1.5 text-xs font-medium text-primary">
										{inProcessRun.length}
									</span>
								)}
							</TabsTrigger>
							<TabsTrigger value="completed" className="flex-1 gap-1.5">
								Completed
								{completedRun.length > 0 && (
									<span className="ml-1 inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-gray-100 px-1.5 text-xs font-medium text-gray-600">
										{completedRun.length}
									</span>
								)}
							</TabsTrigger>
						</TabsList>
						<div className="max-h-[400px] overflow-y-auto overflow-x-hidden mt-4">
							<TabsContent value="active" className="mt-0 space-y-3">
								{inProcessRun.length > 0 ? (
									inProcessRun.map((projectRun) => (
										<ActiveRunCard
											key={projectRun.id}
											projectRun={projectRun}
										/>
									))
								) : (
									<EmptyState message="No active jobs running" />
								)}
							</TabsContent>
							<TabsContent value="completed" className="mt-0 space-y-3">
								{completedRun.length > 0 ? (
									completedRun.map((projectRun) => (
										<CompletedRunCard
											key={projectRun.id}
											projectRun={projectRun}
										/>
									))
								) : (
									<EmptyState message="No completed jobs yet" />
								)}
							</TabsContent>
						</div>
					</Tabs>
				</div>
			</DialogContent>
		</Dialog>
	);
};
