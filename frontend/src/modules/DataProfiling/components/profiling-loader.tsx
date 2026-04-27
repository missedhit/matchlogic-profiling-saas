"use client";

import { useEffect, useState } from "react";
import { Lightbulb } from "lucide-react";
import { Progress } from "@/components/ui/progress";
import { useJobState } from "@/providers/job-state-provider";

// Mock data rows for the scanning animation
const generateMockDataRows = () => {
  const fieldNames = [
    "customer_id",
    "email_address",
    "phone_number",
    "postal_code",
    "transaction_date",
    "product_name",
    "quantity",
    "price",
    "category",
    "status",
    "created_at",
    "updated_at",
  ];

  return Array.from({ length: 20 }, (_, i) => ({
    id: i,
    field: fieldNames[i % fieldNames.length],
    value: generateMockValue(i),
  }));
};

const generateMockValue = (seed: number) => {
  const types = [
    "ABC123",
    "user@example.com",
    "+1-555-0100",
    "12345",
    "2024-01-15",
    "Product Name",
    "42",
    "$99.99",
    "Category A",
    "Active",
  ];
  return types[seed % types.length];
};

interface ProfilingLoaderProps {
  jobId?: string;
}

export function ProfilingLoader({ jobId }: ProfilingLoaderProps) {
  const [mockData] = useState(generateMockDataRows);
  const [fallbackProgress, setFallbackProgress] = useState(0);
  const { activeProjectRuns } = useJobState();

  // Get current job status from context
  const projectRun = jobId ? activeProjectRuns.get(jobId) : null;
  const currentJob = projectRun?.jobs?.[0];

  // Get current step from job
  const steps = currentJob?.steps ? Object.values(currentJob.steps) : [];
  const currentStep = steps.find(step => step.status === 'Processing') || steps[steps.length - 1];

  // Calculate overall progress - use API progress if available, otherwise use fallback
  const apiProgress = currentJob?.progressPercentage || 0;

  const isTerminal =
    projectRun?.runStatus === "Completed" ||
    projectRun?.runStatus === "Cancelled" ||
    projectRun?.runStatus === "Failed";

  // Fallback progress animation when API progress is not available
  useEffect(() => {
    if (isTerminal) {
      // Stop animation on any terminal state; completed jumps to 100, others stay put
      if (projectRun?.runStatus === "Completed") {
        setFallbackProgress(100);
      }
      return;
    }

    if (apiProgress === 0) {
      const interval = setInterval(() => {
        setFallbackProgress((prev) => {
          const next = prev + 1;
          return next >= 90 ? 90 : next; // Stop at 90% to wait for real progress
        });
      }, 150);

      return () => clearInterval(interval);
    } else if (apiProgress > 0) {
      setFallbackProgress(0); // Reset fallback when real progress arrives
    }
  }, [apiProgress, projectRun?.runStatus, isTerminal]);

  const progress = Math.min(100, apiProgress > 0 ? apiProgress : fallbackProgress);

  // If job was cancelled or failed, return null so the parent component
  // (generate-profile / profile-page-tabs) handles the empty/error state
  if (
    projectRun?.runStatus === "Cancelled" ||
    projectRun?.runStatus === "Failed"
  ) {
    return null;
  }

  // Get current stage title and description
  const currentStageTitle = currentStep?.stepName || "Initializing Analysis";
  const currentStageDescription = currentStep?.message || "Preparing data structures...";

  return (
    <div className="relative w-full min-h-[500px] h-[calc(100vh-12rem)] flex items-center justify-center overflow-hidden bg-gradient-to-br from-iris-mist via-white to-primary/5 p-4 sm:p-6">
      {/* Background animated data rows */}
      <div className="absolute inset-0 overflow-hidden opacity-20">
        <div className="animate-scroll-up space-y-2 py-4">
          {mockData.map((row) => (
            <div
              key={row.id}
              className="flex items-center gap-4 px-8 py-2 text-sm font-mono"
            >
              <span className="text-primary font-semibold w-40">
                {row.field}
              </span>
              <span className="text-primary/60">:</span>
              <span className="text-primary/80">{row.value}</span>
            </div>
          ))}
          {/* Duplicate for seamless loop */}
          {mockData.map((row) => (
            <div
              key={`duplicate-${row.id}`}
              className="flex items-center gap-4 px-8 py-2 text-sm font-mono"
            >
              <span className="text-primary font-semibold w-40">
                {row.field}
              </span>
              <span className="text-primary/60">:</span>
              <span className="text-primary/80">{row.value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Scanning line effect */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="scan-line" />
      </div>

      {/* Floating particles */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        {Array.from({ length: 15 }).map((_, i) => (
          <div
            key={i}
            className="particle"
            style={{
              left: `${(i * 7) % 100}%`,
              animationDelay: `${i * 0.3}s`,
              animationDuration: `${3 + (i % 3)}s`,
            }}
          />
        ))}
      </div>

      {/* Main content card */}
      <div className="relative z-10 w-full max-w-2xl mx-auto">
        <div className="bg-white/90 backdrop-blur-lg rounded-2xl shadow-2xl border border-primary/10 p-6 sm:p-8">
          {/* Header */}
          <div className="text-center mb-6 sm:mb-8">
            <div className="inline-flex items-center justify-center w-16 h-16 sm:w-20 sm:h-20 rounded-full bg-gradient-to-br from-primary to-primary mb-4 relative">
              <div className="absolute inset-0 rounded-full bg-primary animate-ping opacity-20" />
              <svg
                className="w-8 h-8 sm:w-10 sm:h-10 text-white animate-pulse"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
            </div>
            <h2 className="text-2xl sm:text-3xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-primary to-primary/80 mb-2">
              Generating Data Profile
            </h2>
            <p className="text-primary text-xs sm:text-sm">
              Analyzing your data to provide comprehensive insights
            </p>
          </div>

          {/* Current stage */}
          <div className="mb-6 p-4 rounded-lg bg-gradient-to-r from-primary/5 to-primary/10 border border-primary/20">
            <div className="flex items-start gap-3">
              <div className="flex-shrink-0 mt-2">
                <div className="w-2 h-2 rounded-full bg-primary animate-pulse" />
              </div>
              <div className="flex-1 min-w-0">
                <h3 className="text-base sm:text-lg font-semibold text-primary mb-1 leading-tight">
                  {currentStageTitle}
                </h3>
                <p className="text-xs sm:text-sm text-primary break-words">
                  {currentStageDescription}
                </p>
              </div>
            </div>
          </div>

          {/* Progress bar */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs sm:text-sm font-medium text-primary">
                Progress
              </span>
              <span className="text-xs sm:text-sm font-semibold text-primary">
                {Math.round(progress)}%
              </span>
            </div>
            <Progress
              value={progress}
              className="h-3 bg-primary/10"
              indicatorClassName="bg-gradient-to-r from-primary to-primary"
            />
          </div>
        </div>

        {/* Fun fact or tip */}
        <div className="mt-4 sm:mt-6 text-center px-4">
          <p className="text-xs sm:text-sm text-primary italic">
            <Lightbulb aria-hidden="true" className="inline h-4 w-4 mr-1" />Tip: Data profiling helps identify quality issues early in your
            analysis pipeline
          </p>
        </div>
      </div>

    </div>
  );
}
