import { TabsContent } from "@/components/ui/tabs";
import { CharacterComposition } from "./overview/character-composition";
import { DataQualityScore } from "./overview/data-quality-score";
import { DataTypeDistribution } from "./overview/data-type-distribution";
import { DataUniqueness } from "./overview/data-uniqueness";
import { DataValidityChart } from "./overview/data-validity-chart";
import { FieldConsistency } from "./overview/field-consistency";
import KeyInsights from "./overview/key-insights";
import { OutlierDetection } from "./overview/outlier-detection";
import { PatternClassification } from "./overview/pattern-classification";
import { useAdvanceAnalyticsQuery } from "../hooks/use-advance-analytics-query";
import { useAppSelector } from "@/hooks/use-store";
import GenerateProfile from "./generate-profile";
import { ProfilingLoader } from "./profiling-loader";
import { Skeleton } from "@/components/ui/skeleton";
import { QueryErrorBoundary } from "@/components/common/QueryErrorBoundary";
import { RuntimeErrorBoundary } from "@/components/common/RuntimeErrorBoundary";

export default function ProfileOverview({
	dataSourceId,
}: {
	dataSourceId: string;
}) {
	const selectedProject = useAppSelector(
		(state) => state.projects.selectedProject
	);

	const { data: advanceAnalytics, isLoading, isError, error, refetch } = useAdvanceAnalyticsQuery({
		dataSourceId,
	});

	// Check if we're loading for the first time or reloading existing data
	const hasExistingProfile = advanceAnalytics && advanceAnalytics.value;
	const isInitialLoad = isLoading;

	return (
		<QueryErrorBoundary isError={isError} error={error} refetch={refetch} message="Failed to load profiling data">
			{/* Show skeletons when loading/reloading */}
			{isInitialLoad && (
				<TabsContent value="overview" className="space-y-6">
					<div className="grid grid-cols-1 md:grid-cols-3 gap-6">
						<Skeleton className="h-96 w-full" />
						<Skeleton className="h-96 w-full" />
						<Skeleton className="h-96 w-full" />
					</div>
					<div className="grid grid-cols-1 md:grid-cols-2 gap-6">
						<Skeleton className="h-96 w-full" />
						<Skeleton className="h-96 w-full" />
					</div>
					<Skeleton className="h-96 w-full" />
				</TabsContent>
			)}
			{!isInitialLoad && (
				<TabsContent value="overview" className="space-y-6">
					<>
						{advanceAnalytics &&
							!advanceAnalytics.value &&
							selectedProject &&
							dataSourceId && (
								<GenerateProfile
									selectedProject={selectedProject}
									dataSourceId={dataSourceId}
								/>
							)}
						{advanceAnalytics && advanceAnalytics.value && (
						<>
							{/* Top Row */}
							<div className="grid grid-cols-1 md:grid-cols-3 gap-6">
								<RuntimeErrorBoundary>
									<DataQualityScore
										datasetQuality={
											advanceAnalytics.value.profileResult.datasetQuality
										}
									/>
								</RuntimeErrorBoundary>

								<RuntimeErrorBoundary>
									<KeyInsights
										datasetQuality={
											advanceAnalytics.value.profileResult.datasetQuality
										}
									/>
								</RuntimeErrorBoundary>

								<RuntimeErrorBoundary>
									<FieldConsistency
										datasetQuality={
											advanceAnalytics.value.profileResult.datasetQuality
										}
										title="Field-Level Data Quality"
										description="Field-level quality scores contributing to overall data quality"
									/>
								</RuntimeErrorBoundary>
							</div>

							{/* Middle Row */}
							<div className="grid grid-cols-1 md:grid-cols-2 gap-6">
								<RuntimeErrorBoundary>
									<PatternClassification
										data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
									/>
								</RuntimeErrorBoundary>

								<RuntimeErrorBoundary>
									<DataTypeDistribution
										data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
									/>
								</RuntimeErrorBoundary>
							</div>

							{/* Data Validity Section */}
							<RuntimeErrorBoundary>
								<DataValidityChart
									data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
									totalRecords={advanceAnalytics.value.profileResult.totalRecords}
									dataSourceId={dataSourceId}
								/>
							</RuntimeErrorBoundary>

							{/* Statistical Summary and Character Composition */}
							<div className="grid grid-cols-1 md:grid-cols-2 gap-6">
								<RuntimeErrorBoundary>
									<OutlierDetection
										data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
									/>
								</RuntimeErrorBoundary>

								<RuntimeErrorBoundary>
									<CharacterComposition
										data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
									/>
								</RuntimeErrorBoundary>
							</div>

							{/* Data Uniqueness */}
							<RuntimeErrorBoundary>
								<DataUniqueness
									data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
									totalRecords={advanceAnalytics.value.profileResult.totalRecords}
								/>
							</RuntimeErrorBoundary>
						</>
						)}
					</>
				</TabsContent>
			)}
		</QueryErrorBoundary>
	);
}
