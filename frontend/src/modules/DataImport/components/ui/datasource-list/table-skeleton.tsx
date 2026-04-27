import { Skeleton } from "@/components/ui/skeleton";

export const TableSkeleton = () => {
	return (
		<div className="space-y-8">
			{/* Table skeleton */}
			<div className="rounded-md border">
				{/* Table header skeleton */}
				<div className="border-b p-4">
					<div className="grid grid-cols-7 gap-4">
						<Skeleton className="h-4 w-24" />
						<Skeleton className="h-4 w-24" />
						<Skeleton className="h-4 w-24" />
						<Skeleton className="h-4 w-24" />
						<Skeleton className="h-4 w-24" />
						<Skeleton className="h-4 w-24" />
						<Skeleton className="h-4 w-24" />
					</div>
				</div>

				{/* Table rows skeleton */}
				<div className="divide-y">
					{[...Array(5)].map((_, index) => (
						<div key={index} className="p-4">
							<div className="grid grid-cols-7 gap-4">
								<Skeleton className="h-4 w-32" />
								<Skeleton className="h-4 w-24" />
								<Skeleton className="h-4 w-20" />
								<Skeleton className="h-4 w-16" />
								<Skeleton className="h-4 w-24" />
								<Skeleton className="h-4 w-24" />
								<Skeleton className="h-4 w-16" />
							</div>
						</div>
					))}
				</div>
			</div>
		</div>
	);
};
