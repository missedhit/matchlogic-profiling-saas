import { Badge } from "@/components/ui/badge";
import {
	Carousel,
	CarouselContent,
	CarouselItem,
	CarouselNext,
	CarouselPrevious,
} from "@/components/ui/carousel";
import { useRouteGuard } from "@/providers/route-guard-provider";
import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function PageHeader() {
	const router = useRouter();
	const { dataSourceId, setDataSourceId, dataSources, currentProject } =
		useRouteGuard();

	const handleDataSourceChange = (id: string) => {
		setDataSourceId(id);
	};
	useEffect(() => {
		if (dataSourceId && currentProject?.id) {
			router.replace(
				`/data-profiling?projectId=${currentProject.id}&dataSourceId=${dataSourceId}`
			);
		}
	}, [dataSourceId, router, currentProject?.id]);

	return dataSources && dataSources.length > 0 ? (
		<Carousel
			opts={{ align: "start", dragFree: true }}
			className="w-full max-w-[450px]"
		>
			<div className="flex items-center gap-1.5">
				{dataSources.length > 3 && (
					<CarouselPrevious className="static translate-y-0 h-7 w-7 rounded-md shrink-0" />
				)}
				<div className="flex-1 min-w-0 overflow-hidden">
					<CarouselContent className="-ml-2">
						{dataSources.map((ds) => (
							<CarouselItem key={ds.id} className="pl-2 basis-auto">
								<Badge
									variant={dataSourceId === ds.id ? "dataSourceSelected" : "dataSource"}
									className="cursor-pointer whitespace-nowrap"
									role="button"
									tabIndex={0}
									onClick={() => handleDataSourceChange(ds.id)}
									onKeyDown={(e) => {
										if (e.key === "Enter" || e.key === " ") {
											e.preventDefault();
											handleDataSourceChange(ds.id);
										}
									}}
								>
									{ds.name}
								</Badge>
							</CarouselItem>
						))}
					</CarouselContent>
				</div>
				{dataSources.length > 3 && (
					<CarouselNext className="static translate-y-0 h-7 w-7 rounded-md shrink-0" />
				)}
			</div>
		</Carousel>
	) : null;
}
