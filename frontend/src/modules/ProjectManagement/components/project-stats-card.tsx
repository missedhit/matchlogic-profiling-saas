import { Card, CardContent } from "@/components/ui/card";
import { Project } from "@/models/api-responses";

export default function ProjectStatsCard({
	projects,
}: {
	projects: Project[];
}) {
	return (
		<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
			<Card className="shadow-sm">
				<CardContent className="p-6">
					<div className="flex items-center gap-2 mb-2">
						<div className="w-3 h-3 rounded-full bg-primary"></div>
						<p className="text-sm font-medium">Number of projects</p>
					</div>
					<div className="flex items-baseline gap-2">
						<h2 className="text-4xl font-bold">{projects?.length}</h2>
						<p className="text-sm text-muted-foreground">Projects</p>
					</div>
				</CardContent>
			</Card>
		</div>
	);
}
