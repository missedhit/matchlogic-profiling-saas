import ProjectCard from "./project-card";
import { useProjectsList } from "../hooks/use-projects-list";
import { Button } from "@/components/ui/button";
import { DashboardAddNewDataIcon } from "@/assets/icons";
import { bindActionCreators } from "@reduxjs/toolkit";
import { projectActions } from "../store/projectSlice";
import { store } from "@/store";
import useSortProjects from "../hooks/use-sort-projects";
import { CustomPagination } from "@/components/ui/custom-pagination";
import { Project } from "@/models/api-responses";

export default function ProjectsCard() {
	const { data, isLoading } = useProjectsList();
	const projects = data?.value;
	const { setCreateModalOpen } = bindActionCreators(
		projectActions,
		store.dispatch
	);
	const { filteredProjects, totalPages, currentPage, setCurrentPage } =
		useSortProjects(9);
	return (
		<div className="space-y-6">
			<div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
				{isLoading ? (
					<div className="col-span-full flex h-[400px] items-center justify-center">
						<div className="flex flex-col items-center">
							<div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-primary"></div>
							<p className="mt-4 text-sm text-muted-foreground">
								Loading projects...
							</p>
						</div>
					</div>
				) : projects?.length === 0 ? (
					<div className="col-span-full flex h-[400px] items-center justify-center rounded-md border border-dashed">
						<div className="text-center">
							<h3 className="text-lg font-medium">No projects found</h3>
							<p className="text-sm text-muted-foreground mb-4">
								Try adjusting your search or filter criteria.
							</p>
							<Button onClick={() => setCreateModalOpen(true)}>
								<DashboardAddNewDataIcon className="mr-2 h-4 w-4" />
								Create Project
							</Button>
						</div>
					</div>
				) : (
					filteredProjects?.map((project: Project) => (
						<ProjectCard
							key={project.id}
							project={project}
							onSelect={() => {}}
							// onEdit={() => setOpenEditDialog(true)}
							// onDelete={() => setOpenDeleteDialog(true)}
						/>
					))
				)}
			</div>
			{!isLoading && filteredProjects.length > 0 && totalPages > 1 && (
				<div className="flex justify-center">
					<CustomPagination
						totalPages={totalPages}
						currentPage={currentPage}
						onPageChange={setCurrentPage}
					/>
				</div>
			)}
		</div>
	);
}
