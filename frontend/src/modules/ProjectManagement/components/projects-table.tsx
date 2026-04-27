"use client";

import {
	Table,
	TableHeader,
	TableRow,
	TableHead,
	TableBody,
	TableCell,
} from "@/components/ui/table";
import {
	DashboardAddNewDataIcon,
	DashboardSortAscendingIcon,
	DashboardSortDescendingIcon,
} from "@/assets/icons";
import { useProjectsList } from "../hooks/use-projects-list";
import EditDropdownMenu from "./ui/edit-dropdown-menu";
import { Button } from "@/components/ui/button";
import { useAppSelector } from "@/hooks/use-store";
import { bindActionCreators } from "@reduxjs/toolkit";
import { store } from "@/store";
import { projectActions } from "../store/projectSlice";
import { useEffect, useState } from "react";
import { CustomPagination } from "@/components/ui/custom-pagination";
import useSortProjects from "../hooks/use-sort-projects";
import clsx from "clsx";
import { Project } from "@/models/api-responses";

export default function ProjectsTable() {
	const { isLoading } = useProjectsList();
	const { sortConfig, selectedProject } = useAppSelector(
		(state) => state.projects
	);
	const [highlightedProjectId, setHighlightedProjectId] = useState<
		string | undefined
	>(selectedProject?.id);
	const { setCreateModalOpen, setSortConfig, setSelectedProject } =
		bindActionCreators(projectActions, store.dispatch);
	const { filteredProjects, totalPages, currentPage, setCurrentPage } =
		useSortProjects();

	return (
		<div className="relative overflow-x-auto">
			<Table>
				<TableHeader>
					<TableRow>
						<TableHead className="w-[80px]">ID</TableHead>
						<TableHead>Projects</TableHead>
						<TableHead>Description</TableHead>
						<TableHead
							className="text-right cursor-pointer"
							role="button"
							tabIndex={0}
							aria-sort={
								sortConfig.key === "modifiedAt"
									? sortConfig.direction === "asc"
										? "ascending"
										: "descending"
									: "none"
							}
							onClick={() => setSortConfig({ key: "modifiedAt" })}
							onKeyDown={(e) => {
								if (e.key === "Enter" || e.key === " ") {
									e.preventDefault();
									setSortConfig({ key: "modifiedAt" });
								}
							}}
						>
							Last modified
							{sortConfig.key === "modifiedAt" ? (
								sortConfig.direction === "asc" ? (
									<DashboardSortAscendingIcon className="inline-block ml-1 h-4 w-4" />
								) : (
									<DashboardSortDescendingIcon className="inline-block ml-1 h-4 w-4" />
								)
							) : (
								<DashboardSortDescendingIcon className="inline-block ml-1 h-4 w-4 opacity-30" />
							)}
						</TableHead>
						<TableHead
							className="text-right cursor-pointer"
							role="button"
							tabIndex={0}
							aria-sort={
								sortConfig.key === "createdAt"
									? sortConfig.direction === "asc"
										? "ascending"
										: "descending"
									: "none"
							}
							onClick={() => setSortConfig({ key: "createdAt" })}
							onKeyDown={(e) => {
								if (e.key === "Enter" || e.key === " ") {
									e.preventDefault();
									setSortConfig({ key: "createdAt" });
								}
							}}
						>
							Created Date
							{sortConfig.key === "createdAt" ? (
								sortConfig.direction === "asc" ? (
									<DashboardSortAscendingIcon className="inline-block ml-1 h-4 w-4" />
								) : (
									<DashboardSortDescendingIcon className="inline-block ml-1 h-4 w-4" />
								)
							) : (
								<DashboardSortDescendingIcon className="inline-block ml-1 h-4 w-4 opacity-30" />
							)}
						</TableHead>
						<TableHead className="w-[100px]">Actions</TableHead>
					</TableRow>
				</TableHeader>
				<TableBody>
					{isLoading && (
						<TableRow>
							<TableCell colSpan={6} className="text-center py-8">
								<div className="flex flex-col items-center justify-center">
									<div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-primary"></div>
									<p className="mt-2 text-sm text-muted-foreground">
										Loading projects...
									</p>
								</div>
							</TableCell>
						</TableRow>
					)}
					{!isLoading &&
						filteredProjects?.map((project: Project) => (
							<TableRow
								key={project.id}
								className={clsx(
									highlightedProjectId === project.id &&
										"bg-primary/10 border-l-4 border-b-0 border-primary",
									"hover:bg-accent/80 cursor-pointer"
								)}
								onClick={() => setHighlightedProjectId(project.id)}
								onDoubleClick={() => setSelectedProject(project)}
							>
								<TableCell>{project.id.slice(0, 8)}</TableCell>
								<TableCell className="font-medium">
									{project.name?.length > 20
										? project.name.slice(0, 20) + "..."
										: project.name}
								</TableCell>
								<TableCell>
									{project.description?.length > 40
										? project.description.slice(0, 40) + "..."
										: project.description}
								</TableCell>

								<TableCell className="text-right">
									{new Date(project.modifiedAt).toLocaleDateString()}
								</TableCell>
								<TableCell className="text-right">
									{new Date(project.createdAt).toLocaleDateString()}
								</TableCell>
								<TableCell className="text-center">
									<EditDropdownMenu project={project} />
								</TableCell>
							</TableRow>
						))}
					{!isLoading && filteredProjects?.length === 0 && (
						<TableRow>
							<TableCell colSpan={6} className="text-center py-8">
								<div className="flex flex-col items-center justify-center">
									<p className="text-lg font-medium mb-2">No projects found</p>
									<p className="text-sm text-muted-foreground mb-4">
										Try adjusting your search or filter criteria, or create a
										new project.
									</p>
									<Button onClick={() => setCreateModalOpen(true)}>
										<DashboardAddNewDataIcon className="mr-2 h-4 w-4" />
										Create Project
									</Button>
								</div>
							</TableCell>
						</TableRow>
					)}
				</TableBody>
			</Table>
			{!isLoading && filteredProjects.length > 0 && totalPages > 1 && (
				<div className="mt-6 flex justify-center">
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
