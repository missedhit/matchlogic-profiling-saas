"use client";

import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
	DashboardAddNewDataIcon,
	DashboardStandardViewIcon,
	DashboardNumericViewIcon,
	GeneralSearchIcon,
} from "@/assets/icons";
import { useRouter } from "next/navigation";
import { projectActions } from "@/modules/ProjectManagement/store/projectSlice";
import { useAppDispatch, useAppSelector } from "@/hooks/use-store";
import ApiConnectionError from "@/modules/ProjectManagement/components/api-connection-error";
import ProjectStatsCard from "@/modules/ProjectManagement/components/project-stats-card";
import ProjectsTable from "./projects-table";
import ProjectsCard from "./projects-card";
import CreateProjectModal from "./ui/create-project-modal";
import { bindActionCreators } from "@reduxjs/toolkit";
import { store } from "@/store";
import { useProjectsList } from "../hooks/use-projects-list";

export default function ProjectManagementPage() {
	const dispatch = useAppDispatch();
	const { viewMode, searchTerm, createModalOpen } = useAppSelector(
		(state: { projects: any }) => state.projects
	);
	const { setViewMode, setSearchTerm, setCreateModalOpen } = bindActionCreators(
		projectActions,
		store.dispatch
	);

	const { status, error, apiConnected } = useAppSelector(
		(state: { projects: any }) => state.projects
	);
	const { data, isLoading } = useProjectsList();
	const projects = data?.value;

	return (
		<div className="space-y-8">
			{/* API Error Alert */}
			{!apiConnected && <ApiConnectionError />}

			{/* Stats Cards */}
			<ProjectStatsCard projects={projects ? projects : []} />

			{/* Projects View */}
			<div className="bg-table-background rounded-lg overflow-hidden shadow-sm">
				{/* Table Controls */}
				<div className="bg-accent p-5 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
					<div className="flex flex-wrap items-center gap-4">
						<div className="flex items-center gap-2 bg-white rounded-md p-1">
							<Button
								variant={viewMode === "list" ? "secondary" : "ghost"}
								size="sm"
								onClick={() => setViewMode("list")}
								className="gap-2"
							>
								<DashboardNumericViewIcon className="h-4 w-4" />
								Standard
							</Button>
							<Button
								variant={viewMode === "cards" ? "secondary" : "ghost"}
								size="sm"
								onClick={() => setViewMode("cards")}
								className="gap-2"
							>
								<DashboardStandardViewIcon className="h-4 w-4" />
								Card View
							</Button>
						</div>

						<div className="relative">
							<GeneralSearchIcon className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
							<Input
								type="text"
								placeholder="Search projects..."
						aria-label="Search projects"
								className="pl-10 pr-4 py-2 w-full md:w-auto"
								value={searchTerm}
								onChange={(e) => setSearchTerm(e.target.value)}
							/>
						</div>

						{/* Uncomment this section if you want to add status filtering in the future */}

						{/* <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="w-[180px] bg-white">
                <SelectValue placeholder="Filter by status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Statuses</SelectItem>
                <SelectItem value="active">Active</SelectItem>
                <SelectItem value="completed">Completed</SelectItem>
                <SelectItem value="archived">Archived</SelectItem>
              </SelectContent>
            </Select> */}
					</div>

					<Button
						className="bg-primary text-white hover:bg-primary/90"
						onClick={() => setCreateModalOpen(true)}
						requiredPermission="projects.create"
					>
						<DashboardAddNewDataIcon className="mr-2 h-4 w-4" />
						Create Project
					</Button>
				</div>

				{/* Content */}
				<div className="p-6">
					{viewMode === "list" ? <ProjectsTable /> : <ProjectsCard />}
				</div>
			</div>
			<CreateProjectModal
				isOpen={createModalOpen}
				setIsOpen={setCreateModalOpen}
			/>
		</div>
	);
}
