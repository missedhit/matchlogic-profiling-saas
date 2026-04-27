"use client";

import {
	Card,
	CardContent,
	CardDescription,
	CardFooter,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { DashboardEditIcon, DashboardDeleteIcon } from "@/assets/icons";
import DeleteProjectDialog from "./ui/delete-project-dialog";
import EditProjectModal from "./ui/edit-project-modal";
import { useState } from "react";
import { bindActionCreators } from "@reduxjs/toolkit";
import { projectActions } from "../store/projectSlice";
import { store } from "@/store";
import { Project } from "@/models/api-responses";

interface ProjectCardProps {
	project: Project;
	onSelect: (project: Project) => void;
}

export default function ProjectCard({ project, onSelect }: ProjectCardProps) {
	const [openDeleteDialog, setOpenDeleteDialog] = useState(false);
	const [openEditDialog, setOpenEditDialog] = useState(false);
	const { setSelectedProject } = bindActionCreators(
		projectActions,
		store.dispatch
	);
	return (
		<>
			<Card className="overflow-hidden">
				<CardHeader className="pb-2">
					<CardTitle title={project.name}>
						{project.name?.length > 20
							? project.name.slice(0, 20) + "..."
							: project.name}
					</CardTitle>
					<CardDescription>
						Created: {new Date(project.createdAt).toLocaleDateString()}
					</CardDescription>
				</CardHeader>
				<CardContent className="min-h-5">
					<div className="space-y-4">
						<p className="text-sm" title={project.description}>
							{project.description?.length > 40
								? project.description.slice(0, 40) + "..."
								: project.description}
						</p>
					</div>
				</CardContent>
				<CardFooter className="bg-muted/50 pt-2 flex flex-col gap-2">
					<Button
						onClick={() => setSelectedProject(project)}
						className="w-full"
					>
						Open Project
					</Button>
					<div className="flex w-full gap-2">
						<Button
							onClick={() => setOpenEditDialog(true)}
							variant="outline"
							className="flex-1"
						>
							<DashboardEditIcon className="mr-2 h-4 w-4" />
							Edit
						</Button>
						<Button
							onClick={() => setOpenDeleteDialog(true)}
							variant="outline"
							className="flex-1 text-red-600 hover:text-red-600"
						>
							<DashboardDeleteIcon className="mr-2 h-4 w-4" />
							Delete
						</Button>
					</div>
				</CardFooter>
			</Card>
			<DeleteProjectDialog
				isOpen={openDeleteDialog}
				setIsOpen={setOpenDeleteDialog}
				project={project}
			/>
			<EditProjectModal
				isOpen={openEditDialog}
				setIsOpen={setOpenEditDialog}
				project={project}
			/>
		</>
	);
}
