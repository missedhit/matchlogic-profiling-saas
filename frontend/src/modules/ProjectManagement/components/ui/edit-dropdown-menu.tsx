import {
	Popover,
	PopoverContent,
	PopoverTrigger,
} from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { Project } from "@/models/api-responses";
import { projectActions } from "@/modules/ProjectManagement/store/projectSlice";
import { store } from "@/store";
import { bindActionCreators } from "@reduxjs/toolkit";
import { MoreVertical } from "lucide-react";
import { DashboardEditIcon, DashboardDeleteIcon } from "@/assets/icons";
import { useState } from "react";
import DeleteProjectDialog from "./delete-project-dialog";
import EditProjectModal from "./edit-project-modal";

export default function EditDropdownMenu({ project }: { project: Project }) {
	const [openDeleteDialog, setOpenDeleteDialog] = useState(false);
	const [openEditDialog, setOpenEditDialog] = useState(false);
	const { setSelectedProject } = bindActionCreators(
		projectActions,
		store.dispatch
	);

	return (
		<>
			<Popover>
				<PopoverTrigger asChild>
					<Button
						variant="outline"
						size="icon"
						aria-label="Project options"
						className="h-8 w-8"
					>
						<MoreVertical className="h-4 w-4" />
					</Button>
				</PopoverTrigger>
				<PopoverContent className="w-52 p-0" align="end">
					<Button
						variant="ghost"
						onClick={() => setSelectedProject(project)}
						className="flex w-full items-center justify-start px-4 py-2 h-auto text-sm font-normal text-gray-700"
						role="menuitem"
					>
						Open Project
					</Button>

					<hr className="my-1 border-gray-200" />
					<Button
						variant="ghost"
						onClick={() => setOpenEditDialog(true)}
						className="flex w-full items-center justify-start px-4 py-2 h-auto text-sm font-normal text-gray-700"
						role="menuitem"
						requiredPermission="projects.update"
					>
						<DashboardEditIcon className="mr-2 h-4 w-4" />
						Edit
					</Button>
					<Button
						variant="ghost"
						onClick={() => setOpenDeleteDialog(true)}
						className="flex w-full items-center justify-start px-4 py-2 h-auto text-sm font-normal text-red-600 hover:text-red-600"
						role="menuitem"
						requiredPermission="projects.delete"
					>
						<DashboardDeleteIcon className="mr-2 h-4 w-4" />
						Delete
					</Button>
				</PopoverContent>
			</Popover>
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
