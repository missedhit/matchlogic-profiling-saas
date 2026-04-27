import {
	AlertDialog,
	AlertDialogContent,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
	AlertDialogDescription,
	AlertDialogCancel,
	AlertDialogAction,
} from "@/components/ui/alert-dialog";
import { Project } from "@/models/api-responses";
import { useDeleteProject } from "@/modules/ProjectManagement/hooks/use-delete-project";
import { usePermission } from "@/hooks/use-permission";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";

export default function DeleteProjectDialog({
	isOpen,
	setIsOpen,
	project,
}: {
	isOpen: boolean;
	setIsOpen: (open: boolean) => void;
	project: Project;
}) {
	const defaultDescription = `This action cannot be undone. This will permanently delete the project `;
	const deleteProjectMutation = useDeleteProject({
		id: project.id,
		name: project.name,
	});
	const deletePermission = usePermission("projects.delete");

	const confirmDeleteProject = () => {
		deleteProjectMutation.mutate();
	};

	return (
		<AlertDialog open={isOpen} onOpenChange={setIsOpen}>
			<AlertDialogContent>
				<AlertDialogHeader>
					<AlertDialogTitle>Are you sure?</AlertDialogTitle>
					<AlertDialogDescription>
						{defaultDescription}
						<strong> {project.name}</strong>.
					</AlertDialogDescription>
				</AlertDialogHeader>
				<AlertDialogFooter>
					<AlertDialogCancel>Cancel</AlertDialogCancel>
					{deletePermission.allowed ? (
						<AlertDialogAction
							onClick={confirmDeleteProject}
							className="bg-red-600 text-white hover:bg-red-700"
						>
							Delete
						</AlertDialogAction>
					) : (
						<Tooltip>
							<TooltipTrigger asChild>
								<span tabIndex={0} className="inline-flex">
									<AlertDialogAction
										disabled
										className="bg-red-600 text-white hover:bg-red-700 disabled:opacity-50 disabled:pointer-events-none"
										onClick={(e) => e.preventDefault()}
									>
										Delete
									</AlertDialogAction>
								</span>
							</TooltipTrigger>
							<TooltipContent>{deletePermission.reason}</TooltipContent>
						</Tooltip>
					)}
				</AlertDialogFooter>
			</AlertDialogContent>
		</AlertDialog>
	);
}
