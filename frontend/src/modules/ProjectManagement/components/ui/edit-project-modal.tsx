import { Button } from "@/components/ui/button";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Project } from "@/models/api-responses";
import { useEditProject } from "@/modules/ProjectManagement/hooks/use-edit-project";
import { useState } from "react";

export default function EditProjectModal({
	isOpen,
	setIsOpen,
	project,
}: {
	isOpen: boolean;
	setIsOpen: (open: boolean) => void;
	project: Project;
}) {
	const editProjectMutation = useEditProject();

	const [name, setName] = useState(project.name);
	const [description, setDescription] = useState(project.description);
	const [errors, setErrors] = useState<Record<string, string>>({});

	const validateForm = () => {
		const newErrors: Record<string, string> = {};

		if (!name.trim()) {
			newErrors.name = "Project name is required";
		}

		setErrors(newErrors);
		return Object.keys(newErrors).length === 0;
	};

	const handleSubmit = (e: React.FormEvent) => {
		e.preventDefault();

		if (validateForm()) {
			editProjectMutation.mutate(
				{ id: project.id, name, description },
				{
					onSuccess: () => {
						setName("");
						setDescription("");
						setIsOpen(false);
					},
				}
			);
		}
	};

	return (
		<Dialog open={isOpen} onOpenChange={setIsOpen}>
			<DialogContent className="sm:max-w-[500px]">
				<form onSubmit={handleSubmit}>
					<DialogHeader>
						<DialogTitle>Edit Project</DialogTitle>
						<DialogDescription>
							Update the details of your project.
						</DialogDescription>
					</DialogHeader>
					<div className="grid gap-4 py-4">
						<div className="grid grid-cols-4 items-center gap-4">
							<Label htmlFor="name" className="text-right">
								Name
							</Label>
							<div className="col-span-3 space-y-1">
								<Input
									id="name"
									value={name}
									onChange={(e) => setName(e.target.value)}
									className={errors.name ? "border-red-500" : ""}
								/>
								{errors.name && (
									<p className="text-xs text-red-500">{errors.name}</p>
								)}
							</div>
						</div>
						<div className="grid grid-cols-4 items-center gap-4">
							<Label htmlFor="description" className="text-right">
								Description
							</Label>
							<div className="col-span-3 space-y-1">
								<Textarea
									id="description"
									value={description}
									onChange={(e) => setDescription(e.target.value)}
									className={errors.description ? "border-red-500" : ""}
								/>
								{errors.description && (
									<p className="text-xs text-red-500">{errors.description}</p>
								)}
							</div>
						</div>
					</div>
					<DialogFooter>
						<Button
							type="button"
							variant="outline"
							onClick={() => setIsOpen(false)}
						>
							Cancel
						</Button>
						<Button type="submit" disabled={editProjectMutation.isPending} requiredPermission="projects.update">
						{editProjectMutation.isPending ? "Saving…" : "Save Changes"}
					</Button>
					</DialogFooter>
				</form>
			</DialogContent>
		</Dialog>
	);
}
