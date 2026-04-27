import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { useState } from "react";
import { useCreateProject } from "@/modules/ProjectManagement/hooks/use-create-project";

export default function CreateProjectModal({
	isOpen,
	setIsOpen,
}: {
	isOpen: boolean;
	setIsOpen: (open: boolean) => void;
}) {
	const [name, setName] = useState("");
	const [description, setDescription] = useState("");
	const [errors, setErrors] = useState<Record<string, string>>({});
	const createProjectMutation = useCreateProject();

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
			createProjectMutation.mutate(
				{ name, description },
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
						<DialogTitle>Create New Project</DialogTitle>
						<DialogDescription>
							Add a new project to your workspace.
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
						<Button type="submit" disabled={createProjectMutation.isPending} requiredPermission="projects.create">
						{createProjectMutation.isPending ? "Creating…" : "Create Project"}
					</Button>
					</DialogFooter>
				</form>
			</DialogContent>
		</Dialog>
	);
}
