import { useEffect, useState } from "react";
import { MessageSquare, Pencil } from "lucide-react";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import {
	applyDoneStatus,
	isDone,
	stripDone,
} from "@/modules/DataProfiling/utils/column-notes-status";

interface ColumnNoteInputProps {
	fieldName: string;
	initialValue: string;
	onSave: (value: string) => void;
	isSaving?: boolean;
}

export function ColumnNoteInput({
	fieldName,
	initialValue,
	onSave,
	isSaving = false,
}: ColumnNoteInputProps) {
	const displayInitial = stripDone(initialValue);
	const wasDone = isDone(initialValue);
	const hasNote = displayInitial.trim().length > 0;

	const [open, setOpen] = useState(false);
	const [value, setValue] = useState(displayInitial);

	useEffect(() => {
		if (open) setValue(displayInitial);
	}, [open, displayInitial]);

	const isDirty = value !== displayInitial;

	const handleSave = () => {
		if (!isDirty) {
			setOpen(false);
			return;
		}
		onSave(applyDoneStatus(value, wasDone));
		setOpen(false);
	};

	return (
		<>
			<button
				type="button"
				onClick={(e) => {
					e.stopPropagation();
					setOpen(true);
				}}
				className={cn(
					"inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs transition-colors",
					hasNote
						? "bg-primary/10 text-primary border border-primary/30 hover:bg-primary/15"
						: "text-muted-foreground hover:text-foreground hover:bg-muted"
				)}
				aria-label={hasNote ? `Edit note for ${fieldName}` : `Add note for ${fieldName}`}
			>
				{hasNote ? (
					<>
						<MessageSquare className="h-3.5 w-3.5 shrink-0" strokeWidth={1.75} />
						<span className="font-medium">1</span>
					</>
				) : (
					<>
						<Pencil className="h-3.5 w-3.5 shrink-0" strokeWidth={1.75} />
						<span>Add</span>
					</>
				)}
			</button>

			<Dialog open={open} onOpenChange={setOpen}>
				<DialogContent
					className="sm:max-w-md"
					onClick={(e) => e.stopPropagation()}
				>
					<DialogHeader>
						<DialogTitle>Note for {fieldName}</DialogTitle>
						<DialogDescription>
							Capture context, observations, or reminders for this column.
						</DialogDescription>
					</DialogHeader>
					<Textarea
						value={value}
						onChange={(e) => setValue(e.target.value)}
						onKeyDown={(e) => e.stopPropagation()}
						placeholder="Write your note..."
						rows={6}
						autoFocus
						className="resize-none"
					/>
					<DialogFooter>
						<Button
							variant="outline"
							onClick={() => setOpen(false)}
							disabled={isSaving}
						>
							Cancel
						</Button>
						<Button onClick={handleSave} disabled={isSaving || !isDirty}>
							{isSaving ? "Saving..." : "Save"}
						</Button>
					</DialogFooter>
				</DialogContent>
			</Dialog>
		</>
	);
}
