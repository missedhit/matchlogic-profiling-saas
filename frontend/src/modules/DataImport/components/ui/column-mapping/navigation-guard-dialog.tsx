import { useState } from "react";
import {
	AlertDialog,
	AlertDialogAction,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
} from "@/components/ui/alert-dialog";

interface NavigationConfirmationDialogProps {
	isOpen: boolean;
	onConfirm: () => void;
	onCancel: () => void;
	title?: string;
	description?: string;
	confirmText?: string;
	cancelText?: string;
}

export function NavigationConfirmationDialog({
	isOpen,
	onCancel,
	onConfirm,
	title = "Unsaved Changes",
	description = "You have unsaved changes that will be lost if you leave this page. Are you sure you want to continue?",
	confirmText = "Leave Page",
	cancelText = "Stay on Page",
}: NavigationConfirmationDialogProps) {
	return (
		<AlertDialog open={isOpen} onOpenChange={onCancel}>
			<AlertDialogContent>
				<AlertDialogHeader>
					<AlertDialogTitle className="text-primary">
						{title}
					</AlertDialogTitle>
					<AlertDialogDescription>{description}</AlertDialogDescription>
				</AlertDialogHeader>
				<AlertDialogFooter>
					<AlertDialogCancel onClick={onCancel}>{cancelText}</AlertDialogCancel>
					<AlertDialogAction
						onClick={onConfirm}
						className="bg-primary hover:bg-primary/90"
					>
						{confirmText}
					</AlertDialogAction>
				</AlertDialogFooter>
			</AlertDialogContent>
		</AlertDialog>
	);
}
