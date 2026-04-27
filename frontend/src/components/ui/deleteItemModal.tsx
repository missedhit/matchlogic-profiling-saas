import React from "react";
import { AlertDialog, AlertDialogContent, AlertDialogHeader, AlertDialogTitle, AlertDialogDescription, AlertDialogFooter, AlertDialogCancel, AlertDialogAction } from "./alert-dialog";

interface DeleteItemModalProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
  itemName: string;
  itemType: string;
  customDescription?: string;
  confirmButtonText?: string;
  cancelButtonText?: string;
}

export const DeleteItemModal = ({
  isOpen,
  onOpenChange,
  onConfirm,
  itemName,
  itemType,
  customDescription,
  confirmButtonText = "Delete",
  cancelButtonText = "Cancel"
}: DeleteItemModalProps) => {
  const defaultDescription = `This action cannot be undone. This will permanently delete the ${itemType} `;

  return (
    <AlertDialog
      open={isOpen}
      onOpenChange={onOpenChange}
  >
    <AlertDialogContent>
      <AlertDialogHeader>
        <AlertDialogTitle>Are you sure?</AlertDialogTitle>
        <AlertDialogDescription>
            {customDescription || defaultDescription}
            <strong> {itemName}</strong>.
        </AlertDialogDescription>
      </AlertDialogHeader>
      <AlertDialogFooter>
          <AlertDialogCancel>{cancelButtonText}</AlertDialogCancel>
        <AlertDialogAction
            onClick={onConfirm}
          className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
        >
            {confirmButtonText}
        </AlertDialogAction>
      </AlertDialogFooter>
    </AlertDialogContent>
  </AlertDialog>
  );
};
