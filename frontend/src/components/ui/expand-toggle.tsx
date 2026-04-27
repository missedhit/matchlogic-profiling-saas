"use client";

import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";

interface ExpandToggleProps {
  isExpanded: boolean;
  onToggle: () => void;
  label?: string;
  /** Extra className for the label <span> */
  labelClassName?: string;
  className?: string;
}

/**
 * Shared expand/collapse toggle button.
 * Renders a proper <button> with aria-expanded, keyboard accessible by default.
 */
export function ExpandToggle({
  isExpanded,
  onToggle,
  label,
  labelClassName,
  className,
}: ExpandToggleProps) {
  return (
    <Button
      type="button"
      variant="ghost"
      onClick={onToggle}
      aria-expanded={isExpanded}
      aria-label={label ?? (isExpanded ? "Collapse" : "Expand")}
      className={cn(
        "flex items-center justify-start gap-2 p-0 h-auto font-normal text-left hover:bg-transparent",
        className
      )}
    >
      <ChevronDown
        className={cn(
          "h-4 w-4 text-gray-400 shrink-0 transition-transform duration-200",
          !isExpanded && "-rotate-90"
        )}
      />
      {label && <span className={labelClassName}>{label}</span>}
    </Button>
  );
}
