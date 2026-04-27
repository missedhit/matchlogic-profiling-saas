"use client";

import { ReactNode } from "react";
import { cn } from "@/lib/utils";

export type SidebarIconState = "default" | "hover" | "active" | "locked";

interface SidebarIconProps {
  /** The icon component to render */
  children: ReactNode;
  /** Whether this icon represents the currently active/selected route */
  isActive?: boolean;
  /** Whether this icon is locked/disabled (route not yet accessible) */
  isLocked?: boolean;
  className?: string;
}

/**
 * SidebarIcon wraps a custom SVG icon and applies the correct visual state
 * based on active/locked/hover status.
 *
 * Color semantics:
 *  - default:  sidebar-foreground/70 (muted white-purple)
 *  - hover:    white (applied via parent hover classes)
 *  - active:   sidebar (dark bg) — icon sits inside sidebar-accent pill
 *  - locked:   sidebar-foreground/40 (very muted)
 *
 * The component itself only controls the `color` via Tailwind text utilities.
 * The parent <Link> or <span> element in Sidebar.tsx is responsible for the
 * background pill on active state.
 */
export function SidebarIcon({
  children,
  isActive = false,
  isLocked = false,
  className,
}: SidebarIconProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center justify-center",
        isLocked && "text-sidebar-foreground/40",
        isActive && "text-sidebar",
        !isActive && !isLocked && "text-sidebar-foreground/70",
        className
      )}
    >
      {children}
    </span>
  );
}
