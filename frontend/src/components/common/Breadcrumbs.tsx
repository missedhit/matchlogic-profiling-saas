"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { ChevronRight, Database } from "lucide-react";
import { Badge } from "../ui/badge";
import { useRouteGuard } from "@/providers/route-guard-provider";

const ROUTE_LABELS: Record<string, string> = {
  "project-management": "Project Management",
  "data-import": "Data Import",
  "data-profiling": "Data Profiling",
  "data-cleansing": "Data Cleansing",
  "match-configuration": "Match Configuration",
  "match-definitions": "Match Definitions",
  "match-results": "Match Results",
  "merge-and-survivorship": "Merge & Survivorship",
  "final-export": "Final Export",
  settings: "Settings",
};

const SUB_ROUTE_LABELS: Record<string, string> = {
  "data-sources": "Data Sources",
  "select-table": "Select Table",
  "column-mapping": "Column Mapping",
  preview: "Preview",
};

export default function Breadcrumbs() {
  const pathname = usePathname();
  const { currentProject, dataSources, dataSourceId } = useRouteGuard();

  const segments = pathname.split("/").filter(Boolean);
  const moduleSlug = segments[0] || "";
  const moduleName =
    ROUTE_LABELS[moduleSlug] ||
    moduleSlug
      .split("-")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(" ") ||
    "Dashboard";
  const subRoute = segments.length > 1 ? SUB_ROUTE_LABELS[segments[1]] : null;

  const isProjectManagement = moduleSlug === "project-management";
  const showDatasourceChip =
    (pathname.startsWith("/data-profiling") ||
      pathname.startsWith("/data-cleansing")) &&
    dataSourceId;
  const selectedDs = dataSources?.find((ds) => ds.id === dataSourceId);

  // Determine the final (current) page title
  const currentTitle = subRoute || moduleName;

  return (
    <div className="flex items-center gap-2 shrink-0">
      <div className="flex items-center gap-1.5 whitespace-nowrap">
        {/* {currentProject && !isProjectManagement && (
          <>
            <Link
              href="/project-management"
              className="text-sm text-muted-foreground hover:text-foreground transition-colors"
            >
              {currentProject.name}
            </Link>
            <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/50" />
          </>
        )} */}
        {subRoute ? (
          <>
            <span className="text-sm text-muted-foreground">{moduleName}</span>
            <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/50" />
            <h1 className="text-sm font-semibold">{subRoute}</h1>
          </>
        ) : (
          <h1 className="text-sm font-semibold">{moduleName}</h1>
        )}
      </div>

      {/* {showDatasourceChip && selectedDs && (
        <Badge variant="secondary" className="ml-2 text-xs font-normal gap-1">
          <Database className="h-3 w-3" />
          Source: {selectedDs.name}
        </Badge>
      )} */}
    </div>
  );
}
