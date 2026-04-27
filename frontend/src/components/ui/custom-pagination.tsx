import * as React from "react";
import {
  Pagination,
  PaginationNext,
  PaginationEllipsis,
  PaginationItem,
  PaginationContent,
  PaginationPrevious,
  PaginationLink,
} from "@/components/ui/pagination";

// If PaginationPage is a custom component, import it from its correct path, e.g.:
// import { PaginationPage } from "@/components/ui/pagination-page";

interface Props {
  totalPages: number;
  currentPage: number;
  onPageChange: (page: number) => void;
}

export function CustomPagination({
  totalPages,
  currentPage,
  onPageChange,
}: Props) {
  // Helper to generate page numbers with ellipsis
  function getPageNumbers() {
    const pages: (number | "ellipsis")[] = [];
    if (totalPages <= 5) {
      for (let i = 1; i <= totalPages; i++) pages.push(i);
    } else {
      if (currentPage <= 3) {
        pages.push(1, 2, 3, "ellipsis", totalPages);
      } else if (currentPage >= totalPages - 2) {
        pages.push(1, "ellipsis", totalPages - 2, totalPages - 1, totalPages);
      } else {
        pages.push(
          1,
          "ellipsis",
          currentPage - 1,
          currentPage,
          currentPage + 1,
          "ellipsis",
          totalPages
        );
      }
    }
    return pages;
  }

  const pageNumbers = getPageNumbers();

  return (
    <div className="flex items-center justify-center gap-1">
      <button
            onClick={() => onPageChange(Math.max(1, currentPage - 1))}
        disabled={currentPage === 1}
        aria-label="Go to previous page"
        className="px-3 py-1.5 rounded-md border border-border hover:bg-accent disabled:opacity-50 disabled:hover:bg-transparent transition-colors"
      >
        Previous
      </button>
        {pageNumbers.map((page, idx) =>
          page === "ellipsis" ? (
          <span key={`ellipsis-${idx}`} className="px-2" aria-hidden="true">
            ...
          </span>
          ) : (
          <button
            key={page}
                onClick={() => onPageChange(Number(page))}
            aria-label={`Go to page ${page}`}
            aria-current={currentPage === page ? "page" : undefined}
            className={`px-2 py-1 rounded ${
              currentPage === page
                ? "bg-primary text-primary-foreground"
                : "hover:bg-accent"
            }`}
              >
                {page}
          </button>
          )
        )}
      <button
            onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
        disabled={currentPage === totalPages}
        aria-label="Go to next page"
        className="px-3 py-1.5 rounded-md border border-border hover:bg-accent disabled:opacity-50 disabled:hover:bg-transparent transition-colors"
      >
        Next
      </button>
    </div>
  );
}
