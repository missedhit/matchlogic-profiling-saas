"use client";

import React from "react";
import { AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";

interface Props {
  children: React.ReactNode;
  fallbackClassName?: string;
}

interface State {
  hasError: boolean;
  retryCount: number;
}

/**
 * Catches unhandled runtime exceptions in child components and renders
 * a graceful error card instead of crashing the whole page.
 */
export class RuntimeErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, retryCount: 0 };
  }

  static getDerivedStateFromError(): Pick<State, "hasError"> {
    return { hasError: true };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error("[RuntimeErrorBoundary]", error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className={`flex items-center justify-center ${this.props.fallbackClassName ?? "h-64"}`}>
          <div className="text-center max-w-sm">
            <AlertCircle className="h-8 w-8 mx-auto mb-3 text-muted-foreground/50" />
            <p className="text-sm font-medium text-muted-foreground">
              Unable to render this section
            </p>
            <p className="text-xs text-muted-foreground/70 mt-1">
              The data may be in an unexpected format.
            </p>
            {this.state.retryCount < 3 ? (
              <Button
                variant="outline"
                size="sm"
                className="mt-3"
                onClick={() => this.setState((s) => ({ hasError: false, retryCount: s.retryCount + 1 }))}
              >
                Retry
              </Button>
            ) : (
              <p className="text-xs text-destructive mt-3">
                This section could not be recovered. Please refresh the page.
              </p>
            )}
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
