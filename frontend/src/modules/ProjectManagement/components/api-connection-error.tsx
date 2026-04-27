import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { GeneralWarningIcon } from "@/assets/icons";
import { useState } from "react";

export default function ApiConnectionError() {
    const [retryCount, setRetryCount] = useState(0);
    
    const handleRetryConnection = () => {
        setRetryCount((prev) => prev + 1);
      };
    return (
        <Alert variant="destructive">
          <GeneralWarningIcon className="h-4 w-4" />
          <AlertTitle>API Connection Error</AlertTitle>
          <AlertDescription>
            Unable to connect to the API. Using local data instead.
            <Button
              variant="outline"
              size="sm"
              onClick={handleRetryConnection}
              className="ml-2"
            >
              Retry Connection
            </Button>
          </AlertDescription>
        </Alert>
    );
}