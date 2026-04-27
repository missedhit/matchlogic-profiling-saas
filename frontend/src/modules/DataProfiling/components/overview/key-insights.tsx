import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/ui/card";
import { DataProfilingAnomaliesIcon, GeneralInfoIcon } from "@/assets/icons";
import { DatasetQuality } from "../../models/profile-result";

interface KeyInsightsProps {
  datasetQuality: DatasetQuality;
}

const ErrorAlert = ({ message }: { message: string }) => {
  return (
    <Alert
      variant="destructive"
      className="bg-red-50 border-red-200 text-red-800 py-2"
    >
      <DataProfilingAnomaliesIcon className="h-4 w-4" />
      <AlertTitle className="text-sm font-medium">{message}</AlertTitle>
    </Alert>
  );
};

const WarningAlert = ({ message }: { message: string }) => {
  return (
    <Alert
      variant="warning"
      className="bg-yellow-50 border-yellow-200 text-yellow-800 py-2"
    >
      <DataProfilingAnomaliesIcon className="h-4 w-4" />
      <AlertTitle className="text-sm font-medium">{message}</AlertTitle>
    </Alert>
  );
};

const InfoAlert = ({ message }: { message: string }) => {
  return (
    <Alert
      variant="info"
      className="bg-blue-50 border-blue-200 text-blue-800 py-2"
    >
      <GeneralInfoIcon className="h-4 w-4" />
      <AlertTitle className="text-sm font-medium">{message}</AlertTitle>
    </Alert>
  );
};

export default function KeyInsights({ datasetQuality }: KeyInsightsProps) {
  return (
    <Card className="md:col-span-1">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">Key Insights</CardTitle>
        <CardDescription className="text-sm">
          Critical issues and recommendations
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4 max-h-[278px] hover-scrollbar custom-scrollbar">
        {datasetQuality?.qualityIssues?.map((issue, index) => {
          return <WarningAlert key={index} message={issue} />;
        })}
      </CardContent>
    </Card>
  );
}
