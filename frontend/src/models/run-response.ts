import { BasicResponse } from "@/models/basic-response";

export interface RunResponse extends BasicResponse {
  value: {
    projectRun: ProjectRun;
  };
}

interface ProjectRun {
  id: string;
  previousRunId: string;
  projectId: string;
  startTime: string; // Consider using Date type if you plan to work with Date objects
  endTime: string; // Consider using Date type if you plan to work with Date objects
  status: number;
  runNumber: number;
  dataImportResult: DataImportResult;
}

interface DataImportResult {
  [key: string]: string; // This allows for flexible string key-value pairs
}
