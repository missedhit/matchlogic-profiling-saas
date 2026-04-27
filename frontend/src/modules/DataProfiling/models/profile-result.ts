import { ColumnProfile } from "./column-profile";

export interface ProfileResult {
  advancedColumnProfiles: ColumnProfile;
  correlationMatrix: {
    columns: string[];
    values: number[][] | null;
  };
  columnRelationships: any[]; // Can be typed more strictly if structure is known
  datasetQuality: DatasetQuality;
  warnings: string[];
  recommendations: string[];
  candidateKeys: {
    columns: string[];
    uniqueness: number;
    nonNullCount: number;
  }[];
  functionalDependencies: {
    determinantColumns: string[];
    dependentColumns: string[];
    confidence: number;
    dependencyType: string;
  }[];
  id: string;
  dataSourceName: string;
  dataSourceId: string;
  profiledAt: string;
  totalRecords: number;
  profilingDuration: string;
  rowReferenceDocumentIds: any[]; // Can be typed more strictly if structure is known
}


export interface DatasetQuality {
  overallScore: number;
  columnScores: Record<string, number>;
  qualityIssues: string[];
}