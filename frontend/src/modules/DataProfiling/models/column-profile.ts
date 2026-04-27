export interface ColumnProfile {
  [key: string]: DataProfile;
}

export interface DataProfile {
    inferredDataType: string;
    typeDetectionConfidence: number;
    typeDetectionResults: TypeDetectionResult[];
    histogram: any;
    outliers: any[];
    clusters: Cluster[];
    skewness: number;
    kurtosis: number;
    interquartileRange: any;
    qualityScore: QualityScore;
    discoveredPatterns: any[];
    appliedRules: Rule[];
    violations: Violation[];
    detectedFormat: DetectedFormat;
    possibleSemanticTypes: SemanticType[];
    warnings: any[];
    fieldName: string;
    type: string;
    length: number;
    pattern: string;
    total: number;
    valid: number;
    invalid: number;
    filled: number;
    null: number;
    distinct: number;
    numbers: number;
    numbersOnly: number;
    letters: number;
    lettersOnly: number;
    numbersAndLetters: number;
    punctuation: number;
    leadingSpaces: number;
    nonPrintableCharacters: number;
    min: string;
    max: string;
    mean: any;
    median: any;
    mode: any;
    extreme: any;
    sampleValues: any[];
    valueDistribution: any;
    patterns: Pattern[];
    characteristicRowDocumentIds: Record<string, any>;
    patternMatchRowDocumentIds: Record<string, any>;
    valueRowDocumentIds: Record<string, any>;
}

interface TypeDetectionResult {
  dataType: string;
  confidence: number;
}

interface Cluster {
  clusterId: number;
  count: number;
  centroid: number;
  representative: string | null;
  sampleValues: string[];
  sampleRows: SampleRow[];
}

interface SampleRow {
  rowData: RowData | null;
  value: string;
  rowNumber: number;
}

export interface RowData {
  _id: {
    timestamp: number;
    machine: number;
    pid: number;
    increment: number;
    creationTime: string;
  };
  _metadata: {
    RowNumber: number;
    Hash: string;
    SourceFile: string;
    BlockingKey: string | null;
  };
  [key: string]: string | number | boolean | object | null | undefined;
}

interface QualityScore {
  overallScore: number;
  completeness: number;
  accuracy: number;
  consistency: number;
  uniqueness: number;
  validity: number;
}

interface Rule {
  ruleName: string;
  description: string;
  passCount: number;
  failCount: number;
  parameters: Record<string, string>;
}

interface Violation {
  ruleName: string;
  value: string | null;
  message: string;
  examples: SampleRow[];
}

interface DetectedFormat {
  format: string;
  confidence: number;
  examples: any[];
}

export interface SemanticType {
  type: string;
  confidence: number;
}

interface Pattern {
  pattern: string;
  count: number;
  matchPercentage: number;
}
