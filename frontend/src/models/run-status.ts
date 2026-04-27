import { ValidationError } from "./validation-error";

export interface RunStatus {
	value: {
		jobStatuses: JobStatus[];
		runStatus: string;
	};
	status: number;
	isSuccess: boolean;
	successMessage: string;
	correlationId: string;
	location: string;
	errors: string[];
	validationErrors: ValidationError[];
}

export interface JobStatus {
	id: string;
	jobId: string;
	processedRecords: number;
	totalRecords: number;
	status: 'Processing' | 'Completed' | 'Failed' | 'Queued' | 'StepCompleted' | 'StepFailed';
	error: string;
	startTime: string; // Consider using Date type
	endTime: string; // Consider using Date type
	steps: {
		[key: string]: JobStep; // Using an index signature for dynamic step keys
	};
	metadata: {
		[key: string]: string; // Using an index signature for dynamic metadata keys
	};
	dataSourceName: string;
	statistics: JobStatistics;
}

interface JobStep {
	stepKey: string;
	stepName: string;
	stepNumber: number;
	totalSteps: number;
	processedItems: number;
	message: string;
	totalItems: number;
	status: 'Processing' | 'Completed' | 'Failed';
	error: string;
	startTime: string; // Consider using Date type
	endTime: string; // Consider using Date type
}

interface JobStatistics {
	recordsProcessed: number;
	errorRecords: number;
	batchesProcessed: number;
	transformationsApplied: number;
	startTime: string; // Consider using Date type
	endTime: string; // Consider using Date type
	duration: string;
	errorCategories: {
		[key: string]: number; // Using an index signature for dynamic error category keys
	};
	operationType: string;
	recordsPerSecond: number;
}

