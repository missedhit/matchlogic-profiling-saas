/**
 * Utility functions for parsing CSV and Excel files
 */

import { getFileType } from "./file-type";

export interface ParsedFile {
  id: string;
  name: string;
  columns: string[];
  data: Record<string, any>[];
  originalFile: File;
  type: string;
}

/**
 * Parse a CSV file and return its contents
 */
export async function parseCSVFile(file: File): Promise<ParsedFile> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();

    reader.onload = (event) => {
      try {
        const content = event.target?.result as string;
        const lines = content.split("\n");

        // Extract headers (first line)
        const headers = lines[0]
          .split(",")
          .map((header) => header.trim().replace(/"/g, ""));

        // Parse data rows
        const data: Record<string, any>[] = [];
        for (let i = 1; i < lines.length; i++) {
          if (lines[i].trim() === "") continue;

          const values = lines[i]
            .split(",")
            .map((value) => value.trim().replace(/"/g, ""));
          const row: Record<string, any> = {};

          headers.forEach((header, index) => {
            row[header] = values[index] || "";
          });

          data.push(row);
        }
        const fileType = getFileType(file.name);
        resolve({
          id: file.name,
          name: file.name,
          columns: headers,
          data: data.slice(0, 100), // Include first 100 rows for preview
          originalFile: file,
          type: fileType,
        });
      } catch (error) {
        reject(new Error(`Failed to parse CSV file: ${error}`));
      }
    };

    reader.onerror = () => {
      reject(new Error("Failed to read file"));
    };

    reader.readAsText(file);
  });
}

/**
 * Parse an Excel file and return its contents
 * Note: This is a simplified version that treats Excel files as CSV
 * In a real application, you would use a library like xlsx or exceljs
 */
export async function parseExcelFile(file: File): Promise<ParsedFile> {
  // For demo purposes, we'll treat Excel files like CSV files
  // In a real application, you would use a library like xlsx or exceljs
  return parseCSVFile(file);
}

/**
 * Parse a file based on its type
 */
export async function parseFile(file: File, type: string): Promise<ParsedFile> {
  if (type === "csv") {
    return parseCSVFile(file);
  } else if (type === "xlsx") {
    return parseExcelFile(file);
  } else {
    throw new Error(`Unsupported file type: ${type}`);
  }
}
