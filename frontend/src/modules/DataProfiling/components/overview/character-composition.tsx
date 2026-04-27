"use client";


import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import { ColumnProfile } from "@/modules/DataProfiling/models/column-profile";
import { useState, useEffect } from "react";
import { cn } from "@/lib/utils";

interface CharacterCompositionProps {
  data: ColumnProfile;
}

interface CompositionData {
  name: string;
  count: number;
  value: number; // Represents percentage
}

interface FieldComposition {
  fieldName: string;
  compositions: CompositionData[];
}


// Define colors for each composition type, supporting light and dark modes
const compositionColors: { [key: string]: string } = {
  'Letters': 'bg-letters',
  'Letters Only': 'bg-lettersOnly',
  'Numbers': 'bg-numbers',
  'Numbers Only': 'bg-numbersOnly',
  'Letters and Numbers': 'bg-lettersAndNumbers',
  'Punctuation': 'bg-punctuation',
  'Leading spaces': 'bg-leadingSpaces',
  'Non Printable Characters': 'bg-nonPrintable',
};

const legendOrder = [
    'Letters',
    'Letters Only',
    'Numbers',
    'Numbers Only',
    'Letters and Numbers',
    'Punctuation',
    'Leading spaces',
    'Non Printable Characters',
];

const mockData: FieldComposition[] = [
  {
    fieldName: 'Company Name',
    compositions: [
      { name: 'Letters', count: 50, value: 50 },
      { name: 'Numbers', count: 50, value: 50 },
      { name: 'Punctuation', count: 1, value: 1 },
      { name: 'Leading spaces', count: 1, value: 1 },
      { name: 'Letters Only', count: 1, value: 1 },
      { name: 'Numbers Only', count: 1, value: 1 },
      { name: 'Non Printable Characters', count: 1, value: 1 },
    ],
  },
]

export function CharacterComposition({ data }: CharacterCompositionProps) {
  const [processedData, setProcessedData] = useState<FieldComposition[]>([]);
  const [filteredData, setFilteredData] = useState<FieldComposition[]>([]);
  
  
  useEffect(() => {
    const formattedData = Object.entries(data)
      .filter(
        ([key, value]) => value.typeDetectionResults?.[0]?.dataType === "String"
      )
      .map(([key, value]) => {
        const total = (value.letters || 0) + (value.numbers || 0) + (value.punctuation || 0) + (value.leadingSpaces || 0) + (value.lettersOnly || 0) + (value.numbersOnly || 0) + (value.numbersAndLetters || 0) + (value.nonPrintableCharacters || 0);
        const pct = (n: number) => total > 0 ? (n / total) * 100 : 0;
        const percentages = {
          letters: pct(value.letters || 0),
          lettersOnly: pct(value.lettersOnly || 0),
          numbers: pct(value.numbers || 0),
          numbersOnly: pct(value.numbersOnly || 0),
          numbersAndLetters: pct(value.numbersAndLetters || 0),
          punctuation: pct(value.punctuation || 0),
          leadingSpaces: pct(value.leadingSpaces || 0),
          nonPrintableCharacters: pct(value.nonPrintableCharacters || 0),
        }
        return {
          fieldName: key,
          compositions: [
            { name: "Letters", count: value.letters, value: percentages.letters <= 1 ? 2 : percentages.letters},
            { name: "Letters Only", count: value.lettersOnly, value: percentages.lettersOnly <= 1 ? 2 : percentages.lettersOnly},
            { name: "Numbers", count: value.numbers, value: percentages.numbers <= 1 ? 2 : percentages.numbers},
            { name: "Numbers Only", count: value.numbersOnly, value: percentages.numbersOnly <= 1 ? 2 : percentages.numbersOnly},
            { name: "Letters and Numbers", count: value.numbersAndLetters, value: percentages.numbersAndLetters <= 1 ? 2 : percentages.numbersAndLetters},
            { name: "Punctuation", count: value.punctuation, value: percentages.punctuation <= 1 ? 2 : percentages.punctuation},
            { name: "Leading spaces", count: value.leadingSpaces, value: percentages.leadingSpaces <= 1 ? 2 : percentages.leadingSpaces},
            { name: "Non Printable Characters", count: value.nonPrintableCharacters, value: percentages.nonPrintableCharacters <= 1 ? 2 : percentages.nonPrintableCharacters},
          ],
        };
      });
    setProcessedData(formattedData);
    setFilteredData(formattedData);
  }, [data]);
  
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">
          Character Composition Breakdown
        </CardTitle>
        <CardDescription className="text-sm">
          <span>Show the proportion of letters, numbers, punctuation, and special
          characters in fields</span>
          <Legend processedData={processedData} setFilteredData={setFilteredData}/>
        </CardDescription>
      </CardHeader>
      <CardContent className="h-[350px] hover-scrollbar custom-scrollbar">
        <div className="space-y-6">
          {filteredData.map((field) => (
            <div key={field.fieldName}>
              <p className="mb-2 text-sm font-medium text-gray-700 dark:text-gray-300">{field.fieldName}</p>
              <CompositionBar compositions={field.compositions} />
            </div>
          ))}
        </div>
      </CardContent>
    </Card>

  );
}

function Legend({processedData, setFilteredData}: {processedData: FieldComposition[], setFilteredData: (data: FieldComposition[]) => void}) {
  const legendSelector = (compositionName: string) => {
    setFilteredData(processedData.map(item => ({
      fieldName: item.fieldName,
      compositions: item.compositions.filter(composition => composition.name === compositionName)
    })))  
  }

  return (
    <div className="flex flex-wrap gap-x-6 gap-y-2 mt-3 text-xs text-gray-500 dark:text-gray-400">
      {legendOrder.map((name) => (
          <div key={name} className="flex items-center gap-2">
            <span
              className={cn(
                "w-3 h-3 rounded-full",
                compositionColors[name]
            )}
          />
          <button type="button" onClick={() => legendSelector(name)} className="cursor-pointer hover:underline">
            {name}
          </button>
        </div>
      ))}
    </div>
  );
}

function CompositionBar({ compositions }: { compositions: CompositionData[] }) {
  return (
    <div className="w-full h-4 flex overflow-hidden bg-gray-200 dark:bg-gray-700">
      {compositions.map((comp, index) => (
        <Tooltip key={index}>
          <TooltipTrigger asChild>
            <div
              className={cn(
                "h-full cursor-pointer", 
                compositionColors[comp.name]
              )}
              style={{ width: `${comp.value}%` }}
            />
          </TooltipTrigger>
          <TooltipContent>
            <p>{`${comp.name}: ${comp.count} Records`}</p>
          </TooltipContent>
        </Tooltip>
      ))}
    </div>
  );
}