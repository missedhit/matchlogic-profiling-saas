import { TabsContent } from "@/components/ui/tabs";
import { StandardViewTable } from "./detailed-analysis/standard-view-table";
import { useState, useEffect } from "react";
import { NumericViewTable } from "./detailed-analysis/numeric-view-table";
import { StandardViewDataTable } from "./detailed-analysis/standard-view-data-table";
import { GraphicalRepresentationButton } from "./detailed-analysis/graphical-representation-button";
import { PatternListContainer } from "./detailed-analysis/pattern-list-container";
import { DistinctValuePanel } from "./detailed-analysis/distinct-value-panel";
import { useAdvanceAnalyticsQuery } from "@/modules/DataProfiling/hooks/use-advance-analytics-query";
import { CellFilterInfo } from "@/modules/DataProfiling/models/cell-filteration";
import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable";
import { Skeleton } from "@/components/ui/skeleton";

interface ProfileDetailsProps {
  dataSourceId: string;
  activeView?: string;
}

export default function ProfileDetails({
  dataSourceId,
  activeView,
}: ProfileDetailsProps) {
  const [selectedField, setSelectedField] = useState<string>("");
  const [selectedCharacteristic, setSelectedCharacteristic] =
    useState<string>("");
  const [activeFilter, setActiveFilter] = useState<CellFilterInfo | null>(null);
  const [filterDocumentId, setFilterDocumentId] = useState<string>("");

  // Pattern view states
  const [showPatternView, setShowPatternView] = useState<boolean>(false);
  const [selectedPattern, setSelectedPattern] = useState<any>(null);
  const [patternFieldName, setPatternFieldName] = useState<string>("");

  // Auto-apply filter when activeFilter changes and has a documentId
  useEffect(() => {
    if (
      activeFilter &&
      activeFilter.documentId &&
      activeFilter.filterType === "characteristic"
    ) {
      setFilterDocumentId(activeFilter.documentId);
    } else if (activeFilter && activeFilter.filterType === "anomalies" && activeFilter.directData) {
      // For anomalies filter, clear document-based filter data
      setFilterDocumentId("");
      setSelectedCharacteristic("");
    } else if (activeFilter && !activeFilter.documentId) {
      // Clear previous filter data when new filter is set but no documentId yet
      setFilterDocumentId("");
      setSelectedCharacteristic("");
    }

    // Clear pattern view if activeFilter is not pattern-related (but not when it's null from pattern click)
    if (activeFilter && activeFilter.filterType !== "pattern") {
      if (showPatternView) {
        setShowPatternView(false);
        setSelectedPattern(null);
        setPatternFieldName("");
      }
    }
  }, [activeFilter, showPatternView]);

  const { data: advanceAnalytics, isFetching } = useAdvanceAnalyticsQuery({
    dataSourceId,
  });

  // Handler for pattern cell clicks
  const handlePatternCellClick = (fieldName: string) => {
    // If the same field's pattern is clicked again and pattern view is already showing, clear it
    if (showPatternView && patternFieldName === fieldName) {
      setShowPatternView(false);
      setPatternFieldName("");
      setSelectedPattern(null);
      setSelectedField(""); // Clear selected field to return to initial state
      setActiveFilter(null);
      setFilterDocumentId("");
      setSelectedCharacteristic("");
      return;
    }

    setShowPatternView(true);
    setPatternFieldName(fieldName);
    setSelectedField(fieldName);
    setSelectedPattern(null);
    // Clear existing filters when switching to pattern view but keep a pattern filter to prevent clearing
    const patternFilterPlaceholder: CellFilterInfo = {
      fieldName: fieldName,
      columnName: "pattern",
      filterType: "pattern",
      patternName: "",
      documentId: "",
    };
    setActiveFilter(patternFilterPlaceholder);
    setFilterDocumentId("");
    setSelectedCharacteristic("");
  };

  // Handler for pattern selection
  const handlePatternSelect = (pattern: any) => {
    // Simply select/keep the pattern selected - don't close the view
    setSelectedPattern(pattern);
  };

  // Handler for pattern cell filtering
  const handlePatternCellFilter = (pattern: any, columnName: string) => {
    setSelectedPattern(pattern);

    // Create document ID based on column clicked
    let documentId = "";
    if (columnName === "validCount") {
      // For valid count, use pattern_Valid format
      documentId =
        advanceAnalytics?.value?.profileResult?.advancedColumnProfiles?.[
          patternFieldName
        ]?.patternMatchRowDocumentIds?.[`${pattern.pattern}_Valid`] || "";
    } else if (columnName === "invalidCount") {
      // For invalid count, use pattern_Invalid format
      documentId =
        advanceAnalytics?.value?.profileResult?.advancedColumnProfiles?.[
          patternFieldName
        ]?.patternMatchRowDocumentIds?.[`${pattern.pattern}_Invalid`] || "";
    }

    // Only proceed with filtering for valid/invalid clicks
    if (
      (columnName === "validCount" || columnName === "invalidCount") &&
      documentId
    ) {
      setFilterDocumentId(documentId);
      setSelectedCharacteristic(documentId);

      // Create filter info for the selected pattern
      const patternFilterInfo: CellFilterInfo = {
        fieldName: patternFieldName,
        columnName: "pattern",
        filterType: "pattern",
        patternName: pattern.pattern,
        documentId: documentId,
      };
      setActiveFilter(patternFilterInfo);
    }
    // For pattern name and description clicks, just select the pattern without filtering
  };

  // Handler to clear pattern view
  const handleClearPatternView = () => {
    setShowPatternView(false);
    setSelectedPattern(null);
    setPatternFieldName("");
    setSelectedField(""); // Clear selected field to return to initial state
    setActiveFilter(null);
    setFilterDocumentId("");
    setSelectedCharacteristic("");
  };

  // Handler to clear pattern view when other cells are clicked
  const handleOtherCellFilter = (filterInfo: CellFilterInfo | null) => {
    if (filterInfo === null) {
      // Clear all filter states
      setActiveFilter(null);
      setFilterDocumentId("");
      setSelectedCharacteristic("");
      return;
    }
    
    if (showPatternView && filterInfo.filterType !== "pattern") {
      setShowPatternView(false);
      setSelectedPattern(null);
      setPatternFieldName("");
    }
    setActiveFilter(filterInfo);
  };

  return (
    <>
      <TabsContent value="detailed">
        {isFetching && (
          <div className="space-y-6">
            <Skeleton className="h-[600px] w-full" />
          </div>
        )}
        {!isFetching && advanceAnalytics && !advanceAnalytics.value && (
          <div className="flex items-center justify-center h-96">
            <p className="text-gray-500">No profile data available. Please generate a profile from the Overview tab.</p>
          </div>
        )}
        {!isFetching && advanceAnalytics && advanceAnalytics.value && (
          <>
            {/* Show ResizablePanelGroup only when filter is active */}
            {(showPatternView || selectedField !== "" || activeFilter?.filterType === "anomalies" || activeFilter?.filterType === "distinct") ? (
              <div className="h-[calc(100vh-8rem)]">
                <ResizablePanelGroup direction="vertical">
                  {/* Main table on top */}
                  <ResizablePanel defaultSize={40} minSize={20} className="overflow-auto">
                    {activeView === "standard" && selectedField !== "" && (
                      <GraphicalRepresentationButton
                        data={
                          advanceAnalytics.value.profileResult.advancedColumnProfiles[
                            selectedField
                          ]
                        }
                        dataSourceId={dataSourceId}
                        fieldName={selectedField}
                      />
                    )}
                    {activeView === "standard" && (
                      <StandardViewTable
                        highlightedColumn={selectedField}
                        onFieldSelect={setSelectedField}
                        data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
                        onCharacteristicSelect={setSelectedCharacteristic}
                        selectedCharacteristic={selectedCharacteristic}
                        onCellFilter={handleOtherCellFilter}
                        activeFilter={activeFilter}
                        dataSourceId={dataSourceId}
                        onPatternCellClick={handlePatternCellClick}
                      />
                    )}
                    {activeView === "numeric" && (
                      <NumericViewTable
                        data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
                        onFieldSelect={setSelectedField}
                        highlightedColumn={selectedField}
                        onCellFilter={handleOtherCellFilter}
                        activeFilter={activeFilter}
                        dataSourceId={dataSourceId}
                        onPatternCellClick={handlePatternCellClick}
                        onCharacteristicSelect={setSelectedCharacteristic}
                      />
                    )}
                  </ResizablePanel>
                  <ResizableHandle withHandle />
                  {/* Filter section on bottom */}
                  <ResizablePanel defaultSize={60} minSize={20} className="overflow-auto">
                    {showPatternView && patternFieldName && (
                      <>
                        <div className="mt-4">
                          <div className="flex justify-between items-center mb-3">
                            <h2 className="text-lg font-medium">Pattern Analysis</h2>
                          </div>
                          <PatternListContainer
                            patterns={
                              advanceAnalytics.value.profileResult.advancedColumnProfiles[
                                patternFieldName
                              ]?.patterns || []
                            }
                            fieldName={patternFieldName}
                            onPatternSelect={handlePatternSelect}
                            selectedPattern={selectedPattern}
                            onPatternCellFilter={handlePatternCellFilter}
                            columnData={
                              advanceAnalytics.value.profileResult.advancedColumnProfiles
                            }
                            dataSourceId={dataSourceId}
                          />
                        </div>
                        {selectedPattern &&
                          filterDocumentId &&
                          activeFilter?.documentId && (
                            <div className="mt-4 h-[400px] overflow-auto">
                              <StandardViewDataTable
                                highlightedColumn={patternFieldName}
                                dataSourceId={dataSourceId}
                                headers={Object.keys(
                                  advanceAnalytics.value.profileResult.advancedColumnProfiles
                                )}
                                selectedCharacteristic={
                                  filterDocumentId || selectedCharacteristic
                                }
                                activeFilter={activeFilter}
                                onFilterApply={(docId) => {
                                  setFilterDocumentId(docId);
                                  setSelectedCharacteristic(docId);
                                }}
                                onFilterClear={() => {
                                  setFilterDocumentId("");
                                  setSelectedCharacteristic("");
                                  // Keep activeFilter so empty state can show
                                }}
                                columnData={
                                  advanceAnalytics.value.profileResult.advancedColumnProfiles
                                }
                                directData={activeFilter?.directData || null}
                              />
                            </div>
                          )}
                      </>
                    )}
                    {!showPatternView &&
                      activeFilter?.filterType === "distinct" &&
                      activeFilter.valueDistribution && (
                        <div className="h-full overflow-hidden">
                          <DistinctValuePanel
                            valueDistribution={activeFilter.valueDistribution}
                            totalRecords={activeFilter.totalRecords ?? 0}
                            fieldName={activeFilter.fieldName}
                          />
                        </div>
                      )}
                    {!showPatternView &&
                      activeFilter?.filterType !== "distinct" &&
                      (activeView === "standard" || activeView === "numeric") &&
                      (selectedField !== "" || activeFilter?.filterType === "anomalies") && (
                        <div className="h-full overflow-auto">
                          <StandardViewDataTable
                            highlightedColumn={selectedField}
                            dataSourceId={dataSourceId}
                            headers={Object.keys(
                              advanceAnalytics.value.profileResult.advancedColumnProfiles
                            )}
                            selectedCharacteristic={
                              filterDocumentId || selectedCharacteristic
                            }
                            activeFilter={activeFilter}
                            onFilterApply={(docId) => {
                              setFilterDocumentId(docId);
                              setSelectedCharacteristic(docId);
                            }}
                            onFilterClear={() => {
                              setFilterDocumentId("");
                              setSelectedCharacteristic("");
                              // Keep activeFilter so empty state can show
                            }}
                            columnData={
                              advanceAnalytics.value.profileResult.advancedColumnProfiles
                            }
                            directData={activeFilter?.directData || null}
                          />
                        </div>
                      )}
                  </ResizablePanel>
                </ResizablePanelGroup>
              </div>
            ) : (
              <>
                {/* Show tables without resizable when no filter is active */}
                {activeView === "standard" && (
                  <StandardViewTable
                    highlightedColumn={selectedField}
                    onFieldSelect={setSelectedField}
                    data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
                    onCharacteristicSelect={setSelectedCharacteristic}
                    selectedCharacteristic={selectedCharacteristic}
                    onCellFilter={handleOtherCellFilter}
                    activeFilter={activeFilter}
                    dataSourceId={dataSourceId}
                    onPatternCellClick={handlePatternCellClick}
                  />
                )}
                {activeView === "numeric" && (
                  <NumericViewTable
                    data={advanceAnalytics.value.profileResult.advancedColumnProfiles}
                    onFieldSelect={setSelectedField}
                    highlightedColumn={selectedField}
                    onCellFilter={handleOtherCellFilter}
                    activeFilter={activeFilter}
                    dataSourceId={dataSourceId}
                    onPatternCellClick={handlePatternCellClick}
                    onCharacteristicSelect={setSelectedCharacteristic}
                  />
                )}
              </>
            )}
          </>
        )}
      </TabsContent>
    </>
  );
}
