// ============================================================
// Barrel export for all custom SVG icon components.
//
// SIDEBAR ICONS — placed in Sidebar.tsx via SidebarIcon wrapper
// ============================================================
export { SidebarIcon } from "./SidebarIcon";
export type { SidebarIconState } from "./SidebarIcon";
export { SidebarProjectDashboardIcon } from "./SidebarProjectDashboardIcon";
export { SidebarDataImportIcon } from "./SidebarDataImportIcon";
export { SidebarDataCleansingIcon } from "./SidebarDataCleansingIcon";
export { SidebarDataProfilingIcon } from "./SidebarDataProfilingIcon";
export { SidebarMatchConfigurationIcon } from "./SidebarMatchConfigurationIcon";
export { SidebarMatchDefinitionIcon } from "./SidebarMatchDefinitionIcon";
export { SidebarMatchResultsIcon } from "./SidebarMatchResultsIcon";
export { SidebarMergeSurvivorshipIcon } from "./SidebarMergeSurvivorshipIcon";
export { SidebarFinalExportIcon } from "./SidebarFinalExportIcon";
// NEEDS_MANUAL_PLACEMENT: SidebarSettingsIcon — no Settings route exists in the
// pipeline sidebar; suggested location: Sidebar.tsx settings/admin link if added
export { SidebarSettingsIcon } from "./SidebarSettingsIcon";

// ============================================================
// DASHBOARD ICONS
// ============================================================
export { DashboardAddNewDataIcon } from "./DashboardAddNewDataIcon";
// NEEDS_MANUAL_PLACEMENT: DashboardDeleteIcon — identical path data to
// DataCleansingDeleteIcon; use in ProjectManagement delete action button
export { DashboardDeleteIcon } from "./DashboardDeleteIcon";
export { DashboardEditIcon } from "./DashboardEditIcon";
export { DashboardNumericViewIcon } from "./DashboardNumericViewIcon";
export { DashboardSortAscendingIcon } from "./DashboardSortAscendingIcon";
export { DashboardSortDescendingIcon } from "./DashboardSortDescendingIcon";
export { DashboardStandardViewIcon } from "./DashboardStandardViewIcon";

// ============================================================
// GENERAL ICONS
// ============================================================
export { GeneralAddIcon } from "./GeneralAddIcon";
export { GeneralArrowRightIcon } from "./GeneralArrowRightIcon";
// NEEDS_MANUAL_PLACEMENT: GeneralArrowRightHoverIcon — button hover-state variant
// of GeneralArrowRightIcon (diagonal arrow); suggested location: call-to-action
// buttons that use GeneralArrowRightIcon on hover
export { GeneralArrowRightHoverIcon } from "./GeneralArrowRightHoverIcon";
export { GeneralChevronDownIcon } from "./GeneralChevronDownIcon";
export { GeneralChevronUpIcon } from "./GeneralChevronUpIcon";
export { GeneralCloseIcon } from "./GeneralCloseIcon";
export { GeneralDragVerticalIcon } from "./GeneralDragVerticalIcon";
export { GeneralHelpIcon } from "./GeneralHelpIcon";
export { GeneralInfoIcon } from "./GeneralInfoIcon";
export { GeneralRetryIcon } from "./GeneralRetryIcon";
export { GeneralMenuIcon } from "./GeneralMenuIcon";
export { GeneralNotificationDefaultIcon } from "./GeneralNotificationDefaultIcon";
export { GeneralNotificationActiveIcon } from "./GeneralNotificationActiveIcon";
export { GeneralSearchIcon } from "./GeneralSearchIcon";
// NEEDS_MANUAL_PLACEMENT: GeneralSelectIcon — cursor/pointer icon; identical path
// data to DataImport/Select.svg; suggested location: select-mode toolbar button
// in DataCleansing canvas or MatchResults table
export { GeneralSelectIcon } from "./GeneralSelectIcon";
export { GeneralSuccessIcon } from "./GeneralSuccessIcon";
export { GeneralWarningIcon } from "./GeneralWarningIcon";

// ============================================================
// DATA IMPORT ICONS
// ============================================================
export { DataImportAuthenticationIcon } from "./DataImportAuthenticationIcon";
export { DataImportColumnsIcon } from "./DataImportColumnsIcon";
export { DataImportConnectIcon } from "./DataImportConnectIcon";
export { DataImportDatabaseIcon } from "./DataImportDatabaseIcon";
export { DataImportExceptionIcon } from "./DataImportExceptionIcon";
export { DataImportHelpIcon } from "./DataImportHelpIcon";
export { DataImportMsSqlServerIcon } from "./DataImportMsSqlServerIcon";
export { DataImportMySqlIcon } from "./DataImportMySqlIcon";
export { DataImportPasswordHiddenIcon } from "./DataImportPasswordHiddenIcon";
export { DataImportPasswordViewIcon } from "./DataImportPasswordViewIcon";
export { DataImportPreviewIcon } from "./DataImportPreviewIcon";
// NEEDS_MANUAL_PLACEMENT: DataImportRadioEmptyIcon / DataImportRadioFillIcon —
// custom radio button visuals; suggested location: DataImport source-type selector
// or any radio group that needs custom styling
export { DataImportRadioEmptyIcon } from "./DataImportRadioEmptyIcon";
export { DataImportRadioFillIcon } from "./DataImportRadioFillIcon";
export { DataImportRemoveFilesIcon } from "./DataImportRemoveFilesIcon";
export { DataImportServerConnectionIcon } from "./DataImportServerConnectionIcon";
export { DataImportSqlIcon } from "./DataImportSqlIcon";
export { DataImportTrustServerCertificateIcon } from "./DataImportTrustServerCertificateIcon";

// ============================================================
// DATA PROFILING ICONS
// ============================================================
export { DataProfilingAddNewPatternIcon } from "./DataProfilingAddNewPatternIcon";
export { DataProfilingAnomaliesIcon } from "./DataProfilingAnomaliesIcon";
export { DataProfilingCustomIcon } from "./DataProfilingCustomIcon";
export { DataProfilingDefaultIcon } from "./DataProfilingDefaultIcon";

// ============================================================
// DATA CLEANSING ICONS
// ============================================================
export { DataCleansingZeroToOIcon } from "./DataCleansingZeroToOIcon";
export { DataCleansingAddressParserIcon } from "./DataCleansingAddressParserIcon";
export { DataCleansingNameParserIcon } from "./DataCleansingNameParserIcon";
export { DataCleansingCloseRulesLibraryIcon } from "./DataCleansingCloseRulesLibraryIcon";
export { DataCleansingConfigureIcon } from "./DataCleansingConfigureIcon";
export { DataCleansingDataSourceIcon } from "./DataCleansingDataSourceIcon";
// NEEDS_MANUAL_PLACEMENT: DataCleansingDeleteIcon — identical path data to
// DashboardDeleteIcon; suggested location: node/rule delete button in the
// DataCleansing canvas toolbar
export { DataCleansingDeleteIcon } from "./DataCleansingDeleteIcon";
export { DataCleansingDocumentConfigurationIcon } from "./DataCleansingDocumentConfigurationIcon";
export { DataCleansingFindAndRemoveIcon } from "./DataCleansingFindAndRemoveIcon";
export { DataCleansingFindAndReplaceIcon } from "./DataCleansingFindAndReplaceIcon";
export { DataCleansingLockedIcon } from "./DataCleansingLockedIcon";
export { DataCleansingMaximizeIcon } from "./DataCleansingMaximizeIcon";
export { DataCleansingMinimizeIcon } from "./DataCleansingMinimizeIcon";
export { DataCleansingOToZeroIcon } from "./DataCleansingOToZeroIcon";
export { DataCleansingOpenRulesLibraryIcon } from "./DataCleansingOpenRulesLibraryIcon";
// NEEDS_MANUAL_PLACEMENT: DataCleansingPlayIcon — identical path data to
// DataCleansingRunIcon; both are play/run triangle icons; suggested location:
// use DataCleansingRunIcon for the "Run" toolbar button and DataCleansingPlayIcon
// as an alias or for a secondary play action
export { DataCleansingPlayIcon } from "./DataCleansingPlayIcon";
export { DataCleansingRegexaIcon } from "./DataCleansingRegexaIcon";
export { DataCleansingRemoveExtraSpacesIcon } from "./DataCleansingRemoveExtraSpacesIcon";
export { DataCleansingRemoveLettersIcon } from "./DataCleansingRemoveLettersIcon";
export { DataCleansingRemoveNumbersIcon } from "./DataCleansingRemoveNumbersIcon";
export { DataCleansingRemoveSymbolsIcon } from "./DataCleansingRemoveSymbolsIcon";
export { DataCleansingRunIcon } from "./DataCleansingRunIcon";
export { DataCleansingToLowercaseIcon } from "./DataCleansingToLowercaseIcon";
export { DataCleansingToReverseCaseIcon } from "./DataCleansingToReverseCaseIcon";
export { DataCleansingToTitleCaseIcon } from "./DataCleansingToTitleCaseIcon";
export { DataCleansingToUppercaseIcon } from "./DataCleansingToUppercaseIcon";
export { DataCleansingDuplicateFieldsIcon } from "./DataCleansingDuplicateFieldsIcon";
export { DataCleansingMergeFieldIcon } from "./DataCleansingMergeFieldIcon";
export { DataCleansingTransformIcon } from "./DataCleansingTransformIcon";
export { DataCleansingTrimLeadingSpacesIcon } from "./DataCleansingTrimLeadingSpacesIcon";
export { DataCleansingTrimSpacesIcon } from "./DataCleansingTrimSpacesIcon";
export { DataCleansingTrimTrailingSpacesIcon } from "./DataCleansingTrimTrailingSpacesIcon";
export { DataCleansingUnlockedIcon } from "./DataCleansingUnlockedIcon";
export { DataCleansingVocabularyGovernanceIcon } from "./DataCleansingVocabularyGovernanceIcon";
export { DataCleansingZipParserIcon } from "./DataCleansingZipParserIcon";
export { DataCleansingZoomInIcon } from "./DataCleansingZoomInIcon";
export { DataCleansingZoomOutIcon } from "./DataCleansingZoomOutIcon";

// ============================================================
// MATCH CONFIGURATION ICONS
// ============================================================
export { MatchConfigurationAllIcon } from "./MatchConfigurationAllIcon";
export { MatchConfigurationBetweenIcon } from "./MatchConfigurationBetweenIcon";
export { MatchConfigurationStartMappingIcon } from "./MatchConfigurationStartMappingIcon";
export { MatchConfigurationWithinIcon } from "./MatchConfigurationWithinIcon";

// ============================================================
// MATCH DEFINITION ICONS
// ============================================================
export { MatchDefinitionAutoMapIcon } from "./MatchDefinitionAutoMapIcon";
export { MatchDefinitionFieldMappingIcon } from "./MatchDefinitionFieldMappingIcon";
export { MatchDefinitionMatchButtonIcon } from "./MatchDefinitionMatchButtonIcon";

// ============================================================
// MATCH RESULTS ICONS
// Note: MatchResultsMsSqlServerIcon and MatchResultsServerConnectionIcon have
// identical path data to DataImportMsSqlServerIcon and DataImportServerConnectionIcon.
// Use the DataImport variants directly where needed in Match Results.
// ============================================================
export { MatchResultsCompactViewOffIcon } from "./MatchResultsCompactViewOffIcon";
export { MatchResultsCompactViewOnIcon } from "./MatchResultsCompactViewOnIcon";
export { MatchResultsGroupsIcon } from "./MatchResultsGroupsIcon";
export { MatchResultsPairsIcon } from "./MatchResultsPairsIcon";
export { MatchResultsStatsIcon } from "./MatchResultsStatsIcon";

// ============================================================
// MERGE & SURVIVORSHIP ICONS
// ============================================================
export { MergeSurvivorshipDoubleTapIcon } from "./MergeSurvivorshipDoubleTapIcon";

// ============================================================
// FINAL EXPORT ICONS
// ============================================================
export { FinalExportDuplicateExportModeIcon } from "./FinalExportDuplicateExportModeIcon";
export { FinalExportDuplicateHandlingIcon } from "./FinalExportDuplicateHandlingIcon";
export { FinalExportMasterRecordIcon } from "./FinalExportMasterRecordIcon";
export { FinalExportRowSelectionBehaviorIcon } from "./FinalExportRowSelectionBehaviorIcon";
