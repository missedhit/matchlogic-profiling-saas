/**
 * Centralized brand and chart color constants.
 *
 * Rules:
 * - Only define colors that are actually used as hardcoded hex strings in TSX/TS files.
 * - Do NOT use these in Tailwind className strings — use Tailwind tokens there.
 * - SVG paint attributes (stroke=, fill=) must use JS constants, not CSS variables.
 */

// ---------------------------------------------------------------------------
// Primary brand palette
// ---------------------------------------------------------------------------
export const BRAND_PRIMARY = "#5A189A";
export const BRAND_PRIMARY_LIGHT = "#FBF5FF"; // iris-mist

// Purple scale used in charts and diagrams
export const BRAND_PURPLE_MID = "#7B2CBF"; // mid purple
export const BRAND_PURPLE_MID_GRADIENT = "#7B2FBE"; // gradient start stop in DatasourceNode
export const BRAND_PURPLE_MID_ALT = "#7924CB"; // node background variant
export const BRAND_PURPLE_SOFT = "#DBC9FF"; // soft-lilac
export const BRAND_PURPLE_PALE = "#E0AAFF"; // lightest chart purple
export const BRAND_PURPLE_TINT = "#B793FF"; // mid-light chart purple
export const BRAND_PURPLE_BRIGHT = "#A84FFF"; // bright accent purple
export const BRAND_PURPLE_LIGHT_BG = "#E9D5FF"; // light bg (Tailwind purple-200)
export const BRAND_PURPLE_FAINT = "#F6E5FF"; // faintest purple bg

// Secondary / gradient stop
export const BRAND_SECONDARY = "#9333EA"; // purple-600 — used as gradient end stop

// Deep purples (used in DataProfiling pie charts)
export const BRAND_PURPLE_DEEP = "#4C1D95"; // Tailwind violet-900
export const BRAND_PURPLE_VIOLET = "#6D28D9"; // Tailwind violet-700
export const BRAND_PURPLE_VIOLET_LIGHT = "#C4B5FD"; // Tailwind violet-300
export const BRAND_PURPLE_VIOLET_MEDIUM = "#A78BFA"; // Tailwind violet-400

// ---------------------------------------------------------------------------
// Score / confidence colors (used in Recharts cells, confidence bars, etc.)
// ---------------------------------------------------------------------------
export const SCORE_EXCELLENT = "#10B981"; // emerald-500
export const SCORE_HIGH = "#34D399"; // emerald-400
export const SCORE_GOOD = "#84CC16"; // lime-400
export const SCORE_MODERATE = "#F59E0B"; // amber-400
export const SCORE_LOW = "#F97316"; // orange-400
export const SCORE_POOR = "#EF4444"; // red-400
export const SCORE_NEEDS_REVIEW = "#F43F5E"; // rose-500

// ---------------------------------------------------------------------------
// Chart legend / series colors
// ---------------------------------------------------------------------------
/** "Max Score" series in the match results legend */
export const CHART_MAX_SCORE = "#2196F3";

/** Info/secondary blue (used in anomaly severity icons and AnomaliesSection) */
export const CHART_INFO_BLUE = "#3B82F6"; // blue-500

/** Gradient from info blue — used in AnomaliesSection icon */
export const CHART_INFO_BLUE_DARK = "#2563EB"; // blue-600

/** Definition color palette — cycled when rendering multiple match definitions */
export const DEF_COLORS = [
  BRAND_PRIMARY,
  CHART_INFO_BLUE,
  SCORE_EXCELLENT,
  SCORE_MODERATE,
  SCORE_POOR,
  "#8B5CF6", // violet-500
  "#06B6D4", // cyan-500
  "#EC4899", // pink-500
] as const;

/** Source color palette — cycled for data-source dot indicators */
export const SOURCE_COLORS = [
  BRAND_PRIMARY,
  CHART_INFO_BLUE_DARK,
  "#059669", // emerald-600
  "#D97706", // amber-600
  "#DC2626", // red-600
  "#7C3AED", // violet-600
  "#0891B2", // cyan-600
  "#CA8A04", // yellow-600
] as const;

/** Returns the color for a data-source indicator at the given index (cycles through SOURCE_COLORS). */
export function getSourceColor(index: number): string {
  return SOURCE_COLORS[index % SOURCE_COLORS.length];
}

// ---------------------------------------------------------------------------
// Anomaly severity
// ---------------------------------------------------------------------------
export const ANOMALY_WARNING_BG = "#E59400"; // amber-ish, severity=Warning
/** Amber-600 — used as gradient end stop in AnomaliesSection warning icon */
export const SCORE_MODERATE_DARK = "#D97706"; // amber-600
// ANOMALY_INFO_BG uses CHART_INFO_BLUE

// ---------------------------------------------------------------------------
// Diagram / SVG specific
// ---------------------------------------------------------------------------
/** Inactive edge color in NetworkDiagram */
export const DIAGRAM_INACTIVE_EDGE = "#D4D4D8"; // zinc-300

// ---------------------------------------------------------------------------
// Chart infrastructure
// ---------------------------------------------------------------------------
/** CartesianGrid stroke */
export const CHART_GRID = "#E2E8F0"; // slate-200
/** X-axis tick label */
export const CHART_AXIS_TEXT = "#64748B"; // slate-500
/** Y-axis tick label (lighter) */
export const CHART_AXIS_TEXT_LIGHT = "#94A3B8"; // slate-400
/** Fallback fill when a band key is not in the color map */
export const CHART_FALLBACK = "#6B7280"; // gray-500
/** Recharts default placeholder fill — used as Brush stroke */
export const CHART_BRUSH_STROKE = "#8884D8";
/** Gauge background arc */
export const GAUGE_TRACK = "#F1F5F9"; // slate-100

// ---------------------------------------------------------------------------
// Semantic UI colors — used in QuickActions and GlobalStatsRow icon chips
// ---------------------------------------------------------------------------
export const SEMANTIC_BLUE = "#2563EB";
export const SEMANTIC_BLUE_BG = "#EFF6FF";
export const SEMANTIC_GREEN = "#059669";
export const SEMANTIC_GREEN_BG = "#ECFDF5";
export const SEMANTIC_AMBER = "#D97706";
export const SEMANTIC_AMBER_BG = "#FFFBEB";

// ---------------------------------------------------------------------------
// DefinitionTabs filter chip colors
// ---------------------------------------------------------------------------
export const FILTER_ALL = "#6B7280"; // gray-500
export const FILTER_EXCELLENT = "#15803D"; // green-700
export const FILTER_HIGH = "#16A34A"; // green-600
export const FILTER_GOOD = "#65A30D"; // lime-600
export const FILTER_MODERATE = "#D97706"; // amber-600
export const FILTER_LOW = "#EA580C"; // orange-600
export const FILTER_POOR = "#DC2626"; // red-600
