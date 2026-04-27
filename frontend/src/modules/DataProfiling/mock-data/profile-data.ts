export const profileData = {
  qualityScore: [
    { name: "Complete", value: 85, color: "#5a189a" },
    { name: "Missing", value: 10, color: "#dbc9ff" },
    { name: "Invalid", value: 5, color: "#f6e5ff" },
  ],

  fieldConsistency: [
    { field: "customer_id", consistency: 92 },
    { field: "email", consistency: 88 },
    { field: "phone", consistency: 85 },
    { field: "address", consistency: 78 },
    { field: "purchase data", consistency: 90 },
    { field: "customer_id", consistency: 92 },
    { field: "email", consistency: 88 },
    { field: "phone", consistency: 85 },
    { field: "address", consistency: 78 },
    { field: "purchase data", consistency: 90 },
  ],

  dataTypeDistribution: [
    { name: "String", value: 45, color: "#5a189a" },
    { name: "Date/Time", value: 15, color: "#7b2cbf" },
    { name: "Double", value: 20, color: "#b793ff" },
    { name: "Integer", value: 12, color: "#dbc9ff" },
    { name: "Boolean", value: 8, color: "#e0aaff" },
  ],

  dataValidity: [
    { name: "Address", valid: 750, invalid: 120 },
    { name: "City", valid: 820, invalid: 50 },
    { name: "State", valid: 840, invalid: 30 },
    { name: "ZIP", valid: 780, invalid: 90 },
    { name: "Contact Name", valid: 700, invalid: 170 },
    { name: "Phone", valid: 650, invalid: 220 },
    { name: "Birthdate", valid: 720, invalid: 150 },
    { name: "Email", valid: 680, invalid: 190 },
  ],

  statisticalSummary: [
    {
      field: "Customer ID",
      min: 1000,
      q1: 2500,
      median: 5000,
      q3: 7500,
      max: 10000,
    },
    { field: "Age", min: 18, q1: 25, median: 35, q3: 45, max: 75 },
    {
      field: "Purchase Amount",
      min: 10,
      q1: 50,
      median: 120,
      q3: 250,
      max: 1000,
    },
    { field: "Order Count", min: 1, q1: 3, median: 7, q3: 15, max: 50 },
  ],

  characterComposition: [
    {
      field: "Company Name",
      letters: 80,
      numbers: 5,
      punctuation: 10,
      special: 5,
    },
    {
      field: "Customer ID",
      letters: 0,
      numbers: 100,
      punctuation: 0,
      special: 0,
    },
    {
      field: "Email Address",
      letters: 70,
      numbers: 10,
      punctuation: 15,
      special: 5,
    },
    {
      field: "Phone Number",
      letters: 0,
      numbers: 85,
      punctuation: 15,
      special: 0,
    },
  ],

  dataUniqueness: [
    { name: "Address", distinct: 850, total: 870 },
    { name: "City", distinct: 120, total: 870 },
    { name: "State", distinct: 50, total: 870 },
    { name: "ZIP", distinct: 350, total: 870 },
    { name: "Contact Name", distinct: 860, total: 870 },
    { name: "Phone", distinct: 865, total: 870 },
    { name: "Birthdate", distinct: 500, total: 870 },
    { name: "Email", distinct: 868, total: 870 },
  ],

  outlierDetection: [
    { name: "Address", value: 120 },
    { name: "City", value: 50 },
    { name: "State", value: 30 },
    { name: "ZIP", value: 90 },
    { name: "Phone", value: 220 },
    { name: "Email", value: 190 },
  ],

  patternClassification: [
    {
      name: "Missing First Names",
      size: 100,
      color: "#5a189a",
      value: "40%",
    },
    {
      name: "Missing Dates of Birth",
      size: 80,
      color: "#7b2cbf",
      value: "30%",
    },
    {
      name: "Age Outliers",
      size: 50,
      color: "#7924cb",
      value: "20%",
    },
    {
      name: "High Uniqueness",
      size: 80,
      color: "#a84fff",
      value: "80%",
    },
    {
      name: "Low Uniqueness",
      size: 40,
      color: "#b793ff",
      value: "40%",
    },
    {
      name: "Invalid Phone Numbers",
      size: 60,
      color: "#dbc9ff",
      value: "60%",
    },
    {
      name: "Invalid Email Formats",
      size: 70,
      color: "#e0aaff",
      value: "70%",
    },
    {
      name: "Missing Email Addresses",
      size: 30,
      color: "#7b2cbf",
      value: "30%",
    },
    {
      name: "Outlier Transaction Amounts",
      size: 45,
      color: "#5a189a",
      value: "45%",
    },
  ],

  entropy: [
    { field: "customer_id", consistency: 92 },
    { field: "email", consistency: 88 },
    { field: "phone", consistency: 85 },
    { field: "address", consistency: 78 },
    { field: "purchase data", consistency: 90 },
    { field: "purchase data", consistency: 90 },
    { field: "customer_id", consistency: 92 },
    { field: "email", consistency: 88 },
    { field: "phone", consistency: 85 },
    { field: "address", consistency: 78 },
    { field: "purchase data", consistency: 90 },
    { field: "purchase data", consistency: 90 },
  ],
};
