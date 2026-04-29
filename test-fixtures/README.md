# Test fixtures

Sample data files for manual smoke-testing the upload + profile flow.

| File | Rows | Columns | Notes |
|---|---|---|---|
| `sample-5-rows.csv` | 5 | 5 | Smallest CSV for sanity-checking the round-trip. |
| `sample-100-rows.csv` | 100 | 8 | Larger CSV with mixed types (string, int, date). |
| `sample-50-rows.xlsx` | 50 | 5 | Single-sheet Excel with currency + numeric columns. |

Generated 2026-04-29 with deterministic seed `42` so re-generation is reproducible
(see git history of this folder for the generator commands).

These are NOT consumed by an automated test suite yet — they exist so that any
human can drop them into the upload UI and verify the column-profiling
pipeline end-to-end.
