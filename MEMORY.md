# MatchLogic Profiling SaaS — Project Memory

> Deep reference + persistent log. **CLAUDE.md** is the slim, always-loaded operating manual; **this file** is the deep reference Claude reads on demand. Original implementation plan (single source of truth for the why): `~/.claude/plans/i-want-you-to-steady-mist.md`.

---

## §1 — When to update this file

- **After any architectural change** — new endpoint, new MediatR feature, schema/entity change, new background job, dependency upgrade.
- **After any user correction** — append a Lessons Learned entry.
- **After any non-obvious solution that worked** — record what + why.
- **After completing any milestone (M1, M2, …)** — promote scope from "Pending" to "Done", update Change Log.
- **Always use ISO dates (YYYY-MM-DD).** Append newest entries to the **top** of Change Log + Lessons Learned.

---

## §2 — What this product is (recap)

A free public lead-gen tool — prospects sign up, upload one or more CSV/Excel files (≤1000 records lifetime per account, atomic two-phase enforcement), run column profiling + analytics (type detection, null/uniqueness/entropy stats, pattern discovery, validity scoring), optionally export the profile as CSV or JSON. Hosted on AWS at `app.profiler.matchlogic.io` (FE) + `api.profiler.matchlogic.io` (API). Single-page experience.

**The full MatchLogic matching/cleansing/survivorship pipeline stays gated** behind sales conversations in the main product. This SaaS is a top-of-funnel converter, not a product-light.

---

## §3 — Provenance & Phase A history

### Origin
Forked from the main MatchLogic codebase on **2026-04-27**. Both source repos tagged `saas-fork-2026-04-27` at the divergence commits.

| Side | Source repo | Source branch | Source commit | Source tag |
|---|---|---|---|---|
| Backend | `github.com/MatchLogic/MatchLogicBackend` | `feat/refreshDataSource` | `78e6548` | `saas-fork-2026-04-27` |
| Frontend | `github.com/MatchLogic/MatchLogicFrontend` | `testing` | `317e1372` | `saas-fork-2026-04-27` |

Phase A extraction was performed locally on `saas-extract` branches in those repos (BE commit `f135079`, FE commit `94b3b5c0`). Those branches are local-only and were never pushed. They exist as reference checkpoints; once you've read this MEMORY.md, you don't need them.

**No merges back.** Cherry-pick critical bug fixes one-way only (main → SaaS).

### What Phase A did

Phase A was a destructive extraction: **delete everything not needed for profiling, prove the surviving slice compiles**. No new code was written. The kill list (§4 below) was executed against both repos, all references were patched, and `dotnet build` (BE) + `tsc --noEmit` + `npm run build` (FE) all succeed cleanly.

Phase B was the file copy + scaffold into this new repo, plus three independent verification agents that audited the result. Critical findings from those agents have already been applied (see Change Log §15).

---

## §4 — Kill list (what was removed, why)

### Backend kills

**Endpoint files** (`backend/src/MatchLogic.Api/Endpoints/`) — 14 deleted, 8 kept:

| Killed | Reason |
|---|---|
| `DataCleansingEndpoints.cs` | Not part of profiling-only product |
| `DictionaryCategoryEndpoints.cs` | Cleansing dependency |
| `FinalExportEndpoints.cs` | Matching-pipeline export, not profile export |
| `IdentityEndpoints.cs` | Replaced by Cognito |
| `LicenseEndpoints.cs` | SaaS uses quota, not licenses |
| `LiveSearchEndpoints.cs` | Different product mode |
| `MatchConfigutaionEndpoints.cs` *(typo intentional)* | Matching-pipeline |
| `MatchDefinitionEndpoints.cs` | Matching-pipeline |
| `MatchResultEndpoints.cs` | Matching-pipeline |
| `OAuthEndpoints.cs` | Cloud storage, not in scope |
| `RemoteStorageEndpoints.cs` | Cloud storage, not in scope |
| `SchedulerEndpoints.cs` | Workflow scheduler, not in scope |
| `SurvivorshipEndpoints.cs` | Master-record/merge, not in scope |
| `WordSmithDictionaryEndpoints.cs` | Cleansing dependency |

**Kept:** `ApiVersionEndpoints`, `ColumnNotesEndpoints`, `DataImportEndpoints`, `DataProfilingEndpoints`, `HealthCheckEndpoints`, `JobStatusEndpoint`, `ProjectEndpoints`, `RegexInfoEndpoints`.

**Other backend kills:**
- `MatchLogic.Setup`, `MatchLogic.Setup.Desktop`, `MatchLogic.Setup.Server` — WiX installer projects (no on-prem install)
- `MatchLogic.Infrastructure/Identity/` (entire folder) — Keycloak auth code
- `MatchLogic.Infrastructure/Licensing/` (entire folder) — License validation code
- `MatchLogic.Api/Configurations/IdentitySetup.cs`, `RbacSetup.cs` — Keycloak setup helpers
- Startup.cs `useLightweightAuth`/Desktop branching, Keycloak `AddIdentitySetup`, `AddRbacSetup`, OperationMode-based endpoint gating, Hangfire dashboard, `LicenseMiddleware`
- Program.cs Mutex + named-pipe + browser-auto-open + IIS hosting paths

### Frontend kills

**Module folders** (deleted from `frontend/src/modules/` and corresponding `frontend/src/app/` route folders):
- `DataCleansing` · `MatchConfiguration` · `MatchDefinitions` · `MatchResults` · `MergeAndSurvivorship` · `FinalExport` · `WorkflowScheduler`

**Auth/license layer:**
- `providers/keycloak-provider.tsx`
- `lib/keycloak.ts`, `lib/auth-config.ts`, `lib/auth-token.ts`
- `store/licenseSlice.ts`, `store/remoteConnectionsSlice.ts`
- `hooks/use-license-status.ts`, `hooks/use-pipeline-status.ts`, `hooks/use-next-step.ts`, `hooks/use-unsaved-changes.ts`, `hooks/use-remote-connection-state.ts`
- `components/common/LicenseBanner.tsx`, `LicenseBlockOverlay.tsx`, `ActivateLicenseModal.tsx`, `NextStepBanner.tsx`, `ActiveJobBanner.tsx`, `RunNotificationBell.tsx`, `UnsavedChangesDialog.tsx`, `ftp-connection-form.tsx`, `remote-update-checker.tsx`

**DataImport non-file connection components:**
- `database-connection-area.tsx` · `ftp-connection-area.tsx` · `s3-connection-area.tsx` · `azure-blob-connection-area.tsx` · `oauth-connection-area.tsx` · `remote-file-browser.tsx` · `import-options-container.tsx`
- `modules/DataImport/components/select-table.tsx` (Excel multi-sheet picker — see Pending §13.1)
- `modules/DataImport/hooks/remote/` (entire folder), `services/` (entire folder)

**ProjectManagement:**
- `components/dashboard/` (entire subfolder — used pipeline-status hooks)
- `hooks/use-project-enrichment.ts`, `utils/pipeline-stage.ts`

**Other:** `utils/purge-module-state.ts`, `test/test-utils.tsx`, `store/__tests__/remoteConnectionsSlice.test.ts`, `app/auth/`, `app/oauth/`, `app/data-import/select-table/`

### Stubbed (TODO markers for M1+)

Files where structure was preserved but Keycloak/license logic was replaced with TODO stubs:
- `frontend/src/app/providers.tsx` — drop KeycloakProvider, License layers
- `frontend/src/utils/apiFetch.ts` — no token injection, no 402 license handling
- `frontend/src/hooks/use-auth.ts`, `use-permission.ts` — return defaults
- `frontend/src/providers/route-guard-provider.tsx` — drop pipeline-stage redirects, license blocking
- `frontend/src/components/common/Sidebar.tsx`, `Header.tsx` — slim Profiling-only navigation
- `backend/src/MatchLogic.Api/Startup.cs` — bare `AddAuthentication()`, no JWT bearer yet
- `backend/src/MatchLogic.Api/Program.cs` — request body still `long.MaxValue` (TODO M4: clamp to 50 MB)

Search for `TODO (M1` / `TODO (M2` / `TODO (M4` / `TODO (M5` to find all phase-gates.

---

## §5 — Architecture

### High-level

```
[Visitor]
    │
    ▼
CloudFront + WAF (CommonRuleSet + KnownBadInputs + AnonymousIpList + rate rules)
    ├─→ S3 (Next.js static export) ........... app.profiler.matchlogic.io
    └─→ ALB → ECS Fargate (.NET 8 API) ........ api.profiler.matchlogic.io
                │
                ├─→ Cognito User Pool (OTP, JWT)
                ├─→ MongoDB Atlas M10 (primary store + Hangfire)
                ├─→ S3 bucket (uploaded files, 7d lifecycle)
                ├─→ ElastiCache Redis (per-IP / per-account token buckets)
                └─→ Hangfire in-process (profile jobs + AccountCleanupJob daily)
```

### Stack rationale

| Decision | Choice | Why |
|---|---|---|
| Backend stack | Lift .NET 8 as-is | Battle-tested CSV/Excel readers + AdvancedProfiling analyzers + IDataStore + Hangfire. No rewrite risk. |
| Compute | ECS Fargate, 1 task warm + auto-scale to 3 | I/O-bound profiling at 1000-row cap finishes in <10s. Fargate avoids Lambda cold-start tax. |
| Routing | CloudFront → ALB → Fargate (no API Gateway) | API Gateway hop adds duplicate WAF cost. CloudFront alone is enough. |
| Data store | MongoDB Atlas M10 | Codebase is `MongoDB.Driver` 3.5.2 native. DocumentDB/DynamoDB rejected (operator gaps / forced schema rework). |
| Auth | AWS Cognito User Pools | Native OTP via SES, JWT, free up to 50k MAU, IAM-integrated. Frontend `CognitoProvider` shim exposes Keycloak-shaped surface so lifted code is auth-agnostic. |
| Email | Cognito + SES | OTP, NOT magic links (corporate scanners pre-click and burn the token). |
| File storage | S3 (presigned PUT, 7d uploads / 30d results lifecycle) | Replaces local-disk `%COMMONAPPDATA%\MatchLogicApi\Uploads\`. |
| Hostname | `profiler.matchlogic.io` subdomain | Brand association, separate Cognito callback/CORS posture from main product. |
| Region | `us-east-1` | Single region for v1; multi-region deferred. |

### Repo layout (this repo)

```
matchlogic-profiling-saas/
├── backend/                     # .NET 8 API (~9 endpoints, profile-focused)
│   ├── src/
│   │   ├── MatchLogic.Api/
│   │   ├── MatchLogic.Application/
│   │   ├── MatchLogic.Domain/
│   │   └── MatchLogic.Infrastructure/
│   ├── tests/                   # KILLED test files; new SaaS tests land in M1+
│   ├── Directory.Packages.props # NEEDS CLEANUP — see §13.4
│   ├── MatchLogicWebApi.sln
│   └── ...
├── frontend/                    # Next.js 14 static export, single-page experience
│   ├── src/
│   │   ├── app/                 # 6 routes total (project-mgmt, data-import×3, data-profiling, root)
│   │   ├── modules/             # DataImport, DataProfiling, ProjectManagement
│   │   ├── hooks/, providers/, components/, store/, utils/, lib/, models/
│   │   └── assets/              # NEEDS CLEANUP — 40+ dead module icons (§13.5)
│   ├── package.json             # NEEDS CLEANUP — keycloak-js, @xyflow/react, @dnd-kit/* (§13.4)
│   ├── .env.example             # Cognito + Turnstile placeholders
│   └── ...
├── infra/cloudformation/        # SCAFFOLD — templates land in M1
├── docs/
│   ├── ARCHITECTURE.md          # Architectural reference, milestone plan
│   ├── RUNBOOK.md               # Ops procedures (TBD)
│   └── ABUSE-RESPONSE.md        # When AbuseScoringService flags an account
├── .github/workflows/
│   ├── ci.yml                   # Compile-check on push/PR (works now)
│   ├── frontend-deploy.yml      # SCAFFOLD — wires up in M1
│   └── backend-deploy.yml       # SCAFFOLD — wires up in M1
├── README.md
├── CLAUDE.md
└── MEMORY.md                    # ← this file
```

---

## §6 — CQRS / data layer (carried over from MatchLogic)

```
HTTP → Minimal API Endpoint → MediatR.Send → ValidationResultPipelineBehavior → Handler → Repository/Service → Ardalis.Result<T>
```

- Endpoints in `backend/src/MatchLogic.Api/Endpoints/` use `MapGroup("api/{feature}")`.
- All commands/queries implement `IRequest<Result<T>>`.
- Validation via FluentValidation through `ValidationResultPipelineBehavior` runs **before** handlers.
- Mapping via Mapster (NOT AutoMapper).
- `IDataStore` abstraction with auto-discovered `IGenericRepository<T,TId>` implementations (Mongo primary; LiteDB/SQLite/InMemory still present in code but unwired — cleanup §13.4).
- Ardalis.Result envelope: `{ isSuccess, value, status, errors[], validationErrors[] }`. Frontend `apiFetch` extracts `value`.
- Background jobs via Hangfire (with `Hangfire.Mongo` storage). Profile jobs run in-process on the same Fargate task.

### Domain entities (relevant subset)

- **`Project`** — logical workspace; **becomes the SaaS account** (1:1 mapping). Will gain `LastActiveAt: DateTime` in M5.
- **`DataSource`** — uploaded file metadata + column mappings. CSV/Excel only in SaaS.
- **`DataSnapshot`** — immutable data version per import.
- **`ProfileResult`** + **`ColumnProfile`** — profiling output.
- **`ProjectRun`** + **`StepJob`** — async job tracking.

No user-level tenancy code. Scoping is `ProjectId` only — already SaaS-friendly.

### Frontend state shape

Surviving Redux slices (in `frontend/src/store/index.ts`):
- `auth` — placeholder, will load Cognito identity in M1
- `projects` — selected project, view mode, search
- `dataImport` — upload state, column mappings
- `uiState` — loader, sidebar expanded
- `dataProfile` — datasource ID, selected tab, view mode (standard/numeric)
- `urlParams` — URL ↔ Redux sync

Killed: `license`, `remoteConnections`, `dataCleansing`, `matchConfiguration`, `matchDefinitions`, `matchResults`, `mergeSurvivorship`, `finalExport`, `scheduler`.

---

## §7 — Quota enforcement (the abuse linchpin)

Coming in M4. Single source of truth: Mongo `AccountQuota` doc `{ projectId, recordsConsumed, recordsLimit: 1000, reservations[], lastUpdatedAt }`.

**Phase 1 (pre-upload, declared):** stream row-count of CSV/XLSX BEFORE persisting to S3, atomic Mongo `findAndModify` reserves rows, 5-min TTL on reservation.

**Phase 2 (post-parse, truthful):** compare actual rows to reserved. If actual ≤ reserved, refund difference. If actual > reserved (user lied), rollback DataSource + S3 object, score `+20`, return 400.

**Enforcement:** integration test scans handler dependencies — any `UploadFileHandler` / `ImportDatasourceHandler` code path that doesn't go through `IQuotaService` fails the build.

---

## §8 — AWS safeguards (M4 + M5)

| Layer | Tool | Where | Notes |
|---|---|---|---|
| Edge / DDoS | CloudFront + WAF managed rule groups | CloudFront distribution | CommonRuleSet, KnownBadInputs, AnonymousIpList, rate-based 200/5min global, 20/5min on `/api/dataimport/File` and `/api/DataProfile/Generate*` |
| Bot challenge | Cloudflare Turnstile | FE on signup + upload, BE filter validates `X-Turnstile-Token` | Free, invisible, no Google tracking |
| Email verification | Cognito-native OTP via SES | 6-digit, 10-min expiry, 5-attempt lockout | NOT magic links (corporate scanners) |
| Disposable email blocklist | Lambda refreshes weekly from open-source list | Pre-Cognito Lambda hook | Block role addresses (`info@`, `admin@`) |
| Quotas | `IQuotaService` two-phase | App layer (Mongo) | 1000 records lifetime; 5 MB / 50k rows per file; 10 uploads/day; 20 profile runs/day |
| Token buckets | ElastiCache Redis sliding window | Backend middleware | Signup 3/hr/IP; upload 5/hr/account, 10/hr/IP; profile run 10/hr/account |
| File safety | Magic-byte sniff + xlsx zip-bomb caps + macro rejection + GuardDuty Malware Protection | Pre-upload streaming + S3 scanning | Reject `.xlsm`/`.xltm`/`.xlsb`; strip formulas, keep cached values; reject `vbaProject.bin` |
| Compute caps | Hangfire job timeout 60s; auto-scale max 3 tasks; `WorkerCount` ≤ 5/task | App config | |
| Budget kill-switch | AWS Budgets → SNS → Lambda → SSM Parameter Store flag | FE polls flag, BE returns 503 when off | Threshold $300/mo; flag `/profiler-saas/feature-flags/uploads-enabled` |
| Abuse scoring | `AbuseScoringService` middleware | Per-request | Datacenter ASN +30, IP cluster signups +20, bot UA +20, >50% 4xx +15, MIME mismatch +10. Score ≥50 soft-suspend; ≥80 hard block |
| Storage hygiene | S3 lifecycle (uploads 7d / results 30d), SSE-S3, block all public, versioning OFF | S3 bucket | IAM scoped with `s3:content-length-range` constraint |
| Account auto-delete | Hangfire daily cron `0 3 * * *` | `AccountCleanupJob` | Cascade Mongo + S3 + Cognito; silent purge per user T&Cs |

See `docs/ABUSE-RESPONSE.md` for runbook when scoring flags an account.

---

## §9 — Sequenced milestones

| M | Scope | Status |
|---|---|---|
| **Phase A** | Extracted from main MatchLogic, kill list executed, BE + FE compile clean | ✅ 2026-04-27 |
| **§13 cleanup** | Dead config sections, packages, icons, file-system artifacts removed; tests folder deleted; rebrand pass | ✅ 2026-04-27 |
| **M1a** | Backend `CognitoJwtSetup` + JwtBearer package + `appsettings.Development.json` placeholders | ✅ 2026-04-27 |
| **Orphan-prune** | Full transitive prune (~225 files) + `ProjectService` and `Startup.cs` rewrites; container surface trimmed to MongoDB+Hangfire+DataProfiling | ✅ 2026-04-27 |
| **M1b** | Frontend `CognitoProvider` shim + signup/login/verify/account pages + `useAuth`/`apiFetch` rewire | ✅ 2026-04-27 |
| **M1c** | Provision Cognito User Pool + Mongo Atlas M10 + plug values into config; smoke-test signup → OTP → login → upload → run profile end-to-end | ✅ 2026-04-28 |
| **M1d** | Dockerfile + ECS Fargate + ALB + CloudFront + ACM + Route 53. Hello-world `/api/HealthCheck` reachable at `api.profiler.matchlogic.io` | ⬜ |
| **M2** | S3 + presigned PUT. `UploadFileHandler` writes to S3. `ImportDatasourceHandler` reads from S3 stream. `DataProfile/GenerateAdvance` runs end-to-end via Hangfire. Test fixtures: 5/100-row CSV, 50-row XLSX. | ⬜ |
| **M3** | New `POST /api/DataProfile/Export/{projectId}/{dataSourceId}` (CSV + JSON). `useExportProfile` hook + UI button. | ⬜ |
| **M4** | WAF + Turnstile + disposable-email Lambda + `IQuotaService` two-phase + Redis token buckets + `AbuseScoringService` + GuardDuty Malware Protection. | ⬜ |
| **M5** | `LastActiveTrackingMiddleware` + `AccountCleanupJob` daily cron + S3 lifecycle (7d/30d) + AWS Budgets→SNS→Lambda kill-switch + CloudWatch dashboards. | ⬜ |

Total ~5 weeks single engineer. See `docs/ARCHITECTURE.md` for milestone-level scope.

---

## §10 — New code yet to write (the M1+ punch list)

### Backend (under `backend/src/`)

| File | Intent | Milestone |
|---|---|---|
| `MatchLogic.Api/Auth/CognitoJwtSetup.cs` | Replace `IdentitySetup`/`RbacSetup`. Wire `Microsoft.AspNetCore.Authentication.JwtBearer` against Cognito JWKS. | M1 |
| `MatchLogic.Infrastructure/Storage/S3FileStorageService.cs` | Replace local-disk uploads. Used by file upload + cleanup. | M2 |
| `MatchLogic.Application/Features/DataProfiling/Export/ExportProfileQuery.cs` + handler | CSV/JSON export of `ProfileResult` + `ColumnProfile[]`. | M3 |
| `MatchLogic.Api/Endpoints/DataProfileExportEndpoint.cs` | New endpoint binding for export query. | M3 |
| `MatchLogic.Application/Services/IQuotaService.cs` + `QuotaService.cs` | Two-phase atomic 1000-row enforcement. | M4 |
| `MatchLogic.Application/Services/AbuseScoringService.cs` | Per-account abuse scoring (ASN, UA, IP cluster, MIME mismatch). | M4 |
| `MatchLogic.Api/Middleware/TurnstileValidationFilter.cs` | Endpoint filter for upload + signup. | M4 |
| `MatchLogic.Api/Middleware/LastActiveTrackingMiddleware.cs` | Debounced project activity heartbeat for inactivity-deletion. | M5 |
| `MatchLogic.Application/Features/Cleanup/AccountCleanupJob.cs` | Hangfire daily cron — cascade Mongo + S3 + Cognito. | M5 |
| `Dockerfile`, `.dockerignore` (root or `backend/`) | Container packaging for ECS Fargate. | M1 |

### Frontend (under `frontend/src/`)

| File | Intent | Milestone |
|---|---|---|
| `lib/cognito.ts` + `providers/cognito-provider.tsx` | Keycloak-shaped Cognito shim. Same `useAuth` / `getAccessToken` surface. | M1 |
| `app/signup/page.tsx`, `app/login/page.tsx`, `app/verify/page.tsx` | Cognito OTP flow. | M1 |
| `app/account/page.tsx` | View quota usage, manual delete account. | M1 |
| `components/common/turnstile-widget.tsx` | Reusable bot challenge widget for signup + upload. | M4 |
| `modules/DataProfiling/hooks/use-export-profile.ts` | Export mutation. | M3 |
| `modules/DataProfiling/components/export-profile-button.tsx` | UI trigger. | M3 |

### Infrastructure (under `infra/cloudformation/`)

| File | Resources | Milestone |
|---|---|---|
| `network.yml` | VPC, public/private subnets, NAT, route tables, SGs | M1 |
| `compute.yml` | ECS cluster, ALB, target group, Fargate service + task def, ECR repo | M1 |
| `data.yml` | S3 bucket (lifecycle), ElastiCache Redis, MongoDB Atlas peering | M1/M4 |
| `auth.yml` | Cognito User Pool, app client, identity pool, SES domain | M1 |
| `edge.yml` | CloudFront (FE + API), WAF web ACLs, ACM cert, Route 53 records | M1/M4 |
| `safeguards.yml` | AWS Budgets, SNS, kill-switch Lambda, SSM Parameter Store, GuardDuty | M5 |

---

## §11 — TODO markers in lifted code

These are intentional phase-gates. Search for `TODO (M1` etc. to surface them:

| File | Line | TODO |
|---|---|---|
| `backend/src/MatchLogic.Api/Startup.cs` | bare-auth scaffold | M1: wire Cognito JWT bearer here |
| `backend/src/MatchLogic.Api/Program.cs` | `MaxRequestBodySize = long.MaxValue` | M4: clamp to 50 MB once edge limits in place |
| `frontend/src/app/providers.tsx` | top of `Providers` | M1: wrap with `CognitoProvider` |
| `frontend/src/utils/apiFetch.ts` | `getAccessToken` stub | M1: re-wire to Cognito ID token |
| `frontend/src/hooks/use-auth.ts` | `logout`/`goToProfile` no-ops | M1: re-wire to Cognito sign-out + account page |
| `frontend/src/providers/route-guard-provider.tsx` | top of provider | M1: replace with single-page provider once auto-create-project on signup wired |
| `frontend/src/components/common/Header.tsx` | account dropdown | M1: wire Cognito-backed dropdown |
| `frontend/src/modules/DataImport/hooks/file/upload-file.ts` | `router.push("/data-import/column-mapping")` | M2: re-add Excel sheet picker (currently jumps straight to column-mapping; CSV always one "sheet"; Excel uses first) |

---

## §12 — Verification status (Phase A → end of Phase B)

### Backend
- ✅ `dotnet build src/MatchLogic.Api/MatchLogic.Api.csproj` → 0 errors, ~1663 warnings (mostly nullability CS8618; pre-existing in main product). Build time ~26s.
- ❌ `dotnet build MatchLogicWebApi.sln` (full solution, including `tests/`) → 24 errors. **Expected** — test files reference deleted endpoint classes. Replace with SaaS-targeted tests in M1+.
- ✅ Endpoint files in `Endpoints/`: exactly the 8 expected.
- ✅ No `MatchLogic.Setup*` projects.
- ✅ Orphaned `Identity/` and `Licensing/` infrastructure folders removed; `IdentityServiceCollectionExtensions`, `LicenseService`, `IdentitySetup`, `RbacSetup` all deleted; `Infrastructure/Startup.cs` `AddLicensingServices()` and `ValidateLicensePublicKey()` removed.

### Frontend
- ✅ `npm install --legacy-peer-deps` succeeds (React 19 / Next 14.0.3 mismatch — known).
- ✅ `npx tsc --noEmit` → 0 errors.
- ✅ `npm run build` → 6 routes generated as static content (`/`, `/data-import`, `/data-import/column-mapping`, `/data-import/data-sources`, `/data-profiling`, `/project-management`). Plus auto `/_not-found` and `/icon.svg`.
- ✅ Modules: exactly `DataImport`, `DataProfiling`, `ProjectManagement`.
- ✅ All 15 forbidden component/hook/lib files confirmed gone.
- ✅ `/data-import/select-table` 404 risks fixed (3 `router.push` redirects updated to `/data-import/column-mapping`).

### npm audit at clone time
- ⚠️ 12 vulnerabilities (3 moderate, 8 high, 1 critical) reported by `npm install`. Run `npm audit fix` (without `--force` first) before launch. `next@14.0.3` has a CVE — bump to latest 14.x patch in M1.

---

## §13 — Pre-launch cleanup (deferred from Phase B)

These are real but non-blocking. Tackle early in M1 before any new feature work — they reduce surface area for confusion.

### §13.1 — Excel sheet picker (functional)

The original `select-table` page handled Excel multi-sheet selection. Killed during extraction. Currently both CSV and XLSX upload jumps straight to `/data-import/column-mapping`, which assumes the first sheet for Excel.

**Action (M2):** Re-add a sheet picker. Either as a new `select-sheet` page (file-only, simpler than the original) OR as a dropdown at the top of the column-mapping page that re-fires the columns query. The latter is preferred — fewer routes.

### §13.2 — appsettings cleanup (configurational hygiene)

`backend/src/MatchLogic.Api/appsettings.json` still has these dead/desktop-specific sections:
- `Application.LiveSearch` (lines ~7-22, plus large commented variants ~26-69)
- `AppMode` (~70-75) — Desktop/Mutex/Pipe
- `OAuth` (~130-146)
- `Identity` (~147-149)
- `Scheduler` (~150-155)
- `LicenseSettings` (~156-160)
- `FeatureFlags.Cleansing` (~128)

Sibling files `appsettings.Desktop.json`, `appsettings.Server.json`, `appsettings.Production.json` are leftover desktop/on-prem variants. **Delete or reduce to `appsettings.Production.json` only**, with Cognito/S3/Mongo-Atlas SaaS config.

### §13.3 — File-system artifacts (security/hygiene)

In `backend/src/MatchLogic.Api/`:
- `Uploads/` — old uploaded files (scrub before push)
- `Logs/` — local Serilog output
- `matchlogic-license-public.pem` — main-product license public key (no longer needed, remove)
- `web.production.config` — IIS deployment config (Fargate doesn't use IIS)

### §13.4 — Package cleanup

**Backend `Directory.Packages.props`** — kill-list packages still listed:
- `Keycloak.AuthServices.Authentication`, `Keycloak.AuthServices.Authorization`
- `LiteDB`, `SQLitePCLRaw.bundle_e_sqlite3` (Mongo-only for SaaS)
- `WixToolset.*` (3 entries; no Setup projects exist)
- `Microsoft.AspNetCore.Identity.EntityFrameworkCore` (Identity killed)
- `FluentFTP`, `SSH.NET`, `AWSSDK.S3` (we'll re-add only `AWSSDK.S3` in M2 for actual S3 use), `Azure.Storage.Blobs`, `Google.Apis.Drive.v3`, `Dropbox.Api`, `Microsoft.Graph`
- `Phonix`, `SimMetrics.Net`, `FuzzySharp`, `TransliterationLibrary`, `Unidecode.NET`, `icu.net`, `MathNet.Numerics`, `Microsoft.ML` — matching/cleansing libs likely orphaned (verify with grep before removing)
- **Duplicate `PackageVersion` entries** for `CsvHelper`, `coverlet.collector`, `FluentAssertions`, `Microsoft.NET.Test.Sdk`, `xunit`, `xunit.runner.visualstudio`, `Microsoft.AspNetCore.Mvc.Testing` — dedupe.

**Frontend `package.json`** — kill-list packages:
- `keycloak-js` (auth replaced by Cognito SDK)
- `@xyflow/react` (was used by killed cleansing flow + scheduler)
- `@dnd-kit/core`, `@dnd-kit/modifiers` (matching-table DnD; verify)
- `popmotion`, `embla-carousel-react`, `react-resizable-panels` — verify usage; likely killed-module-only

### §13.5 — Asset icon cleanup

`frontend/src/assets/icons/` has 40+ icons for killed modules: `DataCleansing*`, `FinalExport*`, `MatchConfiguration*`, `MatchDefinition*`, `MatchResults*`, `MergeSurvivorship*`, plus Sidebar variants. Likely re-exported from `index.ts` and tree-shaken at build, but worth deleting source + index entries.

### §13.6 — Branding rebrand

- `frontend/src/app/layout.tsx:26-27` — page title is "MatchLogic - Data Matching Solution" (rebrand to "MatchLogic Profiler" or similar); description references cleansing+matching.
- `frontend/src/app/layout.tsx:29` — favicon `/logos/ml_favicon.svg` (use profiler-branded icon).
- `frontend/src/components/common/Breadcrumbs.tsx` — entries for killed routes (data-cleansing, match-*, merge-*, final-export, select-table) — remove.
- `frontend/src/providers/route-guard-provider.tsx:28` — `LAST_ROUTE_KEY = "matchlogic_last_routes"` (rebrand to `profiler_last_routes`).
- `frontend/src/components/common/Header.tsx` — `https://help.matchlogic.io/`, `mailto:contact@matchlogic.io` (point to profiler-specific contact, or generic).

### §13.7 — Hangfire dashboard

Currently disabled in `backend/src/MatchLogic.Api/Startup.cs`. Decision for M1: keep disabled (no admin user concept in SaaS) OR re-enable for ops behind IP-whitelist + basic auth. Default: keep disabled.

### §13.8 — Test suite

`backend/tests/` has 24 errors referencing killed endpoints. **Delete the entire `backend/tests/` folder** during M1 and write SaaS-targeted tests:
- Unit tests for `IQuotaService` (M4)
- Integration tests for upload → S3 → profile job (M2)
- Integration test guarding the "every upload path goes through `IQuotaService`" invariant (M4)
- `MatchLogic.Api.IntegrationTests` Testcontainers pattern is reusable.

---

## §14 — Supplementary docs

| Document | Description |
|---|---|
| [`README.md`](README.md) | Overview, build/test instructions |
| [`CLAUDE.md`](CLAUDE.md) | Slim operating manual |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Architectural reference, milestone plan |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | Ops procedures (TBD) |
| [`docs/ABUSE-RESPONSE.md`](docs/ABUSE-RESPONSE.md) | When AbuseScoringService flags an account |
| [`infra/README.md`](infra/README.md) | CloudFormation template scaffold |
| External: `~/.claude/plans/i-want-you-to-steady-mist.md` | Original implementation plan (full reasoning) |

---

## §15 — Change Log

> Append newest entries at the **top**. Format: `YYYY-MM-DD — what changed — why — where (paths)`.

- **2026-04-28** — **M1c — provisioned + smoke-tested end-to-end.** User (non-dev) drove provisioning via AWS + Atlas web consoles (NOT the Terraform/CFN templates from the prior session — those are still valid for M1d but were too dense for this user's first cloud setup). Signup → OTP email → verify → login → create project → upload XLSX → import 2134 rows → run advanced profile → see results, all green. **Live dev resources** (us-east-1, AWS account 274020917421): Cognito user pool `us-east-1_kN55XX1J3`, app client `7shrdvau1keked8jss22flblki` (public SPA, no secret, ALLOW_USER_SRP_AUTH + ALLOW_REFRESH_TOKEN_AUTH); Atlas cluster `profiler-dev.nllu2r.mongodb.net` (M10, project `profiler-saas-dev`, DB user `matchlogic-app`, password rotated to throwaway value, IP allowlist contains user's home IP). Local config dropped into [`frontend/.env.local`](frontend/.env.local) and [`backend/src/MatchLogic.Api/appsettings.Development.json`](backend/src/MatchLogic.Api/appsettings.Development.json) (both gitignored). **Smoke-test journey surfaced multiple latent bugs** that all needed fixing before the loop closed; these are the actual code changes shipped this session: (1) [`backend/src/MatchLogic.Infrastructure/Startup.cs`](backend/src/MatchLogic.Infrastructure/Startup.cs) — replaced reflection-based `AddRepositories` scan (which picked up the open-generic `GenericRepository<,>` itself and crashed DI validation) with explicit registrations: `IJobStatusRepository`, `IDataSourceService`, `IFileImportService`, `ISchemaValidationService`, `IColumnFilter`, `DataImportCommand`, `DataProfilingCommand`, `AdvanceDataProfilingCommand`, `IRecordHasher → SHA256RecordHasher`. (2) [`backend/src/MatchLogic.Api/Startup.cs`](backend/src/MatchLogic.Api/Startup.cs) — registered `ICurrentUser → CurrentUser` (HTTP-context-backed, reads JWT claims for audit fields); registered every `AppPermissions.All()` constant as `RequireAuthenticatedUser` policy (Keycloak-era named policies were referenced by `[RequireAuthorization("projects.read")]` etc but unregistered, causing `'projects.read' was not found` 500s on first authenticated request). (3) [`backend/src/MatchLogic.Infrastructure/Repository/ProfileRepository.cs`](backend/src/MatchLogic.Infrastructure/Repository/ProfileRepository.cs) — rewrote `SaveProfileResultAsync` to buffer all `RowReferenceDocument`s and bulk-insert in one round-trip; the original per-doc `InsertAsync` loop spawned 1000+ network round-trips against Atlas (~300ms WAN each = many minutes). Profile save now drops from 5+ minute hang to ~1 second. (4) [`frontend/src/components/ui/input.tsx`](frontend/src/components/ui/input.tsx) — wrapped Input in `React.forwardRef`; without it, `react-hook-form`'s `register()` couldn't bind to the underlying `<input>` and every form field returned `undefined`. (5) [`frontend/src/app/globals.css`](frontend/src/app/globals.css) — swapped `--destructive` ↔ `--destructive-foreground` (originally `--destructive: #fffbfa` near-white made all error text invisible). (6) [`frontend/next.config.mjs`](frontend/next.config.mjs) — gated `output: "export"` behind `NODE_ENV === "production"`; static-export mode broke dev-server bundling of the Buffer polyfill (browser SyntaxError on layout.js chunk) needed by `amazon-cognito-identity-js` SHA-256 SRP. (7) [`backend/src/MatchLogic.Api/appsettings.Development.json`](backend/src/MatchLogic.Api/appsettings.Development.json) — added `MongoDB:Progress.ConnectionString` (without it, the progress store fell back to `mongodb://localhost:27017` because `MongoDbProgressOptions` inherits a non-empty default that fails the `!IsNullOrEmpty` fallback check in [`MongoDbServiceCollectionExtensions.cs`](backend/src/MatchLogic.Infrastructure/MongoDbServiceCollectionExtensions.cs)). **What's still imperfect (not blocking M1c, parked for next session):** UI styling is rough by user's own description ("UI looked like shit"); the new "quick setup" Cognito wizard's SPA path was tried first but creates an email-alias pool that rejects email-format usernames at signup — had to delete and recreate via the legacy "advanced configuration" wizard with **"Email" as the only sign-in identifier** (NOT email + username, NOT email-as-alias); the new SPA wizard also auto-generated a client secret despite "SPA" selection — had to recreate the app client with the secret toggle explicitly off. Where: `backend/src/MatchLogic.{Api,Infrastructure}/`, `frontend/src/{components/ui,app}`, `frontend/.env.local`, `backend/src/MatchLogic.Api/appsettings.Development.json`. **Not committed yet** — the session ended with everything in working-tree-modified state for the user to review/commit.

- **2026-04-27** — **M1c artifacts written (deploy step deferred).** Provisioning artifacts ready; actual `aws cloudformation deploy` + `terraform apply` deferred because (a) the AWS CLI default profile returned `InvalidClientTokenId` on `aws sts get-caller-identity`, (b) no Atlas org ID / API keys are available in this environment, and (c) M10 Atlas runs ~$57/mo — provisioning is a cost-bearing action that needs explicit user approval. New: [`infra/cloudformation/auth.yml`](infra/cloudformation/auth.yml) (Cognito User Pool with email-as-username, OTP-on-signup verification, password policy 10+ chars/upper/lower/digit/symbol, MFA off, deletion protection on; public SPA App Client with `ALLOW_USER_SRP_AUTH` + `ALLOW_REFRESH_TOKEN_AUTH`, no client secret, 60min access/id token, 30d refresh, `PreventUserExistenceErrors` enabled). New Terraform module under [`infra/terraform/atlas/`](infra/terraform/atlas/) — `versions.tf` (mongodb/mongodbatlas ~> 1.21), `variables.tf` (org_id + project/cluster/region/instance_size + ip_access_list + sensitive db_password), `main.tf` (mongodbatlas_project + REPLICASET cluster M10 in US_EAST_1, mongo 7.0, cloud backup on, auto-scale disk on; database_user with readWrite on `matchlogic` db; project_ip_access_list as for_each map), `outputs.tf`, `terraform.tfvars.example`, `.gitignore`. New: [`docs/M1C-PROVISIONING.md`](docs/M1C-PROVISIONING.md) — operator runbook covering AWS CLI prereqs, Atlas API key generation, deploy commands, output capture, dropping IDs into `frontend/.env.local` + `appsettings.Development.json`, smoke-test walkthrough (signup → OTP → verify → login → /project-management → confirm `Authorization: Bearer <id-token>` reaches API → curl protected endpoint), tear-down on completion (`terraform destroy` to stop M10 billing). Updated [`infra/README.md`](infra/README.md) to reflect what now exists. Updated [`backend/src/MatchLogic.Api/appsettings.Development.json`](backend/src/MatchLogic.Api/appsettings.Development.json) to default `Cognito.Region` to `us-east-1` (architecture-locked). **SES intentionally deferred to launch prep** (not M1c): default Cognito sender (`no-reply@verificationemail.com`, ~50 emails/day cap) is sufficient for smoke testing, and SES requires DNS verification on the matchlogic.io zone + production-access request — both out of M1c scope. **Verification:** `terraform fmt -check -recursive` passes; `terraform init -backend=false && terraform validate` → `Success! The configuration is valid`; `auth.yml` parses as valid CloudFormation YAML (verified via PyYAML with CFN tag constructors registered — Resources: UserPool, AppClient; Outputs: UserPoolId, UserPoolClientId, UserPoolArn, Region). `cfn-lint` not available in this env so deeper schema validation will run when the user executes `aws cloudformation deploy`. Where: `infra/`, `docs/`, `backend/src/MatchLogic.Api/`. **Next blocker for the user:** working AWS credentials + Atlas org ID + Atlas API keys + dev IP CIDR for the allowlist; then walk [M1C-PROVISIONING.md](docs/M1C-PROVISIONING.md).

- **2026-04-27** — **M1b — Frontend Cognito shim + auth pages.** Added `amazon-cognito-identity-js@^6.3.12` to `frontend/package.json`. New: `frontend/src/lib/cognito.ts` (Keycloak-shaped Cognito client wrapping CognitoUserPool/CognitoUser with a module-level `tokens` store + subscribe-pattern; throws `Cognito not configured` if `NEXT_PUBLIC_COGNITO_USER_POOL_ID`/`_CLIENT_ID` are empty so dev failures are loud). New: `frontend/src/providers/cognito-provider.tsx` (restores session on mount, schedules refresh 5 min before id-token expiry, syncs Redux `authSlice` from token-store changes; short-circuits to `setAuthState({ isInitialized: true, isAuthenticated: false })` when pool isn't configured so the splash doesn't hang). New: `frontend/src/components/auth/auth-card.tsx`, `frontend/src/components/common/auth-redirect.tsx`. New auth pages under `frontend/src/app/(auth)/`: `login/`, `signup/`, `verify/` (plus group `layout.tsx` rendering bare centered card). New: `frontend/src/app/(app)/account/page.tsx`. **Restructure:** introduced two route groups under `frontend/src/app/`: `(app)/` for the existing 5 authenticated routes (project-management, data-import × 3, data-profiling) plus the new `/account`, and `(auth)/` for `/login`, `/signup`, `/verify`. Route groups don't change URLs. Moved `frontend/src/app/{project-management,data-import,data-profiling}` into `(app)/` via `git mv`. Sidebar+Header chrome moved out of `app/layout.tsx` into `app/(app)/layout.tsx`; `app/layout.tsx` now wraps `<Providers>{children}</Providers>` only. **Provider rewire:** `app/providers.tsx` now wraps Redux Provider → QueryClientProvider → CognitoProvider; `RouteGuardProvider` and `JobStateProvider` moved into `app/(app)/layout.tsx`. **TODO M1 holes wired (4/4):** `apiFetch.getAccessToken` → imports `getIdTokenSync` from `@/lib/cognito`. `useAuth.logout` → `cognitoSignOut() + dispatch(clearAuth()) + router.push("/login")`; `useAuth.goToProfile` → `router.push("/account")`; `useAuth.authEnabled` flipped to `true`; `useAuth.accountUrl` → `/account`. `route-guard-provider.tsx` reads `useAuth()`, redirects unauthenticated users to `/login`, blocks render until `authInitialized && isAuthenticated`. `Header.tsx` adds an "Account" `DropdownMenuItem` above logout. `app/page.tsx` now renders `<AuthRedirect />` (auth-aware redirect to `/project-management` or `/login`). **`"use client"` added** to `route-guard-provider.tsx` and `job-state-provider.tsx` — necessary because they're now imported into `app/(app)/layout.tsx` (a server component) instead of being nested inside the existing `Providers` client boundary. **Verification:** `npx tsc --noEmit` → 0 errors. `npm run build` → 11 prerendered routes (`/`, `/account`, `/data-import`, `/data-import/column-mapping`, `/data-import/data-sources`, `/data-profiling`, `/login`, `/project-management`, `/signup`, `/verify`, `/_not-found`). `grep -rn "TODO (M1" frontend/src` → 0 matches; only remaining phase-gate is `frontend/src/modules/DataImport/hooks/file/upload-file.ts:92` (`TODO (M2)`, Excel sheet picker — out of M1b scope). Where: `frontend/`. **Smoke test deferred to M1c** when real Cognito pool exists.
- **2026-04-27** — Full-transitive orphan-prune (pre-M1c). Original §15 list (Application/Licensing/, surgical Scheduling/, License+Scheduling handlers, LiveSearchSetup, LicenseMiddleware, ProjectRunCompletedEventHandler) cascaded into a far larger surgery once each delete unblocked the next layer. **Final delete tally: ~225 source files** across all 4 projects. **API:** removed handler folders for `Cleansing`, `DictionaryCategory`, `FinalExport`, `LiveSearch`, `MappedFieldRow`, `MatchConfiguration`, `MatchDefinition`, `MatchResult`, `Survivorship`, `License`, `Scheduling`; deleted `Middleware/LicenseMiddleware.cs`, `Common/Validators/MatchDefinitionDtoValidator.cs`. Slimmed `Configurations/MappingConfig.cs` to only Project/FileImport/RegexInfo Mapster mappings; removed dead `Cleansing` import from `ColumnNotesEndpoints`. **Application:** deleted entire `Features/{CleansingAndStandardization,DataMatching,MergeAndSurvivorship,MatchResult,MatchDefinition,LiveSearch,Export,Transform,Storage,FinalExport}/`; deleted `Interfaces/{Cleansing,CleansingAndStandardization,DataMatching,MergeAndSurvivorship,MatchConfiguration,LiveSearch,Comparator,Phonetics,Dictionary→restored,Export,FinalExport,Persistence/IFieldOverwriteRuleSetRepository,IMasterRecordRuleSetRepository}/`; deleted `EventHandlers/ProjectRunCompletedEventHandler.cs`, `Features/Import/FieldMappingService.cs`, `Interfaces/Scheduling/ISchedulerService.cs`. **Domain:** deleted `Licensing/`, `Scheduling/`, `MergeAndSurvivorship/`, `CleansingAndStandaradization/`, `MatchConfiguration/`, `MatchDefinition/`, `Dictionary→restored/`, `Export/`, `FinalExport/`, `Entities/Common/{ExportTypeResolver,MatchJobInfo,MatchGroup}.cs`. **Infrastructure:** deleted entire `CleansingAndStandardization/`, `Comparator/`, `Phonetics/`, `Dictionary→restored/`, `Export/`, `FinalExport/`, `Scheduling/{SchedulerService,ScheduleRecoveryService,BackgroundServiceScheduler}.cs`, `Configuration/SchedulerSettings.cs`, `Extensions/LiveSearchSetup.cs`, `Persistence/{InMemoryStore,LiteDbDataStore,SqliteDataStore,IndexPersistenceService,MatchGraphStorage}.cs`, `Repository/{FieldOverwriteRuleSetRepository,MasterRecordRuleSetRepository}.cs`, `Project/Commands/{DataCleansingCommand,MatchingCommand,MasterRecordDeterminationCommand,FieldOverwriteCommand,DataExportCommand,FinalExportCommand}.cs`, `Import/PostgresSQLReader.cs`, `Core/Health/SystemResourcesHealthCheck*.cs`, `BackgroundJob/MatchProcessingBackgroundService.cs`. **Major rewrites:** (1) `MatchLogic.Application/Features/Project/ProjectService.cs` collapsed from ~1572 lines to ~370 — dropped 11 repository fields (MatchDefinition/EnhancedCleaningRules/MergeRules/ExportSettings/MatchingDataSourcePairs/MatchDefinitionCollection/FieldOverwriteRuleSet/MasterRecordRuleSet/FinalExportSettings/MappedFieldsRow/AutoMappingService/FieldMappingService), removed RenameDataSource ancillaries, RemoveDataSourceFromMatchingSystem, FieldOverwrite/MasterRecord/FinalExport branches, ScoreBand creation. (2) `MatchLogic.Infrastructure/Startup.cs` rewritten from ~920 lines to ~280 — drops AddSharedMatchingServices/AddBatchServices/AddTransformationServices/AddDataCleansingServices/AddMatchConfiguration/AddWordSmithServices/AddMasterRecordDetermination/AddFieldOverwriting/PreloadFirstNameDictionariesAsync/ProperCaseOptions/AddLiteDbStoreFactory/AddBackgroundServiceScheduler helpers + LiteDb/InMemory branches + matching/comparator/phonetic registrations + operation-mode switch. Now SaaS-only: MongoDB + Hangfire + DataProfiling registrations. **Surviving caller patches:** (3) `DeleteProjectHandler.cs` slimmed — dropped ILicenseService + ISchedulerService deps. (4) `DeleteDataSourceHandler.cs` slimmed — dropped ILicenseService dep. (5) `DataImportCommand.cs` — dropped ILicenseService dep + 3 license-gated blocks; left TODO (M4) markers for IQuotaService two-phase enforcement. (6) `MongoDbDataStore.cs` — dropped `InsertProbabilisticBatchAsync(MatchResult)`, `GroupQueryFilter` parameter from `GetPagedJobWithSortingAndFilteringDataAsync`, `BuildGroupFilters` + `GetBandThresholds` matching helpers from `.Advance.cs`. (7) `MongoDbServiceCollectionExtensions.cs` — dropped InMemoryStore fallback. (8) `IDataStore.cs` — slimmed (dropped probabilistic + GroupQueryFilter API). (9) `Constants.cs` — dropped 21 dead collection-name constants (MatchDefinition/CleaningRules/MergeRules/ExportSettings/MatchDefinitionCollection/MatchDataSourcePairs/MatchSettings/MappedFieldRows/WordSmith*/TransformationGraphs/ProperCaseOptions/FinalExport*/MasterRecordRuleSets/FieldOverwriteRuleSets/ScoreBand/ScheduledTask*/CleansingOperationAttribute/bands/DefaultOptions/Scheduler) — kept SaaS-needed (Projects/ProjectRuns/StepJobs/DataSources/RegexInfo/DictionaryCategory/ImportFile/FieldMapping/DataSourceColumnNotes/DataSnapshots/JobStatus). (10) `IProjectService.cs` slimmed — dropped AddMatchDefinition/RemoveMatchDefinition/UpdateRunStatus/AddCleaningRules/RemoveCleaningRules/AddMergeRules/RemoveMergeRules/AddExportSettings/RemoveExportSettings. (11) `CommandFactory.cs` slimmed to {Import, Profile, AdvanceProfile} only. **Load-bearing types recreated** in stable namespaces (overshoot recovery): `Domain/Entities/Common/FlowStatistics.cs`, `Domain/Entities/Common/FieldMapping.cs` (FieldMapping + FieldMappingEx + FieldOrigin enum), `Domain/Entities/Common/ProjectJobInfo.cs`. **Restored from git** (overshoot recovery — load-bearing for profiling): `Application/Interfaces/Dictionary/IDictionaryCategoryService.cs`, `Domain/Dictionary/DictionaryCategory.cs`, `Infrastructure/Dictionary/DictionaryCategoryService.cs`, `Application/Extensions/GuidCollectionNameConverter.cs`, `Application/Core/DictionaryCategorySeedProvider.cs`. **Verification:** BE `dotnet build src/MatchLogic.Api` → 0 errors / 10 warnings (down from 1031). BE `dotnet build MatchLogicWebApi.sln` → 0 errors. FE `npm run build` → 6 routes generated, untouched by this pass. `IScheduler` registration preserved (line 277 of new Startup.cs) — profile-job dispatch via Hangfire still wired. Where: `backend/src/`. **Notable:** the §15 list (~45 files) cascaded into ~225 file deletes once dependency chains were followed; the wave-by-wave plan structure was abandoned in favor of dependency-driven cascading because deleting Domain.Scheduling immediately broke FinalExportCommand, deleting Application.Features broke ProjectService, etc. — surgically isolating each "wave" would have taken longer than the cascading delete + rewrite. Single commit recommended over per-wave commits.
- **2026-04-27** — §13 cleanup pass + M1a (backend Cognito JWT scaffolding) landed. **§13.2:** stripped dead `Application.LiveSearch`, `AppMode`, `OAuth`, `Identity`, `Scheduler`, `LicenseSettings`, `FeatureFlags.Cleansing` sections from `appsettings.json`; dropped Windows-only Serilog file sink (Fargate uses awslogs driver from console). Changed `StoreSettings.Default` from `LiteDb` → `MongoDB`. **§13.3:** deleted tracked `Uploads/` dir, `Logs/`, `matchlogic-license-public.pem`, `web.production.config`; added `[Uu]ploads/` to `backend/.gitignore`. **§13.4a:** removed `Keycloak.AuthServices.{Authentication,Authorization}`, `Microsoft.AspNetCore.Identity.EntityFrameworkCore`, `WixToolset.{Heat,UI.wixext,Util.wixext}`, duplicate `CsvHelper` from `Directory.Packages.props` and the corresponding `PackageReference` entries in `MatchLogic.Api.csproj` and `MatchLogic.Infrastructure.csproj`. Removed dead `using Microsoft.AspNetCore.Identity.Data;` and pem `Content` ItemGroup from API csproj. **§13.8:** deleted `backend/tests/` (24 files referencing killed endpoints) and removed projects from `MatchLogicWebApi.sln`. **§13.4a part 2:** rewrote `Directory.Packages.props` cleanly — dropped all test-only packages (`Bogus`, `coverlet.*`, `FluentAssertions`, `Microsoft.AspNetCore.Mvc.Testing`, `Microsoft.NET.Test.Sdk`, `Moq`, `Respawn`, `Testcontainers.*`, `xunit*`, `Ardalis.HttpClientTestExtensions`). **§13.4b:** removed `keycloak-js`, `@xyflow/react`, `@dnd-kit/core`, `@dnd-kit/modifiers`, `popmotion`, `@use-gesture/react` from `frontend/package.json`. Deleted `src/components/devtools.tsx` (only `@xyflow/react` consumer). Removed `@import "@xyflow/react/dist/style.css"` from `globals.css`. **§13.5:** deleted 60 dead module icon `.tsx` files (DataCleansing*, FinalExport*, MatchConfiguration*, MatchDefinition*, MatchResults*, MergeSurvivorship*, plus 6 sidebar variants); rewrote `assets/icons/index.ts` to export only the 51 surviving icons. **§13.6:** rebranded `layout.tsx` title → "MatchLogic Profiler", `description` → "Free data profiling for CSV and Excel files". Slimmed `Breadcrumbs.tsx` ROUTE_LABELS / SUB_ROUTE_LABELS to surviving routes only. Renamed `LAST_ROUTE_KEY` → `profiler_last_routes` in `route-guard-provider.tsx`. **M1a:** added `Microsoft.AspNetCore.Authentication.JwtBearer` 8.0.10 to `Directory.Packages.props` + `MatchLogic.Api.csproj`. New: `MatchLogic.Api/Auth/CognitoOptions.cs` and `CognitoJwtSetup.cs` (extension method `AddCognitoJwtAuth(IConfiguration)` — registers JwtBearer with Authority = Cognito issuer URL, validates `iss` + `client_id`/`aud` against `CognitoOptions.ClientId`, falls back to a permissive scheme when config is empty so dev API still boots). Wired into `Startup.cs` (replaced bare `services.AddAuthentication()` + removed obsolete TODO comment, added `services.AddAuthorization()`). Added `Cognito` config block to `appsettings.json` (empty placeholders) and created new `appsettings.Development.json` with debug log level + Cognito placeholders. **Verification:** BE `dotnet build src/MatchLogic.Api` → 0 errors / 207 warnings (down from 1659 — tests deletion eliminated null-safety noise). FE `npm run build` → 6 routes generated as static content. Where: `backend/`, `frontend/`. **Notable orphan finding (deferred):** much larger orphan code footprint than the §13 list anticipated — `Application/Licensing/`, `Application/Interfaces/Scheduling/`, `Infrastructure/Scheduling/`, `Infrastructure/Extensions/LiveSearchSetup.cs`, `Api/Middleware/LicenseMiddleware.cs`, `Api/Handlers/{License,Scheduling}/`, `Domain/Scheduling/`, `EventHandlers/ProjectRunCompletedEventHandler.cs` all survived Phase A (verification agents only caught the `Infrastructure/{Identity,Licensing}` orphans). Compile-only because nothing reaches them, but `Startup.cs` still wires `AddSchedulingServices()` etc. Mark for orphan-pruning before M1c provisioning so DI doesn't pull dead types.
- **2026-04-27** — Phase B complete. Cloned `matchlogic-profiling-saas` repo, copied `backend/` and `frontend/` slices from `saas-extract` branches in main-product repos, scaffolded `infra/`, `docs/`, `.github/workflows/`. Three parallel verification agents audited the result: BE compile PASS, FE compile PASS, orphan audit found 3 critical issues. Fixed: (1) 3 `router.push("/data-import/select-table")` redirects updated to `/data-import/column-mapping` (route was deleted; would 404 at runtime). (2) Removed orphaned `MatchLogic.Infrastructure/Identity/`, `MatchLogic.Infrastructure/Licensing/`, `MatchLogic.Api/Configurations/IdentitySetup.cs`, `RbacSetup.cs` (all imported Keycloak/license code; compiled only because nothing invoked them). (3) Removed `AddLicensingServices()` and `ValidateLicensePublicKey()` from `MatchLogic.Infrastructure/Startup.cs`. Re-verified BE build → 0 errors. Wrote new repo `README.md`, `CLAUDE.md`, `MEMORY.md`, `docs/ARCHITECTURE.md`, `docs/RUNBOOK.md`, `docs/ABUSE-RESPONSE.md`, `infra/README.md`, `.github/workflows/{ci,frontend-deploy,backend-deploy}.yml`. Where: this entire repo.
- **2026-04-27** — Phase A complete. Branched both main-product repos at `saas-fork-2026-04-27` tag. Executed kill list (§4). Verified: BE `dotnet build src/MatchLogic.Api` → 0 errors; FE `tsc --noEmit` + `npm run build` → 0 errors / 10 routes generated (later trimmed to 6 here). Stubs left for M1 Cognito wiring. Local commits on `saas-extract` branches: BE `f135079`, FE `94b3b5c0`. Where: `MatchLogicBackend/saas-extract`, `MatchLogicFrontend/saas-extract`.

---

## §16 — Lessons Learned

> Append newest entries at the **top**. Format:
>
> - **Date:** YYYY-MM-DD
> - **Context:** one-line description
> - **What worked / didn't:** the outcome
> - **Rule going forward:** the durable takeaway

- **Date:** 2026-04-28
- **Context:** Cognito's new "quick setup" wizard was tried twice for the M1c user pool — both attempts produced an unusable pool and had to be deleted. First attempt: picked "Single-page application (SPA)" application type. The wizard auto-configured the pool with **email-alias** (separate username field, email is just a lookup helper) instead of **email-as-username** (email IS the username). At signup, `signUp(email, password)` failed with `Username cannot be of email format, since user pool is configured for email alias`. Second attempt: same wizard but the SPA path also generated a client secret by default; the SDK doesn't send `SECRET_HASH`, so `signUp` failed with `Client X is configured with secret but SECRET_HASH was not received`.
- **What worked:** Deleted both pools and recreated using the wizard's full-control screen — the one that explicitly asks "Options for sign-in identifiers" (checkboxes for Email / Phone / Username) and "Generate secret" toggle. **Email checked alone**, NOT Email + Username (Username being checked recreates the email-alias problem). **Generate secret toggle off**. The legacy 6-screen wizard appears to be gone from the new console UI; the form to look for is the one with that yellow warning *"Options for sign-in identifiers and required attributes can't be changed after the app has been created"* — that's the controlled path.
- **Rule going forward:** Two non-obvious gotchas in the new Cognito wizard for any future user pool:
  - **Sign-in identifier:** must be ONLY Email checked. Email + Username = email-alias mode. Pool sign-in attribute is permanent — can't be edited after creation.
  - **Client secret:** even when "Single-page application" is selected, the wizard may default to generating a secret. Public-SPA clients (using `amazon-cognito-identity-js` directly, no Hosted UI) must have the secret toggle explicitly off. App clients can be deleted + recreated freely; the user pool's sign-in identifier cannot.

- **Date:** 2026-04-28
- **Context:** During M1c smoke-test, the AdvanceProfile job logged `Saving advanced profile result` and then went silent for 5+ minutes with no exception. Hangfire didn't retry the job — it considered it "still running." The user saw the UI bar stuck at 90% and no progress. Investigation showed the analyzer phase finished in ~7 seconds, but `ProfileRepository.SaveProfileResultAsync` then iterated all columns × characteristics × patterns × distinct values and did **one** `_dataStore.InsertAsync` per `RowReferenceDocument` — totalling ~1000+ inserts. Each insert is a separate network round-trip; with Atlas at ~300ms WAN per round-trip, 1000 inserts = 5+ minutes. The original product was designed against a local Mongo (sub-millisecond) where this loop completed in ~2 seconds and nobody noticed.
- **What worked:** Refactored `SaveProfileResultAsync` to buffer all `RowReferenceDocument`s in a `List<>` first, then call `_dataStore.BulkInsertAsync(documents, collectionName)` once. One round-trip instead of 1000+. Profile save dropped from minutes-hanging to ~1 second. The bulk insert path was already implemented in `MongoDbDataStore` — it just wasn't being used.
- **Rule going forward:** When lifting code from a local-DB context to a network-DB context, **audit every `await ...InsertAsync` inside a `foreach` loop**. Sub-millisecond local inserts hide the cost of N×roundtrip patterns; against Atlas the same pattern hangs the user. Same applies to `UpdateAsync`, `DeleteAsync`, and any per-row repository call. Bulk operations (`BulkInsertAsync`, `BulkWriteAsync` via `MongoCollection.BulkWriteAsync`) were already in the codebase but underused. Pre-launch sweep needed: grep `foreach.*await.*Async` in handlers and commands.

- **Date:** 2026-04-28
- **Context:** During M1c boot attempts, the API kept failing with `Cannot instantiate implementation type 'GenericRepository<T,TKey>'` at DI validation. The reflection-based scan in `Startup.cs::AddRepositories` (`AppDomain.GetAssemblies().GetTypes().Where(t => ... implements IGenericRepository<,>)`) iterates loaded types looking for concrete repositories — and picks up `GenericRepository<T,TKey>` itself (the open generic base) because `IsGenericTypeDefinition`-filtering wasn't applied. Trying to register an open-generic concrete to an open-generic interface explicitly via `AddScoped(Type, Type)` blows up at validation.
- **What worked:** Replaced the reflection scan with explicit registrations of the only two specialized repositories that actually exist after the orphan prune (`JobStatusRepository`, `ProfileRepository`). The open generic registration `services.AddScoped(typeof(IGenericRepository<,>), typeof(GenericRepository<,>))` covers everything else.
- **Rule going forward:** **Reflection-based DI scans are a footgun after major code changes.** The original codebase had dozens of typed repositories so the scan made sense; after the orphan prune (~225 files removed) only two remained, but the scan stayed and now failed in a more confusing way (open-generic-on-open-generic) than a missing service. When pruning, audit reflection-based registration code for assumptions that no longer hold. Prefer explicit registrations once the inventory of types is small (<10) and stable.

- **Date:** 2026-04-28
- **Context:** During M1c smoke-test, the first job dispatch silently ran 4 retries with no logged exceptions — `[INF] Executing job ... Step: AdvanceProfile` four times and nothing else. Investigation revealed Hangfire was catching `InvalidOperationException` from `_serviceProvider.GetRequiredService(typeof(AdvanceDataProfilingCommand))` (the command was never registered) but only logged at TRACE level, not propagated to ERR.
- **What worked:** Adding the explicit DI registrations for `DataImportCommand`, `DataProfilingCommand`, `AdvanceDataProfilingCommand` made the next exception (`IRecordHasher` not registered) surface at startup-time validation instead of silently inside the job worker. Once DI validation was clean, the job ran and exposed the next bug (the WAN insert-loop above) which DID log informatively.
- **Rule going forward:** **Trust the startup-time DI validation more than runtime resolution failures.** A registration that's missing at boot will fail loudly when ASP.NET Core validates services on `BuildServiceProvider`. A registration that's missing at job-execution time gets silently retried by Hangfire, looking identical to "still running." When debugging Hangfire jobs that "do nothing," check first whether all the types in `CommandFactory._commands` (or equivalent registry) are explicitly registered in DI — the factory pattern bypasses the framework's validation.

- **Date:** 2026-04-28
- **Context:** User said "UI looked like shit" while the auth + profile flow was working. They're not a developer; the styling complaint was incidental, not a blocker. I noted it and didn't dive in. Confirmed the right call: a non-dev's "looks bad" is a separate visual-polish pass that needs design intent, not a regression on the milestone. M1c was about plumbing.
- **What worked:** Acknowledged the comment, finished the milestone, parked the styling. Saved energy for the actual blockers (DI errors, profile hang).
- **Rule going forward:** When the user's primary objective is end-to-end functionality and they're a non-dev observing their first-ever working flow, treat aesthetic complaints as parking-lot items. Don't yak-shave styling mid-milestone. Visual polish has its own milestone budget; don't draw down on it during plumbing work.

- **Date:** 2026-04-27
- **Context:** M1c-artifact authoring asked whether to wire SES into `auth.yml` from day one (so signup OTP emails come from `noreply@matchlogic.io` instead of the AWS-generic `no-reply@verificationemail.com`).
- **What worked / didn't:** Did **not** add SES to `auth.yml`. Cognito's default sender is rate-limited to ~50 emails/day and uses an unfamiliar From address — but for an M1c smoke test of "signup → OTP → login," that is more than enough. Wiring SES properly requires (a) a verified domain identity (DNS records on the matchlogic.io zone), (b) a production-access request (24-hour AWS approval), and (c) re-pointing `EmailConfiguration` from default to `EmailSendingAccount: DEVELOPER` with the SES SourceArn. Bundling all three into M1c would have blocked the milestone on AWS support response time and DNS access that the runbook step doesn't need. Deferred to launch prep with a clear "Deferred to launch prep" section in the M1c runbook + a callout in this Change Log entry.
- **Rule going forward:** When a milestone has a "smoke-test" exit criterion, separate "what's needed to walk the happy path once with my own email" from "what's needed before public traffic." The latter rolls into a launch-prep checklist; the former is the milestone. SES, MFA-at-rest, disposable-email blocklist, and VPC peering all fall on the launch-prep side for an auth milestone — not the smoke-test side.

- **Date:** 2026-04-27
- **Context:** M1c also forced a choice about MongoDB Atlas in CloudFormation. AWS has an `AWS::QuickSight::DataSet`-style `MongoDB Atlas Resources for AWS CloudFormation` registry, but it's a third-party-published registry that requires per-account activation, a public/private API key registered as a CloudFormation resource secret, and gives less direct control than the native Terraform provider. Native Terraform with the `mongodb/mongodbatlas` provider is the canonical Atlas IaC path.
- **What worked / didn't:** Used Terraform for Atlas, CloudFormation for AWS-native (Cognito here, ECS/ALB/CloudFront in M1d). Two IaC tools is more friction than one but they have non-overlapping scopes: AWS-native resources stay in CFN (free, native, no provider-version drift), Atlas stays in TF (the provider IS Atlas's API surface). Avoided creating a polyglot stack where a Terraform aws-provider would compete with CFN templates for the same resources.
- **Rule going forward:** Pick IaC tools by *vendor*, not by aesthetics. AWS resources → CloudFormation. Non-AWS resources whose API isn't natively reachable from CFN → Terraform with the vendor's official provider. Crossing the streams (Terraform managing AWS *and* Atlas) creates state-import headaches when the AWS side later moves to CFN; keeping each tool to its native vendor avoids it.

- **Date:** 2026-04-27
- **Context:** M1b restructured `app/` into `(app)/` and `(auth)/` route groups, moving `RouteGuardProvider` and `JobStateProvider` out of the `Providers` client boundary and into `app/(app)/layout.tsx` directly. The build broke with `ReactServerComponentsError: You're importing a component that needs createContext / useState. It only works in a Client Component but none of its parents are marked with "use client"`. The two providers had been working for months because they were nested inside `Providers` (which has `"use client"`); pulling them up into a server-component layout removed that boundary.
- **What worked:** Adding `"use client"` directly to `route-guard-provider.tsx` and `job-state-provider.tsx` rather than promoting `app/(app)/layout.tsx` to a client component. Layouts in Next.js App Router benefit from staying server components for streaming/data-fetching, and the boundary belongs at the lowest possible level — here, the providers themselves, which use `useState`/`useContext`/`useEffect` directly.
- **Rule going forward:** When a provider component imports React state/context primitives directly (`useState`, `useEffect`, `useContext`, `createContext`), put `"use client"` on the **provider file itself**, not on consumers. This makes the provider relocatable: it can be dropped into either a client- or server-side layout without flipping the layout's rendering mode. Inversely: a layout that does no I/O of its own should stay a server component so its children can stream.

- **Date:** 2026-04-27
- **Context:** M1b chose `amazon-cognito-identity-js` over `aws-amplify/auth` for the frontend SDK. The deciding factor wasn't bundle size in isolation but the *commitment* the choice signalled: Amplify's `Auth` module brings a config registry, Hub event bus, and assumes you're going to add Storage/API/Analytics later (none of which we plan to — S3 talks to the backend, not Amplify-Storage). `amazon-cognito-identity-js` is the "just Cognito, please" library and maps 1:1 onto a Keycloak-shaped `signUp/confirmSignUp/signIn/signOut/refresh` surface. Install was clean under `--legacy-peer-deps` (added 20 packages, 0 install errors).
- **What worked:** Wrapping the SDK in a tiny module-level `tokens` store + `subscribeToTokens(cb)` pattern so `apiFetch.getAccessToken` can stay synchronous (one of the constraints — `apiFetch` callers don't `await` token resolution). The CognitoProvider keeps the store hot via session restore + a `setTimeout` that fires 5 min before id-token expiry. No timer-juggling in the page-level code.
- **Rule going forward:** When picking a vendor SDK that has a "full ecosystem" version vs. a "just the one thing" version, evaluate the choice as a *commitment*, not a *bundle size*. Amplify-Auth-only is fine in isolation but signals "we'll add Amplify-Storage and Amplify-API later" — and if that's not on the roadmap, the lighter library is the right call regardless of kB.

- **Date:** 2026-04-27
- **Context:** Stale `.next/types/...` files were generated by an earlier `next build` against the pre-restructure `app/` tree. After `git mv`-ing routes into `(app)/`, `npx tsc --noEmit` failed with 16 errors in `.next/types/app/data-import/...page.ts: Cannot find module '../../../../src/app/data-import/page.js'` — because the type-stub paths were frozen from the old layout.
- **What worked:** `rm -rf .next` then re-run `npx tsc --noEmit` → 0 errors. Next.js regenerates the type stubs on the next build pass.
- **Rule going forward:** After any directory restructure under `frontend/src/app/`, delete `.next/` before running `tsc` or `npm run build`. The `.next/types/` directory is a build artifact, not authoritative — stale entries there mask a clean compile.

- **Date:** 2026-04-27
- **Context:** Full-transitive orphan prune. The §15 list (~45 files) cascaded into ~225 file deletes once each delete exposed the next layer of orphan code. The wave-by-wave plan structure (W1: §15 list, W2: FinalExport, W3: LiveSearch, W4: matching family) was abandoned mid-execution because deleting Domain.Scheduling immediately broke FinalExportCommand (which imported it), deleting Application.Features broke ProjectService (which imported its repositories), and so on. Each wave's "self-contained, build-green commit" goal collided with the actual dependency graph.
- **What worked:** Driving the prune by build errors instead of by pre-planned waves. After bulk-deleting obvious orphans (entire API handler folders for non-surviving endpoints, then entire Application/Features/Interfaces/Domain/Infrastructure subfolders), I let `dotnet build` enumerate what was still referenced. Each round of errors revealed the next batch of unreachable code. Build error count went 1 → 4 → 27 → 97 → 88 → 9 → 4 → 9 → 95 → 148 → 35 → 14 → 7 → 15 → 0. The "spikes" each marked a moment when one assembly compiled and exposed errors in a downstream assembly.
- **What didn't:** I overshot twice and had to restore code from git: (1) deleted `Application/Interfaces/Dictionary/`, `Domain/Dictionary/`, `Infrastructure/Dictionary/` thinking they were cleansing-related, but `DataProfiler` actively uses `IDictionaryCategoryService` for value categorization; (2) deleted `Domain/Entities/Common/MatchJobInfo.cs` which also held the load-bearing `ProjectJobInfo` class. Recovery was easy via `git show HEAD:path` but cost a build round.
- **Rule going forward:** When pruning a slice from a larger codebase, drive the surgery from build errors, not from a pre-planned wave decomposition. Pre-planned waves assume you understand the dependency graph; build errors *enumerate* it. Also: before deleting any folder, grep for **types defined in that folder being referenced from the surviving surface** — not just "this folder imports a deleted thing." `IDictionaryCategoryService` was deleted because the *folder* matched a "matching/cleansing" mental category, but the surviving DataProfiler depended on it. The grep "what consumes types from `X/`" should run before delete, not after build error.

- **Date:** 2026-04-27
- **Context:** Recovering load-bearing types into stable namespaces. After deleting `Domain/CleansingAndStandaradization/DataFlowContext.cs` for being orphan, the build broke in 13 unrelated files because `FlowStatistics` (a job-stats DTO used by JobEventPublisher, BaseCommand, ProjectService, etc.) happened to live inside that file, sharing the namespace with cleansing-only types.
- **What worked:** Recreating `FlowStatistics` as its own file under `Domain/Entities/Common/` — a stable, neutral namespace. Then `sed -i 's|using MatchLogic.Domain.CleansingAndStandaradization;|using MatchLogic.Domain.Entities.Common;|g'` across the 13 consumer files. One namespace move, 13 import rewrites, build green.
- **Rule going forward:** Before deleting a file from a larger codebase slice, grep its **public type names** against the surviving code. If any are referenced, those types are load-bearing — extract them into a stable namespace BEFORE the delete, then update consumers. Same pattern applied to `FieldMapping`/`FieldMappingEx`/`FieldOrigin` (lived in `MatchDefinition.cs`, used by ImportModule + OrderedDataImportModule for column metadata) and `ProjectJobInfo` (lived in `MatchJobInfo.cs`, used by HangfireScheduler/JobExecutor/ProjectService for profile-job dispatch). The pattern: **co-located types in main-product source files often span multiple concerns; extract them along the cleavage line before pruning.**

- **Date:** 2026-04-27
- **Context:** §13 cleanup uncovered orphan footprint Phase B's verification agents missed. Phase B caught `Infrastructure/{Identity,Licensing}/` because they imported Keycloak types and tripped a "what imports killed packages" search. But identical orphans living one layer up (`Application/Licensing/`, `Application/Interfaces/Scheduling/`, `Infrastructure/Scheduling/`, `Api/Handlers/{License,Scheduling}/`, `Api/Middleware/LicenseMiddleware.cs`, `Domain/Scheduling/`) compiled fine — they don't import deleted packages, just contain code with no API surface anymore. `Startup.cs` even still calls `AddSchedulingServices()`. They survived because the Phase B audit pattern only flagged "imports a deleted thing".
- **What worked:** Looking at appsettings.json sections we wanted to delete (e.g. `Scheduler`, `LicenseSettings`), then reverse-grepping for `GetSection("Scheduler")` / `LicenseSettings` references. Each surviving config-key reference exposed a chain of orphan code (LicenseSettings.cs class → ILicenseService.cs → license handlers → license endpoints).
- **Rule going forward:** When extracting a slice, pair "imports-a-killed-package" audits with "what config keys do we no longer need" audits. Each dead config section is the entry point to a dependency tree the host layer doesn't invoke but that DI still wires up. Run the config-key audit BEFORE marking extraction "done."

- **Date:** 2026-04-27
- **Context:** Frontend `npm run build` failed AFTER `tsc --noEmit` passed. `globals.css` had `@import "@xyflow/react/dist/style.css"` — type-checking doesn't read CSS imports but webpack does, so removing `@xyflow/react` from `package.json` only surfaced at build time.
- **What worked:** Running `npm run build` (not just `tsc --noEmit`) as the build verification step. The actual webpack pass catches CSS/asset references that the type-checker silently passes.
- **Rule going forward:** When removing frontend dependencies, search `*.css` / `*.scss` for `@import` references too — type-check + tsc are not sufficient verification. Always end with a real build.

- **Date:** 2026-04-27
- **Context:** Phase A "compile-check" was more efficient than feared because the kill list mostly removed endpoint registrations and unused projects, not deeply integrated code. Killed features compiled in isolation because nothing referenced them.
- **What worked:** Building `MatchLogic.Api.csproj` ALONE (not the full `.sln`) — surfaced runtime breakage cleanly while ignoring tests-that-reference-killed-endpoints noise. ~26s build time vs. multi-minute full solution.
- **Rule going forward:** When extracting a slice from a larger codebase, start by killing endpoint registrations in the host layer (Startup.cs / Program.cs) — the application layer often compiles fine because it's invoked only via DI. The host layer is the real entry point.

- **Date:** 2026-04-27
- **Context:** Three parallel verification agents (one per concern: BE compile, FE compile, orphan audit) caught a runtime 404 risk that compile-only checks missed: 3 `router.push("/data-import/select-table")` calls pointing to a deleted route. TypeScript happily compiles strings.
- **What worked:** Static-only audit agent (no builds) explicitly searched for hard-coded URLs and grep-mapped them against the surviving routes list.
- **Rule going forward:** After any kill list, run an explicit "do hard-coded URLs still resolve" audit. TypeScript's type system does not validate `router.push()` strings against the actual `app/` route tree. Future check: write a Vitest test that walks `router.push` literals and asserts each resolves to a real page file.
