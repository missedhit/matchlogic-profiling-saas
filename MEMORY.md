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
| **M1b** | Frontend `CognitoProvider` shim + signup/login/verify/account pages + `useAuth`/`apiFetch` rewire | ⬜ Next |
| **M1c** | Provision Cognito User Pool + Mongo Atlas M10 + plug values into config; smoke-test signup/login locally | ⬜ |
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
