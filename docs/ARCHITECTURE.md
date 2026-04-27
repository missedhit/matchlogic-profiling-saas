# Architecture

> Source of truth for the Profiling SaaS architecture. Mirrors the original implementation plan at `~/.claude/plans/i-want-you-to-steady-mist.md`. Update this doc when architecture decisions change.

## What this product is

A free public lead-gen tool that lets prospects upload a CSV/Excel file (≤1000 records lifetime per account, across all files) and run **column profiling + analytics** — type detection, null/uniqueness/entropy stats, pattern discovery, validity scoring — then export the profile as CSV or JSON. The full MatchLogic matching/cleansing/survivorship pipeline stays gated behind sales conversations.

## Hard product constraints

- Free public sign-up. CSV/Excel uploads only. **No** database connectors, **no** cloud-storage connectors.
- **1000 records lifetime** per account, across all files combined.
- Accounts auto-delete after **30 days of inactivity** (silent purge — T&Cs disclose).
- Bot resistance + abuse prevention non-negotiable; this is on AWS and abuse burns our cloud spend.
- Zero impact on existing main-product work.

## Architecture diagram

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

## Stack decisions

| Layer | Choice | Why |
|---|---|---|
| Backend | .NET 8 lifted as-is on ECS Fargate | Battle-tested CSV/Excel readers + AdvancedProfiling analyzers + IDataStore plumbing. No rewrite. 0.5 vCPU / 1 GB / 1 task warm, auto-scale 1→3 on CPU >70%/2min. |
| Frontend | Next.js 14 static export → S3 + CloudFront | `output: "export"` already on. WAF at distribution. |
| Auth | AWS Cognito User Pools | Native OTP via SES, JWT, free up to 50k MAU. Frontend `CognitoProvider` shim exposes same shape as legacy `KeycloakProvider` so lifted code is auth-agnostic. |
| Data | MongoDB Atlas M10 (~$60/mo) | Reuses `MongoDB.Driver` 3.5.2 + Hangfire.Mongo. DocumentDB and DynamoDB rejected (operator gaps / forced schema rework). |
| Hostname | `profiler.matchlogic.io` (FE: `app.`, API: `api.`) | Subdomain on existing brand zone. |
| Region | `us-east-1` | Single region for v1; multi-region deferred. |

## Repo layout

```
matchlogic-profiling-saas/
├── backend/                     # .NET 8 API — ex MatchLogicBackend slice (saas-extract)
├── frontend/                    # Next.js 14 static export — ex MatchLogicFrontend slice
├── infra/cloudformation/        # AWS infrastructure (scaffold; templates land in M1)
├── docs/                        # Architecture + runbook + abuse response
├── .github/workflows/           # CI: lint, type-check, test, build, deploy
├── README.md
├── CLAUDE.md                    # Slim operating manual for Claude
└── MEMORY.md                    # Deep reference + persistent log
```

## Sequenced milestones

| M | Scope | Wks |
|---|---|---|
| **M1** | Cognito User Pool + `CognitoProvider` shim + JWT bearer wired against Cognito JWKS. Mongo Atlas M10 cluster up. ECS Fargate task running `/api/HealthCheck`. CloudFront + S3 serving FE shell with login + signup. ACM + Route 53 records. | 1.5 |
| **M2** | S3 + presigned PUT. `UploadFileHandler` writes to S3. `ImportDatasourceHandler` reads from S3 stream. `DataProfile/GenerateAdvance` runs end-to-end via Hangfire. Test fixtures: 5/100-row CSV, 50-row XLSX. | 1 |
| **M3** | New `POST /api/DataProfile/Export/{projectId}/{dataSourceId}` (CSV + JSON). `useExportProfile` hook + UI button. | 0.5 |
| **M4** | WAF + Turnstile + disposable-email Lambda + `IQuotaService` two-phase enforcement + Redis token buckets + `AbuseScoringService` + GuardDuty Malware Protection + magic-byte sniffing. | 1.5 |
| **M5** | `LastActiveTrackingMiddleware` + `AccountCleanupJob` daily Hangfire cron + S3 lifecycle (7d uploads, 30d results) + AWS Budgets→SNS→Lambda kill-switch + CloudWatch dashboards. | 1 |

## New code to write (per milestone)

### Backend
- `backend/src/MatchLogic.Application/Services/QuotaService.cs` — two-phase atomic 1000-row enforcement (the abuse linchpin).
- `backend/src/MatchLogic.Infrastructure/Storage/S3FileStorageService.cs` — replaces local-disk uploads.
- `backend/src/MatchLogic.Application/Features/DataProfiling/Export/ExportProfileQueryHandler.cs` — CSV/JSON profile export (only feature gap).
- `backend/src/MatchLogic.Api/Endpoints/DataProfileExportEndpoint.cs` — new endpoint binding.
- `backend/src/MatchLogic.Application/Features/Cleanup/AccountCleanupJob.cs` — 30-day silent purge across Mongo + S3 + Cognito.
- `backend/src/MatchLogic.Api/Middleware/LastActiveTrackingMiddleware.cs` — debounced project activity heartbeat.
- `backend/src/MatchLogic.Api/Middleware/TurnstileValidationFilter.cs` — endpoint filter on upload + signup.
- `backend/src/MatchLogic.Application/Services/AbuseScoringService.cs` — ASN/UA/IP-cluster scoring.
- `backend/src/MatchLogic.Api/Auth/CognitoJwtSetup.cs` — replaces Keycloak setup.
- `backend/Dockerfile`, `backend/.dockerignore` — container packaging.

### Frontend
- `frontend/src/providers/cognito-provider.tsx` + `frontend/src/lib/cognito.ts` — Keycloak-shaped Cognito shim.
- `frontend/src/components/common/turnstile-widget.tsx` — bot challenge.
- `frontend/src/modules/DataProfiling/hooks/use-export-profile.ts` — export mutation.
- `frontend/src/modules/DataProfiling/components/export-profile-button.tsx` — UI trigger.
- `frontend/src/app/account/page.tsx` — view quota usage + manual delete.
- `frontend/src/app/signup/page.tsx`, `login/page.tsx`, `verify/page.tsx` — Cognito OTP flow.

## TODO markers in lifted code

Look for `TODO (M1` / `TODO (M4` comments in:
- `backend/src/MatchLogic.Api/Program.cs` (request body clamp)
- `backend/src/MatchLogic.Api/Startup.cs` (Cognito JWT registration)
- `frontend/src/app/providers.tsx` (CognitoProvider wrapping)
- `frontend/src/utils/apiFetch.ts` (Cognito ID token injection)
- `frontend/src/hooks/use-auth.ts` (Cognito sign-out + account page)
- `frontend/src/providers/route-guard-provider.tsx` (single-page redesign)

## Cost projection

- **Quiet** (50 signups/mo): ~$118/mo
- **Busy** (10k signups/mo): ~$211/mo
- Hard ceiling: AWS Budgets kill-switch at $300/mo

## Verification checklist (per milestone)

See `~/.claude/plans/i-want-you-to-steady-mist.md` §11 for the full manual test script.
