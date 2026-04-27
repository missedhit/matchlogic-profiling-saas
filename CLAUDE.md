# CLAUDE.md — MatchLogic Profiling SaaS

> Slim operating manual. Deep reference (full kill list, architecture decisions, milestone plan, pending work) lives in **[MEMORY.md](./MEMORY.md)**. Original implementation plan: `~/.claude/plans/i-want-you-to-steady-mist.md`.

## ALWAYS read MEMORY.md first

Before any non-trivial task in this repo, open [MEMORY.md](./MEMORY.md). It carries: what this product is, what was extracted from the main MatchLogic codebase, what was killed, what TODOs are intentionally left as M1+ markers, and the sequenced milestones still to ship. After meaningful work, **append** entries to its Change Log and/or Lessons Learned so the next session inherits what you learned.

---

## What this product is

A **free public lead-gen tool** — prospects sign up, upload one or more CSV/Excel files (max 1000 records lifetime per account), run column profiling, optionally export the profile as CSV or JSON. Hosted on AWS. Designed to convert visitors into qualified leads for the gated main MatchLogic product.

**Hard product constraints** (non-negotiable):
- 1000-record lifetime cap per account, atomic two-phase enforcement.
- CSV/Excel only — no DB connectors, no cloud storage, no remote fetch.
- Accounts auto-delete after 30 days inactive (silent purge, T&Cs disclose).
- Bot resistance + abuse prevention layered (WAF + Turnstile + OTP + quotas + scoring).
- AWS Budgets kill-switch hard-caps cloud spend at $300/mo.

## Stack at a glance

| Layer | Choice |
|---|---|
| Backend | .NET 8, ECS Fargate (1 task warm, scale 1→3), behind ALB + CloudFront + WAF |
| Frontend | Next.js 14 static export → S3 + CloudFront + WAF |
| Auth | AWS Cognito User Pools (OTP, JWT) — `CognitoProvider` shim exposes Keycloak-shaped API so lifted module code is auth-agnostic |
| Data | MongoDB Atlas M10 (Mongo.Driver native + Hangfire.Mongo); S3 for files; ElastiCache Redis for token buckets |
| Hostname | `app.profiler.matchlogic.io` (FE) + `api.profiler.matchlogic.io` (API) |
| Region | `us-east-1` |

## Non-negotiable rules

1. **Never fabricate.** Don't invent files, functions, APIs, types, props, or facts. If you don't know, check the code or [MEMORY.md](./MEMORY.md). "Probably" / "should" are red flags — verify.
2. **Reuse before you write.** Before adding any new component, hook, util, slice, type, or style, search the codebase. New code is the **last** resort.
3. **Inherit, don't duplicate.** Brand fonts/colors/spacing/types are defined ONCE — in `frontend/tailwind.config.ts`, CSS vars, `frontend/src/lib/brand-colors.ts` — and consumed by reference. No inline hex codes. No copy-pasted helpers.
4. **Minimal new code.** The existing structure already covers most needs. No abstractions or refactors a task didn't ask for.
5. **Maintainability is the bar.** Any human dev should understand the change at a glance. Clear, boring, conventional code.
6. **Quota enforcement is the abuse linchpin.** Any code path that touches file upload OR records counting MUST go through `IQuotaService` (M4). Integration tests will fail builds that bypass it.

## Workflow

1. **Plan Mode by default** for anything 3+ steps or with architectural impact.
2. **Subagents liberally** — offload research/exploration so the main context stays clean.
3. **Self-improvement** — after any user correction, append to MEMORY.md Lessons Learned.
4. **Verify before "done"** — never claim completion without proving it works (compile, run, test).
5. **TODO markers are M1+ phase gates.** Look for `TODO (M1` / `TODO (M2` / `TODO (M4` / `TODO (M5` comments in code. They're intentional and document where Cognito wiring, S3 storage, quota enforcement, etc. need to land. Don't remove them as "stale comments" — they ARE the work.

## Build / test / run

```bash
# Backend
cd backend
dotnet build src/MatchLogic.Api/MatchLogic.Api.csproj           # Should be 0 errors
dotnet run --project src/MatchLogic.Api/MatchLogic.Api.csproj   # Local dev (M1: with Cognito + Mongo Atlas)

# Frontend
cd frontend
npm install --legacy-peer-deps                                   # React 19 / Next 14.0.3 mismatch
npx tsc --noEmit                                                 # 0 errors
npm run build                                                    # Static export, 6 routes
npm run dev                                                      # Local dev server on :3000
```

**Endpoints:** HTTP `http://localhost:7122` · Swagger `/api-docs`

`backend/tests/` projects are intentionally not built — they reference killed features. Replace with SaaS-targeted tests during M1+.

## Pipeline (single-page experience)

The SaaS is one linear flow: **ProjectManagement → DataImport → DataProfiling → (Export)**. No matching, cleansing, survivorship, or final-export pipeline — those stayed in the main product.

| # | Module | Route |
|---|---|---|
| 1 | ProjectManagement | `/project-management` |
| 2 | DataImport (CSV/Excel only) | `/data-import` → `/data-import/data-sources` → `/data-import/column-mapping` |
| 3 | DataProfiling | `/data-profiling` |

Sidebar collapses to these 3. No NextStepBanner, no pipeline-stage gating beyond "do you have a datasource yet."

## Sequenced milestones

| M | Scope | Status |
|---|---|---|
| **Phase A** | Extracted from main MatchLogic, kill list executed, compiles clean | ✅ Done |
| **§13 cleanup** | Dead config/packages/icons/artifacts removed; tests deleted; rebrand pass | ✅ Done |
| **M1a** | Backend `CognitoJwtSetup` + JwtBearer package + dev appsettings | ✅ Done |
| **M1b** | FE `CognitoProvider` shim + signup/login/verify/account pages + `useAuth`/`apiFetch` rewire | ✅ Done |
| **M1c–d** | Provision Cognito/Atlas → Dockerfile/Fargate/CF/ACM/R53 | ⬜ Next |
| **M2** | S3 upload flow + presigned PUTs + profile job runs end-to-end | ⬜ |
| **M3** | New `POST /api/DataProfile/Export/{projectId}/{dataSourceId}` (CSV/JSON) + UI button | ⬜ |
| **M4** | WAF + Turnstile + IQuotaService + Redis token buckets + abuse scoring + GuardDuty | ⬜ |
| **M5** | LastActiveTrackingMiddleware + AccountCleanupJob + Budgets kill-switch + dashboards | ⬜ |

Total ~5 weeks single engineer. See `docs/ARCHITECTURE.md` for full milestone scope.

## Provenance

Forked from main MatchLogic at tag `saas-fork-2026-04-27` on:
- Backend: branched from `feat/refreshDataSource` of `MatchLogic/MatchLogicBackend` (commit `78e6548`)
- Frontend: branched from `testing` of `MatchLogic/MatchLogicFrontend` (commit `317e1372`)

Phase A extraction commits live on `saas-extract` branches in the main-product repos (local-only, never pushed). They are reference checkpoints; ignore them after Phase B.

**No merges back.** Cherry-pick critical bug fixes one-way only (main → SaaS).

---

**Reminder:** for the deep architecture reference, full kill list, pre-launch cleanup checklist, and sequenced milestones — **see [MEMORY.md](./MEMORY.md)**. For deployment specifics — see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
