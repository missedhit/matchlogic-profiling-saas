# MatchLogic Profiling SaaS

A free public lead-gen tool that lets prospects upload a CSV/Excel file, run column profiling + analytics, and export the results — all hosted on AWS. Up to 1000 records per account, accounts auto-delete after 30 days inactive.

**Status:** Phase A complete (extracted from main MatchLogic codebase, compiles clean). M1+ work pending — see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the milestone plan.

## Layout

```
backend/    → .NET 8 API (lifted from MatchLogic main product, kill-list trimmed)
frontend/   → Next.js 14 static export (lifted, slimmed to single-page experience)
infra/      → AWS CloudFormation templates (scaffold; populated in M1)
docs/       → ARCHITECTURE, RUNBOOK, ABUSE-RESPONSE
.github/    → CI workflows (compile-check; deploy stubs land in M1)
CLAUDE.md   → Operating manual for Claude when working in this repo
MEMORY.md   → Deep reference + persistent log of decisions, kills, and pending work
```

## Build & test

```bash
# Backend
cd backend
dotnet build src/MatchLogic.Api/MatchLogic.Api.csproj

# Frontend
cd frontend
npm install --legacy-peer-deps   # React 19 / Next 14.0.3 peer mismatch — known
npx tsc --noEmit
npm run build
```

Test projects under `backend/tests/` are intentionally NOT built — they reference handlers from killed features and will be replaced with SaaS-specific tests during M1+.

## Where to start

1. Read [`CLAUDE.md`](CLAUDE.md) for the operating rules.
2. Read [`MEMORY.md`](MEMORY.md) for full architectural context, kill list, and remaining work.
3. Read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the deployment + safeguards plan.
