# Runbook

> Ops procedures for the Profiling SaaS. Filled in as M1+ deployment work lands.

## Deploy

_TBD — populate when CI workflows + CloudFormation templates ship in M1._

## Rollback

_TBD._

## Kill switch (M5)

When AWS Budgets fires the alarm at $300/mo:

1. SNS → Lambda flips `/profiler-saas/feature-flags/uploads-enabled` in SSM Parameter Store to `false`.
2. Frontend reads flag via cached `/api/version` payload (30s TTL) and shows "Service paused for maintenance" banner on upload click.
3. Backend `/api/dataimport/File` returns 503 until flag is flipped back.

To re-enable manually:
```
aws ssm put-parameter --name /profiler-saas/feature-flags/uploads-enabled --value true --overwrite
```

Investigate cost driver before re-enabling: CloudWatch Logs Insights query for top accounts by request count over last 24h.

## Account inactivity cleanup (M5)

`AccountCleanupJob` runs daily at 03:00 UTC via Hangfire recurring job. To trigger manually for testing:
```csharp
BackgroundJob.Enqueue<AccountCleanupJob>(j => j.Run());
```

Idempotent — safe to re-run. Audit log appears in CloudWatch.

## Common queries

_TBD — populate after first weeks of production traffic surface real ops needs._
