# Abuse Response

> What to do when `AbuseScoringService` flags an account or WAF surfaces unusual traffic.

## Scoring thresholds

- **Score ≥ 50** → soft-suspend (re-verify required to continue).
- **Score ≥ 80** → hard block.

## Score signals (per ARCHITECTURE.md §7)

| Signal | Points |
|---|---|
| Datacenter ASN (AWS/GCP/Azure/Hetzner/OVH) at signup | +30 |
| >3 signups from same IP /24 in 24h | +20 |
| Bot UA (`python-requests`, `curl`, missing UA, headless Chrome fingerprint) | +20 |
| >50% of account's requests return 4xx | +15 |
| File MIME mismatch / unusual file size (0 bytes, exactly at limit) | +10 |
| Quota phase-2 rollback (declared row count < actual) | +20 |

## Investigation steps (when flagged)

1. **CloudWatch Logs Insights** — query `risk_events` table for the account's signal history.
2. **Mongo** — check `Project.AbuseStatus` field, `AccountQuota.recordsConsumed`, recent `DataSource` uploads.
3. **WAF logs** — search by source IP for blocked requests.
4. **Cognito** — confirm email verification status, signup IP, signup user-agent.

## Manual actions

- **Confirm legitimate user** (false positive): clear `Project.AbuseStatus`, reset score to 0, log decision in `risk_events` with `manual_clear` reason.
- **Confirm abuse**: ensure `AbuseStatus = HardBlocked`, expire all sessions, optionally `cognito.AdminDeleteUser` for repeat offenders.
- **Pattern across multiple accounts**: add WAF custom rule for the IP/ASN pattern, raise alarm threshold awareness.

## Escalations

_Document team-specific paths here once on-call rotation is established._
