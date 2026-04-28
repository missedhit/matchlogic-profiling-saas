# Infrastructure

AWS CloudFormation + MongoDB Atlas Terraform for the Profiling SaaS deployment. Region: `us-east-1`. Single environment (`dev`) until M1d; production gets its own stacks.

## Templates

### CloudFormation — `cloudformation/`

| Template | Resources | Status |
|---|---|---|
| `auth.yml` | Cognito User Pool, App Client (public SPA, SRP auth, no client secret) | M1c |
| `network.yml` | VPC, public/private subnets, NAT, route tables, security groups | M1d |
| `compute.yml` | ECS cluster, ALB, target group, Fargate service + task definition, ECR repo | M1d |
| `data.yml` | S3 bucket (uploads + lifecycle), ElastiCache Redis | M2/M4 |
| `edge.yml` | CloudFront distributions (FE + API), WAF web ACLs, ACM cert, Route 53 records | M1d |
| `safeguards.yml` | AWS Budgets, SNS topic, kill-switch Lambda, SSM Parameter Store flag, GuardDuty | M5 |

Deploy `auth.yml` per the [M1C provisioning runbook](../docs/M1C-PROVISIONING.md).

### Terraform — `terraform/`

MongoDB Atlas can't be CloudFormation'd cleanly — Atlas lives outside AWS. Atlas resources are Terraform.

| Module | Resources | Status |
|---|---|---|
| `terraform/atlas/` | Atlas project, M10 cluster, DB user, IP allowlist | M1c |

Provider auth is via `MONGODB_ATLAS_PUBLIC_KEY` / `MONGODB_ATLAS_PRIVATE_KEY` env vars. DB user password via `TF_VAR_db_password`. Never commit `terraform.tfvars` or state files (gitignored).

Deploy per the [M1C provisioning runbook](../docs/M1C-PROVISIONING.md).
