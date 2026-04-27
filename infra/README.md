# Infrastructure

CloudFormation templates for the Profiling SaaS AWS deployment.

**Status:** scaffold only — actual templates land in M1+ alongside the deployment work.

## Planned templates (per [`../docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) §6–§7)

| Template | Resources |
|---|---|
| `cloudformation/network.yml` | VPC, public/private subnets, NAT, route tables, security groups |
| `cloudformation/compute.yml` | ECS cluster, ALB, target group, Fargate service + task definition, ECR repo |
| `cloudformation/data.yml` | S3 bucket (uploads + lifecycle), ElastiCache Redis, MongoDB Atlas peering |
| `cloudformation/auth.yml` | Cognito User Pool, app client, identity pool, SES domain |
| `cloudformation/edge.yml` | CloudFront distributions (FE + API), WAF web ACLs, ACM cert, Route 53 records |
| `cloudformation/safeguards.yml` | AWS Budgets, SNS topic, kill-switch Lambda, SSM Parameter Store flag, GuardDuty |

Region: `us-east-1`. Single environment to start (`prod`); add `staging` after launch.
