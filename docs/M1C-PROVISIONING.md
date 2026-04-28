# M1c — Cognito + Atlas provisioning runbook

End-to-end procedure for the M1c milestone: provision a Cognito User Pool and a MongoDB Atlas M10 cluster, drop the values into local config, and smoke-test the full signup → OTP → login → `/project-management` loop against real services.

> **Cost note.** M10 Atlas is **~$57/mo** while it runs. Cognito is free up to 50K MAU. SES is not used in M1c (default Cognito sender, ~50 emails/day cap, sufficient for smoke testing). Tear the Atlas cluster down (`terraform destroy`) the moment you're done if you're not moving straight into M1d/M2.

---

## 0. Prerequisites

- AWS CLI v2 with credentials for the target account. Region default `us-east-1`.
  ```bash
  aws sts get-caller-identity   # must succeed before continuing
  ```
- Terraform ≥ 1.6.
- A MongoDB Atlas account + organization. Generate **Programmatic API Keys** at:
  `https://cloud.mongodb.com/v2#/account/profile` → Programmatic API Keys → Create API Key with role `Organization Owner` (M1c) or `Organization Project Creator` (tighter).
- Decide on the dev IP allowlist (your home/office static IP, in CIDR form). M1c uses IP allowlisting; M1d/M2 will switch to VPC peering once the Fargate VPC exists.

---

## 1. Deploy the Cognito stack

```bash
cd infra/cloudformation

aws cloudformation deploy \
  --stack-name profiler-saas-auth-dev \
  --template-file auth.yml \
  --parameter-overrides Environment=dev \
  --no-fail-on-empty-changeset \
  --region us-east-1
```

Capture the outputs:

```bash
aws cloudformation describe-stacks \
  --stack-name profiler-saas-auth-dev \
  --region us-east-1 \
  --query 'Stacks[0].Outputs'
```

You'll get:
- `UserPoolId` → e.g. `us-east-1_AbC123dEf`
- `UserPoolClientId` → e.g. `7abc123def456ghi789jkl0mno`
- `UserPoolArn` (M5 cleanup job will need this)

---

## 2. Provision the Atlas M10 cluster

Atlas API keys go in env vars, never on disk:

```bash
# bash
export MONGODB_ATLAS_PUBLIC_KEY="..."
export MONGODB_ATLAS_PRIVATE_KEY="..."
export TF_VAR_db_password="$(openssl rand -base64 24)"   # save this somewhere safe
```
```powershell
# PowerShell
$env:MONGODB_ATLAS_PUBLIC_KEY  = "..."
$env:MONGODB_ATLAS_PRIVATE_KEY = "..."
$env:TF_VAR_db_password        = [Convert]::ToBase64String((1..24 | %{ Get-Random -Maximum 256 } | %{ [byte]$_ }))
```

Then:

```bash
cd infra/terraform/atlas

cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set atlas_org_id and ip_access_list

terraform init
terraform plan -out=atlas.plan
terraform apply atlas.plan
```

Cluster provisioning takes ~7–10 minutes. Capture outputs:

```bash
terraform output cluster_standard_srv
terraform output db_username
```

The full connection URI for the backend `appsettings.Development.json` is:

```
mongodb+srv://<db_username>:<db_password>@<srv_host>/matchlogic?retryWrites=true&w=majority
```

> URL-encode the password if it contains `@`, `:`, `/`, `?`, `#`, `[`, `]`. Easiest: use the `openssl rand -base64` form above, which uses `+/=`; only `+` and `=` need encoding (`%2B`, `%3D`).

---

## 3. Drop values into local config

### Frontend — `frontend/.env.local`

Create from the template:

```bash
cd frontend
cp .env.example .env.local
```

Fill in:

```
NEXT_PUBLIC_API_URL=http://localhost:7122/api
NEXT_PUBLIC_AUTH_MODE=cognito
NEXT_PUBLIC_COGNITO_USER_POOL_ID=us-east-1_AbC123dEf
NEXT_PUBLIC_COGNITO_CLIENT_ID=7abc123def456ghi789jkl0mno
NEXT_PUBLIC_COGNITO_REGION=us-east-1
```

### Backend — `backend/src/MatchLogic.Api/appsettings.Development.json`

Region is already defaulted to `us-east-1`. Fill in the IDs:

```json
{
  "Cognito": {
    "Region": "us-east-1",
    "UserPoolId": "us-east-1_AbC123dEf",
    "ClientId": "7abc123def456ghi789jkl0mno"
  }
}
```

If Mongo connection-string placement isn't already set elsewhere (M1c doesn't strictly require this for the Cognito smoke test, but you'll want it for M2), add to the same file under `MongoDbSettings` matching the existing config shape.

---

## 4. Smoke-test the auth loop

### 4a. Boot both servers

```bash
# Terminal 1
cd backend
dotnet run --project src/MatchLogic.Api/MatchLogic.Api.csproj

# Terminal 2
cd frontend
npm run dev
```

Expected: API on `http://localhost:7122`, FE on `http://localhost:3000`.

### 4b. Walk the flow

1. Open `http://localhost:3000` — should redirect to `/login` (no session).
2. Click "Sign up" → enter a real email you can check + a strong password.
3. Cognito sends a 6-digit code from `no-reply@verificationemail.com` (default sender). Check inbox/spam.
4. On `/verify`, paste the code. Should redirect to `/login` (or straight in, depending on the page wiring — verify by reading [verify/page.tsx](../frontend/src/app/(auth)/verify/page.tsx) for the post-confirm redirect target).
5. Sign in with the same email + password.
6. Should land on `/project-management`.

### 4c. Confirm the bearer token reaches the API

In the browser DevTools Network tab, find any XHR to `/api/...` and inspect the request headers. You should see:

```
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

Decode the JWT at jwt.io (paste the token; never paste production tokens into a public site, but a dev pool token is fine). Confirm:
- `iss` matches `https://cognito-idp.us-east-1.amazonaws.com/<UserPoolId>`
- `aud` (id token) or `client_id` (access token) matches the App Client ID
- `email_verified` is `true`

### 4d. Confirm the API accepts the token

Hit a protected endpoint:

```bash
TOKEN="eyJ..."   # paste from DevTools
curl -i http://localhost:7122/api/Project -H "Authorization: Bearer $TOKEN"
```

Expected: `200 OK` (or `404`/empty list — anything *not* `401`). A `401` means [CognitoJwtSetup.cs](../backend/src/MatchLogic.Api/Auth/CognitoJwtSetup.cs) couldn't validate; check the API console output for the validation failure reason.

---

## 5. Wrap up

If the smoke test passed:

- Update [MEMORY.md](../MEMORY.md) §15 Change Log + flip M1c to ✅ in §9.
- Commit only the **infra templates** (`infra/cloudformation/auth.yml`, `infra/terraform/atlas/*.tf`, the `.example`/`.gitignore`).
- Do **not** commit `frontend/.env.local`, `appsettings.Development.json` IDs (those are local-only), or `terraform.tfvars`/`terraform.tfstate` (Terraform's `.gitignore` blocks them).

If you're not moving straight to M1d:

```bash
cd infra/terraform/atlas
terraform destroy   # M10 stops billing
```

Cognito has no per-resource cost; leave it.

---

## Deferred to launch prep (not M1c)

- **SES integration.** Cognito's default sender caps at ~50 emails/day and uses an unfamiliar `no-reply@verificationemail.com` From address. Before any public traffic, verify a domain identity in SES, request production access, and reconfigure the User Pool's `EmailConfiguration` to `EmailSendingAccount: DEVELOPER` with the SES SourceArn.
- **Disposable email blocklist Lambda + Pre-Cognito hook** (M4 scope per [MEMORY.md §8](../MEMORY.md#8--aws-safeguards-m4--m5)).
- **VPC peering between Atlas and the Fargate VPC** (M1d/M2). The IP allowlist used here is dev-only.
- **MFA.** Off in M1c (`MfaConfiguration: OFF`). The product uses OTP-at-signup, not ongoing MFA.
