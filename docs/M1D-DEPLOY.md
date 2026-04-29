# M1d — Backend Deploy Runbook

Walks through getting `https://api.profiler.matchlogic.io/api/HealthCheck`
returning 200 OK from the live API on AWS Fargate.

This doc is broken into the seven sub-steps from the M1d kickoff. Each
section is self-contained — you do the clicks listed, paste any captured
values where the next section says, and move on. **Do not skip steps.**

## Prerequisites

Before starting, you should have:
- AWS account 274020917421 (the same one used for M1c — Cognito + the
  Atlas IP allowlist live there).
- A GitHub account with admin access to
  `github.com/missedhit/matchlogic-profiling-saas`.
- Docker Desktop installed locally (only needed for Step 1 — already done).

---

## Step 2 — ECR repository + GitHub Actions push pipeline

**Goal:** every commit to `main` that touches `backend/` automatically builds
the Docker image and pushes it to a private AWS container registry. The
ECS Fargate service in Step 5 will pull from that registry.

### 2.1 — Create the ECR repository (CloudFormation)

1. Open https://us-east-1.console.aws.amazon.com/cloudformation/home in a
   browser. Make sure the **region selector** in the top-right reads
   **N. Virginia (us-east-1)**. If it reads anything else, change it now.

2. Click the orange **Create stack** button → **With new resources
   (standard)**.

3. On the **Specify template** screen:
   - Choose **Upload a template file**.
   - Click **Choose file** and select
     `infra/cloudformation/ecr.yml` from this repo on your disk.
   - Click **Next**.

4. On the **Specify stack details** screen:
   - **Stack name:** `profiler-saas-ecr-dev`
   - Leave both parameters at their defaults (`Environment=dev`,
     `RepositoryName=matchlogic-profiling-saas/api`).
   - Click **Next**.

5. On the **Configure stack options** screen:
   - Scroll to the bottom. Leave everything default. Click **Next**.

6. On the **Review** screen:
   - Scroll down. Leave the **Capabilities** checkbox unchecked (no IAM
     resources in this template).
   - Click **Submit**.

7. Wait for the stack status to reach **CREATE_COMPLETE** (~30 seconds).
   Refresh the **Events** tab once or twice to track progress.

8. Click the **Outputs** tab. Copy the value of **RepositoryUri** to a
   notepad — it'll look like:
   ```
   274020917421.dkr.ecr.us-east-1.amazonaws.com/matchlogic-profiling-saas/api
   ```
   You'll need this for Step 5. **Don't close the tab yet.**

### 2.2 — Create the IAM user that GitHub Actions will use

This user has exactly one permission: push images to the ECR repo we just
made. No console access, no other privileges.

1. Open https://us-east-1.console.aws.amazon.com/iam/home → click **Users**
   in the left sidebar → click **Create user** (top-right).

2. **User details** screen:
   - **User name:** `github-actions-backend-deploy`
   - **Provide user access to AWS Management Console:** leave **unchecked**
     (this is a programmatic-only user).
   - Click **Next**.

3. **Permissions** screen:
   - Choose **Attach policies directly**.
   - Click **Create policy** (opens in a new tab).
   - In the new tab, click the **JSON** tab and replace the contents with:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Sid": "ECRGetAuth",
           "Effect": "Allow",
           "Action": "ecr:GetAuthorizationToken",
           "Resource": "*"
         },
         {
           "Sid": "ECRPushPull",
           "Effect": "Allow",
           "Action": [
             "ecr:BatchCheckLayerAvailability",
             "ecr:BatchGetImage",
             "ecr:CompleteLayerUpload",
             "ecr:GetDownloadUrlForLayer",
             "ecr:InitiateLayerUpload",
             "ecr:PutImage",
             "ecr:UploadLayerPart"
           ],
           "Resource": "arn:aws:ecr:us-east-1:274020917421:repository/matchlogic-profiling-saas/api"
         }
       ]
     }
     ```
   - Click **Next**.
   - **Policy name:** `profiler-saas-ecr-push`
   - Click **Create policy**.
   - Close that browser tab. Go back to the **Permissions** tab still open
     from step 2.
   - Click the **circular refresh icon** above the policy list (just to the
     left of the search box).
   - In the search box, type `profiler-saas-ecr-push` and check the box
     next to it.
   - Click **Next**.

4. **Review and create** screen:
   - Click **Create user**.

5. Back on the user list, click the name **github-actions-backend-deploy**
   to open it. Click the **Security credentials** tab.

6. Scroll down to **Access keys** → click **Create access key**.
   - **Use case:** select **Application running outside AWS**.
   - Tick the confirmation checkbox below. Click **Next**.
   - **Description tag** (optional): `github-actions-deploy`.
   - Click **Create access key**.

7. **CRITICAL — copy both values now:**
   - **Access key:** `AKIA...` (looks like a short string)
   - **Secret access key:** click **Show** then copy (long string)

   You will **not** be able to see the secret again after this screen.
   Paste both into a notepad, but **do not save them in the repo or in
   any cloud-synced file.** They go into GitHub Secrets in the next
   sub-step and can be deleted from the notepad after that.

   Click **Done**.

### 2.3 — Add the credentials to GitHub Secrets

1. Open
   https://github.com/missedhit/matchlogic-profiling-saas/settings/secrets/actions

2. Click **New repository secret** (green button, top right).
   - **Name:** `AWS_ACCESS_KEY_ID`
   - **Secret:** paste the Access key from step 2.2.7
   - Click **Add secret**.

3. Click **New repository secret** again.
   - **Name:** `AWS_SECRET_ACCESS_KEY`
   - **Secret:** paste the Secret access key from step 2.2.7
   - Click **Add secret**.

4. You can now delete both values from your notepad.

### 2.4 — Trigger the first deploy

The workflow runs automatically on push to `main` if anything under
`backend/` changes. To force-trigger it now without code changes:

1. Open https://github.com/missedhit/matchlogic-profiling-saas/actions
2. Click **Backend Deploy** in the left sidebar.
3. Click the **Run workflow** dropdown (right side) → **Run workflow**
   (the green button).
4. Wait ~3-5 min for it to complete (look for the green ✅).

If it fails:
- **AWS credentials error:** double-check the secrets in step 2.3 — the
  most common mistake is pasting an extra space.
- **AccessDenied on ECR push:** the IAM policy ARN is scoped to
  account `274020917421`. If your account ID is different (check the
  CloudFormation **Outputs** tab, the RepositoryUri starts with the
  account ID), edit the policy in step 2.2.3 to match.

### 2.5 — Verify the image is in ECR

1. Open
   https://us-east-1.console.aws.amazon.com/ecr/repositories/private
2. Click `matchlogic-profiling-saas/api`.
3. You should see two image tags: `latest` and `sha-<some-git-hash>`.
   Image size ~155 MB compressed (~414 MB uncompressed). ✅

---

## Step 2 — Done. What's live now

- ECR repo `matchlogic-profiling-saas/api` with the API image inside.
- A GitHub Actions workflow that auto-pushes a new image on every commit
  to `main` touching `backend/`.
- A scoped IAM user with push-only permission.

---

## Step 3 — Parameter Store secrets

**Goal:** the seven config values currently in
`backend/src/MatchLogic.Api/appsettings.Development.json` need to be moved
into AWS Systems Manager Parameter Store (encrypted at rest with the
AWS-managed KMS key) so the ECS Fargate task definition can inject them
into the container at runtime as environment variables.

We're using **Parameter Store SecureString** (free) rather than Secrets
Manager (~$0.40/secret/mo). For a dev environment with seven secrets,
that's $0 vs ~$33/yr — Parameter Store is plenty.

### 3.1 — Have your local `appsettings.Development.json` open

You'll be copying values from it. The file is at
`backend/src/MatchLogic.Api/appsettings.Development.json` on your disk.
Don't close it until Step 3 is fully done.

The seven values you'll need are:
- `Cognito.Region` (e.g. `us-east-1`)
- `Cognito.UserPoolId` (e.g. `us-east-1_kN55XX1J3`)
- `Cognito.ClientId` (e.g. `7shrdvau1keked8jss22flblki`)
- `MongoDB.ConnectionString` — long string starting with `mongodb+srv://`
- `MongoDB.DatabaseName` (e.g. `matchlogic`)
- `MongoDB.Progress.ConnectionString` — also `mongodb+srv://`
- `MongoDB.Progress.DatabaseName` (e.g. `matchlogic_progress`)

### 3.2 — Create the parameters in Systems Manager

1. Open
   https://us-east-1.console.aws.amazon.com/systems-manager/parameters
   (region must say **N. Virginia** in top-right).

2. Click **Create parameter** (orange button, top-right).

3. Repeat the following 7 times, once per row:

   | Name (paste verbatim) | Type | Value |
   |---|---|---|
   | `/profiler-saas/dev/cognito/region` | SecureString | `us-east-1` |
   | `/profiler-saas/dev/cognito/user-pool-id` | SecureString | `us-east-1_kN55XX1J3` (or your value) |
   | `/profiler-saas/dev/cognito/client-id` | SecureString | `7shrdvau1keked8jss22flblki` (or your value) |
   | `/profiler-saas/dev/mongodb/connection-string` | SecureString | the full `mongodb+srv://...` URI from your local file |
   | `/profiler-saas/dev/mongodb/database-name` | SecureString | `matchlogic` |
   | `/profiler-saas/dev/mongodb/progress-connection-string` | SecureString | the second `mongodb+srv://...` URI |
   | `/profiler-saas/dev/mongodb/progress-database-name` | SecureString | `matchlogic_progress` |

   For each parameter:
   - **Name:** paste from the table above (note the leading `/`, all lowercase, slash-separated).
   - **Description:** leave blank (optional).
   - **Tier:** **Standard** (the only free option — sufficient up to 4 KB).
   - **Type:** select **SecureString** (radio button).
   - **KMS key source:** **My current account** (default).
   - **KMS Key ID:** leave at **alias/aws/ssm** (the AWS-managed default key — also free).
   - **Value:** paste the actual value from your `appsettings.Development.json` file. For Cognito.Region just type `us-east-1`. For Mongo connection strings, copy the entire string including the password.
   - Click **Create parameter** at the bottom. You'll land back on the parameter list.

   **Then click Create parameter again for the next row.** The form
   resets between creates — you have to re-enter Tier and Type each time.

4. After all 7 are created, the parameter list filtered by `/profiler-saas/dev/`
   should show seven rows. **All Type column entries must read SecureString.**
   If any read just "String", click into it → Edit → change Type to
   SecureString → Save.

### 3.3 — Verify one parameter decrypts

Just to confirm the values were stored correctly:

1. Click any parameter name in the list (e.g. `/profiler-saas/dev/cognito/region`).
2. Click the **Show** button next to the masked value.
3. The plaintext should appear (`us-east-1` for that one).

If the **Show** button doesn't work, the parameter may have been created
as a `String` instead of `SecureString` — recreate it.

### 3.4 — Done with Step 3

Total parameters: 7. Total cost: $0/mo. The ECS task definition we
write in Step 5 will reference these by ARN and inject their plaintext
values as environment variables into the container at startup.

---

## Steps 4 + 5 — VPC, ECS Fargate cluster, ALB

**Goal:** the API container actually runs in AWS, listening on a public
ALB hostname. After this step you'll be able to `curl` a long
AWS-generated URL and get `200 OK` back from `/api/HealthCheck/`.

These two steps are bundled into one CloudFormation template
(`infra/cloudformation/compute.yml`) because they share resources and
only one environment exists. We'll split them later if we add staging.

**Cost while running:** ~$40/mo (~$17 Fargate + ~$22 ALB + ~$1 logs).
The runbook section 5.5 covers how to pause / fully tear down.

### 4.1 — Open the Atlas IP allowlist

The Fargate task will connect to your Atlas cluster. Fargate's public IP
rotates between restarts, so we need a wildcard in the Atlas allowlist
for now. (For launch we'll switch to VPC peering — see launch-prep.)

1. Open https://cloud.mongodb.com → log in → click your project
   `profiler-saas-dev`.
2. Left sidebar → **Network Access**.
3. Click **+ Add IP Address** (top-right).
4. **Access List Entry:** `0.0.0.0/0` (yes, all-zeroes).
5. **Comment:** `Fargate dev — REMOVE BEFORE LAUNCH`.
6. Click **Confirm**.

The list should now contain two entries: your home IP (or whatever was
there) and `0.0.0.0/0`. **Don't remove the existing entries** — your
local backend still needs them.

### 4.2 — Create the compute stack (CloudFormation)

1. Open https://us-east-1.console.aws.amazon.com/cloudformation/home →
   verify region reads **N. Virginia**.

2. Click **Create stack** → **With new resources (standard)**.

3. **Specify template** screen:
   - Choose **Upload a template file**.
   - Click **Choose file** → select `infra/cloudformation/compute.yml`.
   - Click **Next**.

4. **Specify stack details** screen:
   - **Stack name:** `profiler-saas-compute-dev`
   - Leave all parameters at their defaults — `EcrRepositoryUri` already
     points at your dev ECR repo's `:latest` tag.
   - Click **Next**.

5. **Configure stack options** screen:
   - Scroll to the bottom. **Capabilities** section — check the box
     **"I acknowledge that AWS CloudFormation might create IAM resources
     with custom names."**
     *(Required because the template creates the task execution role and
     task role — both IAM. Without this checkbox the stack create will
     fail with `Requires capabilities: [CAPABILITY_IAM]`.)*
   - Click **Next**.

6. **Review** screen:
   - Scroll to the bottom and click **Submit**.

7. Wait for the stack to reach **CREATE_COMPLETE** (~3-5 min). The
   slowest resource is the ALB (~2 min to provision). Refresh the
   **Events** tab while it works — you'll see Vpc → Subnets → ALB →
   TargetGroup → TaskDefinition → Service in roughly that order.

   If the stack fails with `RESOURCE_FAILED` on the ECS Service, the
   most common cause is that the `:latest` image isn't in ECR yet —
   verify the GitHub Actions workflow finished (Step 2.4) before
   creating this stack.

### 4.3 — Capture the outputs

Click the **Outputs** tab. Three values matter; copy them to your notepad.

| Key | What to do with it |
|---|---|
| `AlbDnsName` | Used in the next sub-step to test. Looks like `profiler-saas-dev-alb-1234567890.us-east-1.elb.amazonaws.com` |
| `AlbHostedZoneId` | Needed for Route 53 alias records in step 7. |
| `AlbArn` | Listener-443 (step 6) attaches here. |

### 4.4 — Wait for the task to become healthy

The Service is created by CloudFormation, but the first Fargate task
takes another ~60-90 seconds to:
1. Pull the image from ECR (~15 sec)
2. Start the .NET runtime (~10 sec)
3. Connect to Mongo Atlas (~5 sec)
4. Pass two consecutive health checks (~60 sec — we configured 30s
   intervals, requiring 2 healthy)

To watch progress:

1. Open https://us-east-1.console.aws.amazon.com/ecs/v2/clusters/profiler-saas-dev/services
2. Click the service `profiler-saas-dev-api`.
3. **Tasks** tab — you should see one task in **PROVISIONING** →
   **PENDING** → **RUNNING**. It's RUNNING when the container started,
   but not yet HEALTHY.
4. Click **Last status** column header to refresh. Wait for the
   **Health status** to read **Healthy** (~2 min after RUNNING).

If the task flips between RUNNING and STOPPED with task IDs changing,
the container is crashing. Click into a STOPPED task → **Logs** tab to
see why. Common causes:
- **Mongo connection refused / timeout:** Atlas allowlist still hasn't
  propagated, or the connection string in SSM is malformed (extra
  whitespace, missing query params).
- **Container exited with code 1 immediately:** likely a missing
  required env var. Check the task's environment variables for any
  `Cognito__*` or `MongoDB__*` reading as `(empty)`.

### 4.5 — Verify with curl

From any terminal:

```bash
curl -i http://<paste-AlbDnsName-here>/api/HealthCheck/
```

Expected response:

```
HTTP/1.1 200 OK
Content-Length: 4
Content-Type: text/plain; charset=utf-8

"OK"
```

🎉 If you got that, **the API is live on AWS**. The hostname is ugly
(it's the auto-generated ALB DNS) — steps 6+7 swap that for
`api.profiler.matchlogic.io`.

### 4.6 — Pause / tear down

While the stack runs, you pay ~$40/mo. Three options to reduce cost:

**Pause Fargate only (~$22/mo for ALB):** edit the stack →
**Update stack** → re-upload `compute.yml` → set `DesiredCount` to `0`.
The task stops; the ALB stays. Resume by setting it back to `1`.

**Full pause ($0):** **Delete stack**. All resources go away. Recreate
later by repeating section 4.2.

**Keep running (~$40/mo):** the simplest, leaves the API reachable
24/7 for testing.

---

---

## Step 6 — ACM certificate (DNS validation via Cloudflare)

**Goal:** issue a public TLS certificate covering `*.profiler.matchlogic.io`,
attach it to the ALB on a new HTTPS listener, and switch port 80 to
redirect to HTTPS. After this step the ALB's auto-generated DNS name
serves HTTPS (with a hostname-mismatch warning, which is fine — step 7
gives it the friendly name).

DNS for `matchlogic.io` lives at Cloudflare. ACM uses DNS validation
(adds a CNAME, you copy it to Cloudflare, ACM polls until it sees the
record, cert issues).

### 6.1 — Request the certificate in ACM

1. Open https://us-east-1.console.aws.amazon.com/acm/home → region must
   read **N. Virginia**. ACM certificates are region-specific and the
   ALB lives in us-east-1, so the cert must too.

2. Click **Request a certificate** (orange button).

3. **Certificate type:** select **Request a public certificate**. Click **Next**.

4. **Domain names** — add a single entry:
   - Click **Add another name to this certificate** isn't needed; the
     first input box is enough.
   - **Fully qualified domain name:** `*.profiler.matchlogic.io`
   *(the wildcard covers `api.profiler.matchlogic.io` plus any other
   subdomain we add later — `app.profiler.matchlogic.io` for the
   frontend, etc.)*

5. **Validation method:** **DNS validation** (default — keep it).

6. **Key algorithm:** **RSA 2048** (default — keep it).

7. Leave tags empty. Click **Request**.

8. You land on the cert list. Status reads **Pending validation**.
   Click the cert ID (a UUID-looking string) to open its detail page.

### 6.2 — Capture the validation CNAME

On the cert detail page, scroll down to the **Domains** section. You'll
see one row with these columns:

| Domain | Validation status | CNAME name | CNAME value |
|---|---|---|---|
| *.profiler.matchlogic.io | Pending validation | `_xxxxxxxx.profiler.matchlogic.io.` | `_yyyyyyyy.acm-validations.aws.` |

Both the **CNAME name** and **CNAME value** are needed. Copy both to a
notepad. The exact strings are unique per cert request.

### 6.3 — Add the CNAME at Cloudflare

1. Open https://dash.cloudflare.com → log in → click `matchlogic.io`.
2. Left sidebar → **DNS** → **Records**.
3. Click **Add record** (top of the records table).
4. Fill in:
   - **Type:** `CNAME`
   - **Name:** the **CNAME name** value from ACM, **but strip the
     `.profiler.matchlogic.io.` suffix off the end**. So if ACM says
     `_abc123.profiler.matchlogic.io.`, you only paste `_abc123.profiler`.
     *(Cloudflare automatically appends the zone name; if you paste the
     full string Cloudflare will create
     `_abc123.profiler.matchlogic.io.matchlogic.io` which won't validate.)*
   - **Target:** the full **CNAME value** from ACM, **trailing dot
     optional**. So paste `_yyyyyyyy.acm-validations.aws` (with or
     without the trailing dot — Cloudflare normalizes it).
   - **Proxy status:** **DNS only** (gray cloud — important; orange
     cloud breaks ACM validation).
   - **TTL:** Auto (default).
5. Click **Save**.

### 6.4 — Wait for the cert to issue

Back in the ACM cert detail page, click the refresh icon next to the
cert status every minute or so. Validation usually completes in
**5-15 min**. Status will move from **Pending validation** → **Issued**.

If it stays "Pending validation" for >30 min, the most common cause is
the CNAME name was pasted with the wrong scope (full vs stripped — see
6.3). Re-check the Cloudflare record and edit if needed.

### 6.5 — Capture the certificate ARN

Once issued, on the cert detail page copy the **ARN** value at the
top — it looks like:

```
arn:aws:acm:us-east-1:274020917421:certificate/abcd1234-ef56-7890-abcd-1234567890ab
```

You'll paste this into the next sub-step.

### 6.6 — Update the compute stack with the cert ARN

1. Open https://us-east-1.console.aws.amazon.com/cloudformation/home →
   click `profiler-saas-compute-dev`.

2. Click **Update stack** (top-right) → **Make a direct update**.

3. **Prepare template:** select **Use existing template** → click **Next**.

4. **Specify stack details:** find the **CertificateArn** parameter
   (currently empty). Paste the ARN from step 6.5 into the field.
   Leave all other parameters unchanged. Click **Next**.

5. **Configure stack options:** scroll to the bottom and click **Next**
   (no IAM acknowledgement needed this time — we're not changing IAM).

6. **Review:** scroll down. Under **Changes**, you should see two
   actions:
   - `HttpListener` — Modify (changes redirect from HTTPS)
   - `HttpsListener` — Add

   If the change preview shows anything else (cluster modify, service
   modify, etc.), stop and paste the change set here so we can check
   before applying.

7. Click **Submit**.

8. Wait for **UPDATE_COMPLETE** (~1 min — listeners are fast).

### 6.7 — Verify HTTPS is live

The ALB DNS name (long `*.elb.amazonaws.com` URL) now serves HTTPS,
but with a **hostname mismatch warning** because the cert is for
`*.profiler.matchlogic.io` not for the ALB's auto-generated name.
That's expected — step 7 fixes it. To test that HTTPS is wired:

```bash
curl -ki https://<alb-dns>/api/HealthCheck/
```

The `-k` flag tells curl to ignore the cert mismatch warning. You
should still get `HTTP/1.1 200 OK` with body `OK`. The redirect
behavior is testable too:

```bash
curl -i http://<alb-dns>/api/HealthCheck/
```

That should now return `HTTP/1.1 301 Moved Permanently` with a
`Location: https://<alb-dns>/api/HealthCheck/` header.

---

## Step 7 — Custom hostname via Cloudflare DNS

**Goal:** `https://api.profiler.matchlogic.io/api/HealthCheck/`
returns 200 OK. This is the M1d exit criterion.

### 7.1 — Add the CNAME at Cloudflare

1. Open https://dash.cloudflare.com → `matchlogic.io` → **DNS** →
   **Records** → **Add record**.

2. Fill in:
   - **Type:** `CNAME`
   - **Name:** `api.profiler` *(Cloudflare appends `.matchlogic.io`
     to make `api.profiler.matchlogic.io`)*
   - **Target:** the **AlbDnsName** from step 4.3 — e.g.
     `profiler-saas-dev-alb-2006153364.us-east-1.elb.amazonaws.com`
     (no `http://`, no trailing slash).
   - **Proxy status:** **DNS only** (gray cloud). Orange-cloud (proxied)
     would route via Cloudflare's CDN, which adds complexity (Cloudflare
     terminates HTTPS with its own cert and re-fetches from the ALB).
     For now keep it simple — DNS only.
   - **TTL:** Auto.

3. Click **Save**.

### 7.2 — Wait for DNS to propagate (~1-5 min)

Cloudflare DNS propagates fast, usually under a minute. To confirm:

```bash
nslookup api.profiler.matchlogic.io
```

Output should show a CNAME chain ending at `*.elb.amazonaws.com`.

### 7.3 — Final smoke test

```bash
curl -i https://api.profiler.matchlogic.io/api/HealthCheck/
```

Expected:

```
HTTP/1.1 200 OK
Server: Kestrel

OK
```

🎉 **M1d done.** The API is live at `https://api.profiler.matchlogic.io`.

### 7.4 — What's next

- **M2** — file upload flow (S3 presigned PUTs, profile job triggers
  end-to-end against the deployed API).
- **M3** — `POST /api/DataProfile/Export/{projectId}/{dataSourceId}`
  (CSV/JSON export endpoint + UI button).
- **Pre-launch** — replace `0.0.0.0/0` Atlas allowlist with VPC peering
  or static NAT IP, migrate IAM static keys to OIDC federation, set
  Cognito deletion protection ON, wire SES properly.

---

## Step 8 — Auto-redeploy on push to main

**Goal:** every commit to `main` that touches `backend/` not only pushes
a new image to ECR but also tells ECS to pull it and restart the task.
After this, your code-to-production loop is one `git push`.

The workflow file changes (`Force ECS service redeploy` + `Wait for
service stability` steps) are already committed. The only thing missing
is permissions — the `github-actions-backend-deploy` IAM user can push
to ECR but can't yet call ECS.

### 8.1 — Add the ECS deploy policy

1. Open https://us-east-1.console.aws.amazon.com/iam/home → left sidebar
   → **Policies** → **Create policy** (orange button).

2. Click the **JSON** tab and replace the contents with:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "EcsForceRedeploy",
         "Effect": "Allow",
         "Action": [
           "ecs:UpdateService",
           "ecs:DescribeServices"
         ],
         "Resource": "arn:aws:ecs:us-east-1:274020917421:service/profiler-saas-dev/profiler-saas-dev-api"
       }
     ]
   }
   ```

3. Click **Next**.
   - **Policy name:** `profiler-saas-ecs-deploy`
   - Click **Create policy**.

### 8.2 — Attach it to the deploy user

1. Left sidebar → **Users** → click `github-actions-backend-deploy`.
2. **Permissions** tab → **Add permissions** → **Attach policies directly**.
3. Search for `profiler-saas-ecs-deploy`. Tick the checkbox.
4. **Next** → **Add permissions**.

The user now has both policies: `profiler-saas-ecr-push` (existing) +
`profiler-saas-ecs-deploy` (new).

### 8.3 — Verify

The next push to main touching `backend/` will run the full pipeline:
build → push → force redeploy → wait for stability. ~5-7 min total.

To force-trigger now without code changes:

1. https://github.com/missedhit/matchlogic-profiling-saas/actions
2. **Backend Deploy** → **Run workflow** → **Run workflow**.

Watch the run. The "Force ECS service redeploy" step should pass; the
"Wait for service stability" step blocks until the new task is healthy
and the old one drained (~2-3 min). If either fails:
- **AccessDenied on UpdateService:** the policy didn't attach — recheck
  step 8.2.
- **`waiter ServicesStable failed`:** the new task is unhealthy.
  Check the ECS service event log and the CloudWatch log group
  `/ecs/profiler-saas-dev/api`.
