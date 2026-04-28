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

**Next: Step 4 — VPC + networking**, where we'll create the network
plumbing that the Fargate task and ALB will live inside.
