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

**Next: Step 3 — Parameter Store secrets**, where we'll stash the
Cognito + Atlas connection strings as encrypted env vars that the
ECS Fargate task definition will inject into the container at runtime.

(Steps 3–7 will be added to this doc as we reach them.)
