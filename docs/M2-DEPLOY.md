# M2 — S3 Upload Flow Deploy Runbook

Walks through wiring file uploads through S3 instead of local disk.

This doc is broken into the same eleven sub-steps from the M2 kickoff.
Each section is self-contained — you do the clicks listed, paste any
captured values where the next section says, and move on. **Do not
skip steps.**

## Prerequisites

You should already have (all from M1d):
- AWS account 274020917421 with console access.
- Working AWS CLI on your machine (used in M1d for `aws ecs update-service`).
- The seven SSM SecureString parameters under `/profiler-saas/dev/` from M1d.

---

## Step 1 — S3 bucket (CloudFormation)

**Goal:** create one private S3 bucket with two key prefixes
(`uploads/` and `results/`), lifecycle rules to auto-expire content
(7d / 30d), and CORS rules that let the browser PUT directly to it.

### 1.1 — Deploy the storage stack

1. Open https://us-east-1.console.aws.amazon.com/cloudformation/home in
   a browser. Make sure the **region selector** in the top-right reads
   **N. Virginia (us-east-1)**. If it reads anything else, change it now.

2. Click the orange **Create stack** button → **With new resources
   (standard)**.

3. On the **Specify template** screen:
   - Choose **Upload a template file**.
   - Click **Choose file** and select
     `infra/cloudformation/storage.yml` from this repo on your disk.
   - Click **Next**.

4. On the **Specify stack details** screen:
   - **Stack name:** `profiler-saas-storage-dev`
   - Leave all three parameters at their defaults
     (`Environment=dev`, `LocalDevOrigin=http://localhost:3000`,
     `ProductionFrontendOrigin=https://app.profiler.matchlogic.io`).
   - Click **Next**.

5. On the **Configure stack options** screen:
   - Scroll to the bottom. Leave everything default. Click **Next**.

6. On the **Review** screen:
   - Scroll down. Leave the **Capabilities** checkbox unchecked (no IAM
     resources in this template).
   - Click **Submit**.

7. Wait for the stack status to reach **CREATE_COMPLETE** (~20 seconds).
   Refresh the **Events** tab once or twice to track progress.

8. Click the **Outputs** tab. Copy the value of **BucketName** to a
   notepad — it'll look like:
   ```
   profiler-saas-uploads-dev-274020917421
   ```
   You'll need this for the next sub-step. **Don't close the tab yet.**

### 1.2 — Create the SSM SecureString parameters

These two parameters tell the API which bucket to use. Storing them in
SSM (instead of hardcoding into the task definition) means we can swap
the bucket later without re-deploying the container.

1. Open https://us-east-1.console.aws.amazon.com/systems-manager/parameters
   in a new tab. (Region must still be **N. Virginia (us-east-1)**.)

2. Click the orange **Create parameter** button (top-right).

3. **First parameter — bucket name:**
   - **Name:** `/profiler-saas/dev/s3/uploads-bucket`
   - **Tier:** **Standard**
   - **Type:** **SecureString**
   - **KMS key source:** **My current account**
   - **KMS Key ID:** leave the default (`alias/aws/ssm`).
   - **Value:** paste the bucket name you copied above
     (e.g. `profiler-saas-uploads-dev-274020917421`). No quotes, no
     trailing spaces.
   - Click **Create parameter**.

4. Click **Create parameter** again. **Second parameter — region:**
   - **Name:** `/profiler-saas/dev/s3/region`
   - **Tier:** **Standard**
   - **Type:** **SecureString**
   - **KMS key source:** **My current account** (default key).
   - **Value:** `us-east-1`
   - Click **Create parameter**.

5. The parameter list should now show **9** parameters under
   `/profiler-saas/dev/` (the original 7 plus these 2).

### 1.3 — Sanity check

Run this in your local terminal (the same one where `aws s3 ls` works
from M1d):

```bash
aws s3 ls s3://profiler-saas-uploads-dev-274020917421/
```

Expected output: nothing (bucket is empty). No errors. If you see
`AccessDenied`, something is off — stop and tell me.

Then verify the SSM params readback:

```bash
aws ssm get-parameters --with-decryption \
  --names /profiler-saas/dev/s3/uploads-bucket /profiler-saas/dev/s3/region \
  --query 'Parameters[].[Name,Value]' --output table
```

Expected: a 2-row table showing the bucket name and `us-east-1`.

---

## Step 2 — ECS task role gets S3 permissions (CloudFormation update)

**Goal:** the API container running in Fargate currently has zero AWS
permissions — it can talk to Cognito and Mongo over the public internet
and that's it. We need to grant it `s3:GetObject` / `s3:PutObject` /
`s3:DeleteObject` on the uploads bucket so the API can stream files
in/out, plus `s3:ListBucket` for HEAD checks. The change goes onto the
existing `profiler-saas-compute-dev` stack as a stack update.

### 2.1 — Update the compute stack

1. Open https://us-east-1.console.aws.amazon.com/cloudformation/home and
   click on the `profiler-saas-compute-dev` stack in the stack list.

2. Click the orange **Update** button (top-right) → **Update stack**.

3. On the **Prerequisite — Prepare template** screen:
   - Choose **Replace existing template**.
   - Choose **Upload a template file**.
   - Click **Choose file** and select
     `infra/cloudformation/compute.yml` from this repo on your disk.
   - Click **Next**.

4. On the **Specify stack details** screen:
   - All previous parameter values are already filled in. There is now
     **one new parameter**: **UploadsBucketName**. The default value
     should already be `profiler-saas-uploads-dev-274020917421`.
   - Leave everything else untouched.
   - Click **Next**.

5. On the **Configure stack options** screen, scroll to the bottom.
   Leave everything default. Click **Next**.

6. On the **Review** screen:
   - Scroll to the bottom. CloudFormation will ask you to acknowledge
     **two capability checkboxes**:
     - ☑ I acknowledge that AWS CloudFormation might create IAM resources.
     - ☑ I acknowledge that AWS CloudFormation might create IAM resources
       with custom names.
   - The "Change set preview" section will show **TaskRole** as the
     resource being modified, with the new inline policy added. **No
     other resources should change.** If it tries to replace the ECS
     service or task definition, stop and tell me — that would mean
     the parameter wasn't picked up correctly.
   - Click **Submit**.

7. Wait for the stack status to reach **UPDATE_COMPLETE** (~30 seconds).
   The ECS service does NOT need to redeploy for this — IAM role changes
   take effect on the next API call the container makes.

### 2.2 — Sanity check via console

1. Open https://us-east-1.console.aws.amazon.com/iam/home → **Roles** in
   the left sidebar.

2. Find the role with `profiler-saas-compute-dev-TaskRole-` in the name
   (the suffix is auto-generated, will look like
   `profiler-saas-compute-dev-TaskRole-ABC123XYZ`). Click into it.

3. The **Permissions policies** tab should now show one inline policy
   named **AccessUploadsBucket**. Click it → **{ } JSON** → confirm the
   policy lists `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on
   the bucket and `s3:ListBucket` on the bucket itself.

If it looks right, you're done with Step 2. The container won't actually
USE these permissions until we add S3 code to the API in Step 3.

---

## Steps 3–8 — Backend + Frontend code (no manual clicks)

Steps 3 through 8 are entirely code edits + an XLSX file generated locally —
nothing for you to click. Summary of what landed:

- **Step 3** — `AWSSDK.S3` re-added; new `IFileStorageService` + `S3FileStorageService`;
  S3 config block in appsettings; container picks up bucket name + region from
  the two new SSM params via the updated `compute.yml`.
- **Step 4** — `POST /api/dataimport/File/PresignedUpload` mints a 5-minute
  presigned PUT URL.
- **Step 5** — `POST /api/dataimport/File/Confirm` verifies the S3 object,
  persists the `FileImport` doc, returns the same shape as the legacy upload.
- **Step 6** — `IFileSourceResolver` + per-call temp-file lease. `DataImportCommand`,
  `PreviewTablesHandler`, `PreviewColumnsHandler`, `PreviewDataHandler` all use it.
  `BaseConnectionInfoHandler` removed (replaced by `ConnectionInfoConfigurator`).
- **Step 7** — `useUploadFileMutation` rewritten to the 3-step flow
  (PresignedUpload → S3 PUT → Confirm). Downstream calls now pass `FileId`
  instead of `FilePath`.
- **Step 8** — Three smoke-test fixtures landed under `test-fixtures/`:
  `sample-5-rows.csv` (5 rows × 5 cols), `sample-100-rows.csv` (100 × 8),
  `sample-50-rows.xlsx` (50 × 5, single sheet "Inventory").

---

## Step 9 — Deploy backend (push to main)

**Goal:** push the M2 backend changes to `main`. The GitHub Actions workflow
auto-builds the Docker image, pushes it to ECR, and force-redeploys the
ECS service with the new task definition (which now pulls `S3:BucketName`
and `S3:Region` from SSM).

I'll run the commit + push from this session. The auto-deploy workflow does
the rest (~5 min push-to-prod). You'll watch the run in GitHub Actions.

Verify when done:
1. Open https://github.com/missedhit/matchlogic-profiling-saas/actions and watch
   the latest **backend-deploy** workflow turn green.
2. Curl the deployed API:
   ```
   curl https://api.profiler.matchlogic.io/api/HealthCheck/
   ```
   Expected: `200 OK / "OK"`.
3. Check that the new endpoints are reachable (will return 401 because no
   auth header — that's the right answer):
   ```
   curl -i -X POST https://api.profiler.matchlogic.io/api/dataimport/File/PresignedUpload
   ```
   Expected: `401 Unauthorized` (NOT 404 — 404 would mean the endpoint isn't
   wired or the deploy didn't land).

---

## Step 10 — End-to-end smoke test

**Goal:** prove the full upload → profile flow works against the deployed API
using the 3 fixtures.

1. Make sure the local frontend is running and pointed at the deployed API.
   Check `frontend/.env.local` has:
   ```
   NEXT_PUBLIC_API_URL=https://api.profiler.matchlogic.io
   ```
   (If it points at `localhost:7122`, update it and restart `npm run dev`.)

2. Open http://localhost:3000 in incognito, log in.

3. Create a project (or use existing).

4. Upload `test-fixtures/sample-5-rows.csv` via the upload flow.
   - Open browser DevTools → Network tab.
   - You should see THREE requests in order:
     - `POST /dataimport/File/PresignedUpload` → 200 with `presignedUrl`
     - `PUT https://...amazonaws.com/...` → 200 (this one is the actual upload)
     - `POST /dataimport/File/Confirm` → 200 with the file metadata
   - Then a normal `POST /dataimport/Preview/Tables` (with `FileId` in payload).

5. Continue through column-mapping → run profile → confirm results render.

6. Repeat for `sample-100-rows.csv` and `sample-50-rows.xlsx`.

7. Open https://us-east-1.console.aws.amazon.com/s3/buckets and click into
   `profiler-saas-uploads-dev-274020917421` → Objects tab → there should be
   three files in `uploads/` with the GUID-named keys you saw in DevTools.

If anything fails, paste the failing request/response from DevTools and we
debug from there.

---

## Step 11 — Memory + change log

I'll update `MEMORY.md` (§15 Change Log + §16 Lessons Learned) and add a new
auto-memory file `m2_dev_resources.md` recording the bucket name, ARN, key
prefixes, and CORS origins. No clicks for you on this step.
