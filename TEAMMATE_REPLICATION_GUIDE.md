# FlowSentinel AI — Teammate Replication Guide (Windows)

This walks a teammate through running the FlowSentinel AI Ops Dashboard on their own
Windows machine, then lists what's left to take it from "runs on my laptop" to a
production Cloud Run deployment.

Repo: `LatentView-Analytics-Ltd/ctech-flowsentinel-demo` · GCP Project: `ctech-flowsentinel-ai`

---

## Part 1 — Run it locally on Windows

### Step 1: Get access first

Before installing anything, request:

1. **GCP IAM access** on `ctech-flowsentinel-ai` — ask whoever has Owner/IAM Admin
   access (see [ADMIN_ACCESS_REQUEST.md](ADMIN_ACCESS_REQUEST.md) for who that is) to run:
   ```bash
   PROJECT=ctech-flowsentinel-ai
   for ROLE in roles/composer.user roles/bigquery.dataEditor roles/storage.objectAdmin \
               roles/logging.viewer roles/aiplatform.user roles/secretmanager.secretAccessor \
               roles/pubsub.viewer; do
     gcloud projects add-iam-policy-binding $PROJECT \
       --member="user:<your-email>@latentview.com" --role="$ROLE" --condition=None
   done
   ```
2. **GitHub repo access** — an org admin adds you as a Collaborator with **Write**
   access under `Settings → Collaborators and teams` on
   `LatentView-Analytics-Ltd/ctech-flowsentinel-demo`.

### Step 2: Install prerequisites

| Tool | Windows install | Verify |
|---|---|---|
| **Git** | [git-scm.com/download/win](https://git-scm.com/download/win) | `git --version` |
| **Python 3.11+** | [python.org/downloads](https://python.org/downloads) — check "Add python.exe to PATH" during install | `python --version` |
| **Google Cloud SDK** | [cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install) (Windows installer) | `gcloud --version` |

### Step 3: Authenticate gcloud

```powershell
gcloud init
gcloud auth login
gcloud auth application-default login
gcloud config set project ctech-flowsentinel-ai
```
The middle command opens a browser sign-in twice — once for the CLI, once for
Application Default Credentials (ADC), which is what the dashboard's Python code
actually uses to reach BigQuery, GCS, Secret Manager, and Vertex AI Gemini.

### Step 4: Clone and configure

```powershell
git clone https://github.com/LatentView-Analytics-Ltd/ctech-flowsentinel-demo.git
cd ctech-flowsentinel-demo
```

No `.env` file to fill in — every setting has a working default (see
[run_dashboard.ps1](run_dashboard.ps1)) pointing at the shared `ctech-flowsentinel-ai`
project. Override any of these first if you need to point somewhere else:
```powershell
$env:GCP_PROJECT_ID = "ctech-flowsentinel-ai"
$env:GITHUB_OWNER = "LatentView-Analytics-Ltd"
$env:GITHUB_REPO = "ctech-flowsentinel-demo"
$env:DASHBOARD_ADMIN_KEY = "test123"   # gates Production Mode actions only
```

### Step 5: Run it

```powershell
powershell -ExecutionPolicy Bypass -File .\run_dashboard.ps1
```
First run creates a `dashboard\.venv` virtual environment and installs dependencies —
takes a minute. Every run after that starts in a couple seconds.

If PowerShell blocks the script with an execution-policy error even with `-Bypass`,
run this once per machine (not a global security downgrade, just your user account,
just for scripts you run directly):
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### Step 6: Verify

Open **http://localhost:3000**. You should see:
- **Summary tab**: pipeline health grid + table freshness (real GCS/BigQuery calls once ADC is set up; sample data otherwise — never blank)
- **Demo tab**: toggle in the header top-right reads **Demo Mode** by default — leave it there for a first run, it never touches real GCP/GitHub
- Click **🎬 Begin Demo**, then go to **Ticket Board** → **🧭 Run Issue Segment Agent** → click into a ticket to run its fix/analysis

If the Summary tab shows GCS/GitHub errors instead of data, that's expected until
Step 1's IAM grant and a valid GitHub PAT are in place — Demo Mode still works
regardless, since it never calls those services.

---

## Part 2 — Next: deploy to Cloud Run (production)

This is **blocked on one thing today**: the project has no default Cloud Build
service account, and deploying needs IAM roles granted to our own service account
instead. See **Section 1 of [ADMIN_ACCESS_REQUEST.md](ADMIN_ACCESS_REQUEST.md)** —
once that's granted, run:

```bash
cd ctech-flowsentinel-demo
ADMIN_KEY="<pick a real one, not test123>"

gcloud run deploy flowsentinel-dashboard \
  --source=. \
  --region=us-central1 \
  --project=ctech-flowsentinel-ai \
  --service-account=flowsentinel-ai@ctech-flowsentinel-ai.iam.gserviceaccount.com \
  --allow-unauthenticated \
  --set-env-vars="GCP_PROJECT_ID=ctech-flowsentinel-ai,GCP_REGION=us-central1,GITHUB_OWNER=LatentView-Analytics-Ltd,GITHUB_REPO=ctech-flowsentinel-demo,GCS_LANDING_BUCKET=ctech-flowsentinel-ai-landing-zone,DASHBOARD_ADMIN_KEY=${ADMIN_KEY}"
```

This builds from the repo's [Dockerfile](Dockerfile) (no `gcloud`/Cloud SDK baked into
the image — the dashboard talks to Composer/BigQuery/GCS/Secret Manager/Vertex AI
directly via their REST APIs, which is what makes this work in a plain container).

### After it deploys

1. **Note the service URL** `gcloud run deploy` prints, or find it with:
   ```bash
   gcloud run services describe flowsentinel-dashboard --region=us-central1 --format="value(status.url)"
   ```
2. **Public + unauthenticated** was the earlier call for this demo — anyone with the
   URL sees the read-only Summary/Ticket Board views, but the mutating actions
   (approve, start-fix, start-analysis, run-backfill) require `DASHBOARD_ADMIN_KEY`
   as an `X-Admin-Key` header, same as locally. Share that key only with whoever
   should be able to act, not the URL's audience at large.
3. **Store the real GitHub PAT** (Section 2 of the admin doc) — Cloud Run's
   `flowsentinel-ai` service account already has `secretmanager.secretAccessor`, so
   once the secret has a real value, the deployed dashboard picks it up on its next
   request with no redeploy needed.
4. **Redeploying after code changes**: re-run the same `gcloud run deploy` command —
   it rebuilds from the current source and replaces the running revision with zero
   config to repeat.

### What's still open after that

- The GitHub PAT (Section 2 of the admin doc) — without it, Production Mode's
  GitHub sync and the Schema Drift Repair Agent's PR merge stay in fallback mode
  even on Cloud Run.
- Two Cloud Composer environments (`ctech-flowsentinel-demo-dev`, `test-sentinel`)
  are running continuously and billing by the hour regardless of dashboard traffic —
  worth knowing about, not something this deploy changes either way.
