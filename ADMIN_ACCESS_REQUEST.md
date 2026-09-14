# FlowSentinel AI — Pending Access Request

**Requested by:** Kasthurirangan Sampathkumar (`kasthurirangan.sampathkumar@latentview.com`)
**Project:** FlowSentinel AI — DE On-Call Automation Framework
**GCP Project:** `ctech-flowsentinel-ai`
**GitHub Repo:** `LatentView-Analytics-Ltd/ctech-flowsentinel-demo`

---

## Why this document exists

The framework is fully built and running — GCP resources (BigQuery, Pub/Sub, Cloud Composer, Vertex AI Gemini, Cloud Run), the dashboard, and the AI agent framework are all live. Two access gaps remain, both GitHub-side, plus one new item to finish wiring up CI/CD. Everything else that was originally requested here has been resolved.

---

## ✅ Resolved — no action needed

### 1. GCP: Artifact Registry write access for Cloud Run deploys
`roles/artifactregistry.writer` was granted to `flowsentinel-ai@ctech-flowsentinel-ai.iam.gserviceaccount.com`. Verified: the dashboard is deployed and live at the shared Cloud Run URL, redeploys work end to end.

### 2. GitHub: Collaborator access on the org repo
My account (`kasthurirangansampathkumar`) was added as a collaborator on `LatentView-Analytics-Ltd/ctech-flowsentinel-demo`. Verified: `git push org main` succeeds from my local machine (last confirmed with commit `406a307`).

---

## ⏳ Still open

## 3. GitHub: Approve a fine-grained PAT for the org repo

### Who can do this
Whoever administers the **LatentView-Analytics-Ltd** GitHub organization's Personal Access Token policy (org **Settings → Personal access tokens → Pending requests**), or whoever can grant me permission to generate one scoped to this repo.

### Current status: still not working
Re-tested against the live Cloud Run app just now:

```bash
curl -s "https://flowsentinel-remediation-agent-673338764809.us-central1.run.app/api/incidents" \
  -H "X-Demo-Mode: live"
```

Returns:
```json
{"error":"404 Client Error: Not Found for url: https://api.github.com/repos/LatentView-Analytics-Ltd/ctech-flowsentinel-demo/issues?state=all&labels=incident","issues":[]}
```

A `404` from the GitHub API on a repo I otherwise have collaborator access to (Section 2, above) means there's still no valid token in `github-pat-token` in Secret Manager — GitHub returns 404 rather than 401/403 for an invalid/missing token, to avoid confirming private-repo existence to unauthenticated callers. **Production Mode's GitHub-backed features (real Incidents, real PRs Awaiting Approval, GitHub Projects sync) are blocked on this token.** Demo Mode is unaffected — it never calls the real GitHub API.

### Steps
1. I generate a **fine-grained PAT**: GitHub → my profile → **Settings → Developer settings → Personal access tokens → Fine-grained tokens → Generate new token**.
   - **Resource owner**: `LatentView-Analytics-Ltd`
   - **Repository access**: only `ctech-flowsentinel-demo`
   - **Permissions**: `Issues: Read & write`, `Pull requests: Read & write`, `Contents: Read & write`, `Projects: Read & write`, `Workflows: Read & write`
2. If the org restricts fine-grained PATs (likely, for a security-conscious org), it shows as **pending approval**. An org admin approves it under **org Settings → Personal access tokens → Pending requests**.
3. Once approved, I store it in Secret Manager myself (the token is never shared in chat, email, or committed to the repo):
   ```bash
   echo -n "<the approved PAT>" | gcloud secrets versions add github-pat-token \
     --data-file=- --project=ctech-flowsentinel-ai
   ```

### What this unlocks
- Real GitHub Issues created automatically when the Issue Segment Agent files an incident
- Real PR creation/merge for the Schema Drift Repair Agent's approved fixes
- Production Mode's "Sync from GitHub" pulling real tickets from the GitHub Projects board

---

## 4. GitHub: Add two Actions secrets to enable CI/CD for Composer DAGs

### Why this exists
The Airflow DAGs that run the demo's data pipelines live in version control at [`composer_dags/`](composer_dags/) in this repo (see [`TEAMMATE_REPLICATION_GUIDE.md`](TEAMMATE_REPLICATION_GUIDE.md) for how developers browse/debug them). Previously, deploying a DAG change to the live Composer environment meant someone manually running `gsutil rsync` from their laptop. I've now built a GitHub Actions workflow ([`.github/workflows/deploy-dags.yml`](.github/workflows/deploy-dags.yml)) that automatically validates and syncs any change under `composer_dags/` to the Composer environment's GCS bucket on every push to `main` — no manual step, no long-lived key on anyone's laptop.

It authenticates to GCP using **Workload Identity Federation (WIF)** — GitHub Actions gets a short-lived GCP credential via OIDC, scoped to only this exact repo, with no service account key ever stored anywhere. I've already created the WIF pool, provider, and IAM binding on the GCP side (no admin action needed there). The only remaining step is adding two **repository secrets** on GitHub, which needs repo Admin access (I only have Write via Section 2).

### Who can do this
Whoever has **Admin** access to `LatentView-Analytics-Ltd/ctech-flowsentinel-demo` (repo **Settings → Secrets and variables → Actions → New repository secret**), or grants me Admin so I can add them myself.

### Steps
Go to `https://github.com/LatentView-Analytics-Ltd/ctech-flowsentinel-demo/settings/secrets/actions` and add exactly these two secrets:

| Secret name | Value |
|---|---|
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/673338764809/locations/global/workloadIdentityPools/github-actions-pool/providers/github-actions-provider` |
| `GCP_SERVICE_ACCOUNT` | `flowsentinel-ai@ctech-flowsentinel-ai.iam.gserviceaccount.com` |

Neither value is a secret credential in the traditional sense — they're identifiers, not keys — but they're stored as Actions secrets per Google's own recommended pattern for WIF, since anyone able to trigger a workflow in the repo can otherwise see plain env vars in logs.

### How to verify it worked
Push any trivial change under `composer_dags/` (or trigger it manually via the **Actions** tab → "Deploy Composer DAGs" → **Run workflow**), and confirm the run goes green. A green run means the DAG file(s) synced to `gs://us-central1-ctech-flowsenti-34a08e5b-bucket/dags/` with no service account key ever touching GitHub's servers.

### What this unlocks
- Any DAG change merged to `main` reaches the live Composer environment automatically, with a syntax-validation gate that stops a broken file before it can break the environment's DAG parser
- No one needs local `gsutil`/GCP credentials on their laptop just to ship a pipeline change
- A visible, auditable deploy history for pipeline code (the Actions run log), separate from git history

---

## Checklist for the admin

- [x] ~~Grant `roles/artifactregistry.writer` to the service account~~ — done
- [x] ~~Add `kasthurirangansampathkumar` as a Write collaborator on the org repo~~ — done
- [ ] Approve my fine-grained PAT request for `ctech-flowsentinel-demo` once I've generated it (Section 3) — **blocks real GitHub Issues/PRs/Projects sync in Production Mode**
- [ ] Add the two `GCP_WORKLOAD_IDENTITY_PROVIDER` / `GCP_SERVICE_ACCOUNT` secrets, or grant me Admin to add them myself (Section 4) — **blocks the new DAG CI/CD pipeline from ever running**

Everything else (BigQuery, Pub/Sub, Cloud Composer, Vertex AI Gemini, GCS, Cloud Run) is already working under my existing access.
