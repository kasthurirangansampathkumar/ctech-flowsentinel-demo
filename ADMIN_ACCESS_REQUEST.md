# FlowSentinel AI — Pending Access Request

**Requested by:** Kasthurirangan Sampathkumar (`kasthurirangan.sampathkumar@latentview.com`)
**Project:** FlowSentinel AI — DE On-Call Automation Framework
**GCP Project:** `ctech-flowsentinel-ai`
**GitHub Repo:** `LatentView-Analytics-Ltd/ctech-flowsentinel-demo`

---

## Why this document exists

The framework is up and running locally with real GCP resources (BigQuery, Pub/Sub, Cloud Composer, Vertex AI Gemini) already provisioned and working under my own account. Two access gaps remain, and both need someone with more privilege than I currently have (`roles/editor` on the GCP project) to unblock. This document gives the exact commands — nothing here needs guesswork or a support ticket, just someone with the right role running the commands below.

**Update:** you already granted my account `roles/iam.serviceAccountUser` and `roles/logging.configWriter` — thank you, that got the Cloud Run deploy most of the way there. I ran it end to end to find exactly what's still missing, so Section 1 below is now a single, verified role instead of the original six-role guess.

---

## 1. GCP: One role left to unblock Cloud Run deploy

### Who can do this
**`Pam.master22@latentview.com`** — confirmed via `gcloud projects get-iam-policy` to be the only `roles/owner` on `ctech-flowsentinel-ai`. I only have `roles/editor` plus the two roles above, none of which include `resourcemanager.projects.setIamPolicy` — I still can't grant IAM roles myself.

### Why it's needed
With the `iam.serviceAccountUser` grant, I can now deploy using `flowsentinel-ai@ctech-flowsentinel-ai.iam.gserviceaccount.com` as the build identity (working around this project having no default Compute Engine service account). I ran the actual deploy — **the build itself succeeds**, but pushing the built image to Artifact Registry fails:

```
denied: Permission 'artifactregistry.repositories.uploadArtifacts' denied on resource
'//artifactregistry.googleapis.com/projects/ctech-flowsentinel-ai/locations/us-central1/repositories/cloud-run-source-deploy'
```

That's the one missing piece — `roles/artifactregistry.writer` on the same service account.

### Command to run

```bash
gcloud projects add-iam-policy-binding ctech-flowsentinel-ai \
  --member="serviceAccount:flowsentinel-ai@ctech-flowsentinel-ai.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer" \
  --condition=None
```

One role, one existing service account — nothing new created, no other access touched.

### (Recommended, optional) Let me self-serve future IAM changes

Every time the framework needs a new IAM grant, it currently has to come back to you. If you're comfortable with it, granting my account `roles/resourcemanager.projectIamAdmin` (narrower than full Owner) on `ctech-flowsentinel-ai` would let me handle IAM changes like the one above myself going forward:

```bash
gcloud projects add-iam-policy-binding ctech-flowsentinel-ai \
  --member="user:kasthurirangan.sampathkumar@latentview.com" \
  --role="roles/resourcemanager.projectIamAdmin" \
  --condition=None
```

### How to verify it worked

```bash
gcloud projects get-iam-policy ctech-flowsentinel-ai \
  --flatten="bindings[].members" \
  --filter="bindings.members:flowsentinel-ai@ctech-flowsentinel-ai.iam.gserviceaccount.com" \
  --format="table(bindings.role)"
```
Should include `roles/artifactregistry.writer` in the list.

---

## 2. GitHub: Approve a fine-grained PAT for the org repo

### Who can do this
Whoever administers the **LatentView-Analytics-Ltd** GitHub organization's Personal Access Token policy (org **Settings → Personal access tokens → Pending requests**), or whoever can grant me permission to generate one scoped to this repo.

### Why it's needed
The framework's agents (RCA ticket creation, PR generation/merge, GitHub Projects sync) all call the GitHub API on behalf of the `LatentView-Analytics-Ltd/ctech-flowsentinel-demo` repo. Right now there's no valid token in GCP Secret Manager (`github-pat-token` exists as a secret but has no working value), so every GitHub-touching feature falls back to a safe simulated/offline mode — the demo still works, but nothing actually reaches the real repo.

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

## Checklist for the admin

- [ ] Grant `roles/artifactregistry.writer` to `flowsentinel-ai@ctech-flowsentinel-ai.iam.gserviceaccount.com` (Section 1) — the one thing left blocking Cloud Run deploy
- [ ] Approve my fine-grained PAT request for `ctech-flowsentinel-demo` once I've generated it (Section 2)

Nothing else is currently blocking the framework — everything else (BigQuery, Pub/Sub, Cloud Composer, Vertex AI Gemini, GCS) is already working under my existing access.
