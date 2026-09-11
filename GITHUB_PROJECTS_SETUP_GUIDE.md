# GitHub Projects v2 Setup Guide: FlowSentinel AI Incident Command Board

**Prepared by:** CTech Data Engineers  
**Target Audience:** GitHub Organization Admins & Data Engineering Leads  

---

## 📌 Overview

**GitHub Projects (v2)** serves as the **100% Free, Code-Native Incident Command & Backfill Board** for FlowSentinel AI. It visualizes the entire incident lifecycle in real time—moving items from **🔴 Incident Triaged** to **🟢 Resolved & Closed** without requiring paid external ticketing SaaS like Jira.

---

## 🛠️ Step-by-Step GitHub Projects v2 Configuration

```
┌────────────────────────┐  ┌────────────────────────┐  ┌────────────────────────┐  ┌────────────────────────┐
│  🔴 Incident Triaged  │ ──► 🟡 PR Drafted (Awaiting)│ ──► 🔵 Backfill Scheduled  │ ──► 🟢 Resolved & Closed   │
└────────────────────────┘  └────────────────────────┘  └────────────────────────┘  └────────────────────────┘
```

---

### Step 1: Create a New GitHub Project (v2)

1. Navigate to your GitHub Organization or Personal Account:
   `https://github.com/orgs/YOUR_ORG/projects` OR `https://github.com/orgs/LatentView-Analytics-Ltd/projects`
2. Click the green **`New project`** button.
3. Select **`Board`** layout template and click **`Create`**.
4. Rename the Project Title:
   **`FlowSentinel AI - Incident & Backfill Command Board`**

---

### Step 2: Configure Board Status Columns

Click on the **`Status`** field header dropdown and configure the following 4 columns:

| Column Name | Color Badge | Meaning / Triggering Event |
| :--- | :--- | :--- |
| **`🔴 Incident Triaged`** | Red | Automatically created when Gemini 2.0 intercepts a Cloud Logging failure log and posts an issue. |
| **`🟡 PR Drafted (Awaiting Approval)`** | Yellow | Code Repair Agent opens a GitHub PR with the SQL schema fix and links `#Issue-ID`. |
| **`🔵 Backfill Scheduled`** | Blue | Backfill Agent calculates partition blast radius & BigQuery slot cost ($1.20). |
| **`🟢 Resolved & Closed`** | Green | Lead engineer clicks `[Approve & Auto-Deploy]` on Dashboard, PR merges, and backfill finishes. |

---

### Step 3: Add Custom Metadata Fields

To give executive stakeholders and on-call engineers full visibility, add 4 custom fields to your GitHub Project:

1. Click **`+`** (Add field) on the right side of the project table/board header.
2. Add the following custom fields:

| Field Name | Field Type | Options / Format | Example Value |
| :--- | :--- | :--- | :--- |
| **`Incident Severity`** | `Single Select` | `P1 - Critical` (Red), `P2 - High` (Orange), `P3 - Moderate` (Yellow) | `P1 - Critical` |
| **`Pipeline Name`** | `Text` | Plain text | `sales_transformation_pipeline` |
| **`Fault Line`** | `Text` | Code location | `stg_orders.sql:L42` |
| **`Est. Backfill Cost ($)`** | `Number` | Decimal number | `1.20` |

---

### Step 4: Configure Automation Workflows

Set up native GitHub Projects automation so cards move across columns automatically:

1. Click the **`⚡ Workflows`** icon at the top right of your project board.
2. Enable the following built-in workflow rules:
   - **Auto-add to project**: Set filter to `label:incident` (Automatically adds any issue created by Gemini 2.0 Triage Agent to `🔴 Incident Triaged`).
   - **Item closed**: Set column to `🟢 Resolved & Closed` (Automatically moves cards to Done when the Pull Request merges and closes `#Issue-ID`).
   - **PR merged**: Set column to `🟢 Resolved & Closed`.

---

### Step 5: Link Repository & Obtain Project ID

1. Click **`...`** (Project settings) ➔ **`Linked repositories`**.
2. Select your repository: **`ctech-flowsentinel-demo`**.
3. Copy the **Project Number / Node ID** from the URL bar:
   - URL: `https://github.com/orgs/YOUR_ORG/projects/1` ➔ Project Number is `1`.
4. Export the Project Number in your environment setup:
   ```bash
   export GITHUB_PROJECT_NUMBER="1"
   ```

---

## 👥 6. User Management & Access Control Guide

Users need to be added in **two distinct locations** depending on whether they require GitHub repository/board access or GCP cloud infrastructure access:

### A. Adding Users in GitHub (Repository & Projects Access)

#### 1. Repository Access (For Engineers & Reviewers)
- **Location**: GitHub Repository Settings ➔ `Collaborators` (or `Teams and people` for Orgs).
- **Steps**:
  1. Go to `https://github.com/LatentView-Analytics-Ltd/ctech-flowsentinel-demo/settings/access`
  2. Click **`Add people`** and search by GitHub username or corporate email.
  3. Select Role:
     - `Write` (For On-Call Data Engineers to push branches, review PRs, and manage issues).
     - `Admin` (For Project Leads).
     - `Read` (For Auditors / Viewers).

#### 2. Project Board Access (For Managing the Command Board)
- **Location**: GitHub Projects Board ➔ `...` (Project Settings) ➔ `Manage access`.
- **Steps**:
  1. Go to your Project Board: `https://github.com/orgs/YOUR_ORG/projects/1`
  2. Click `...` ➔ **`Manage access`** ➔ **`Add collaborators`**.
  3. Assign Role: `Editor` / `Contributor` (Can edit custom fields, move cards across status columns, and approve items).

---

### B. Adding Users in GCP (Cloud Infrastructure & Logs Access)

- **Location**: GCP Console ➔ `IAM & Admin` ➔ `IAM`  
  `https://console.cloud.google.com/iam-admin/iam?project=ctech-flowsentinel-demo-dev`
- **Steps**:
  1. Click **`GRANT ACCESS`** (at top of page).
  2. In **`New principals`**, enter corporate user emails.
  3. Assign IAM Roles:
     - **For Data Engineers**: `roles/composer.user`, `roles/bigquery.dataEditor`, `roles/storage.objectAdmin`, `roles/logging.viewer`.
     - **For Platform Admins**: `roles/composer.admin`, `roles/bigquery.admin`.

---

## 📌 Verification & Testing

To test your GitHub Projects setup:
1. Run Demo 1: `./run_all_demos.sh 1`
2. Open your GitHub Projects board URL. You will see a new card created in **`🔴 Incident Triaged`** titled:  
   `🚨 [P1 Incident] Schema Drift Failure in sales_transformation_pipeline`
3. Run Demo 2: `./run_all_demos.sh 2`
4. Watch the card automatically transition to **`🟡 PR Drafted (Awaiting Approval)`** with linked PR #142!
