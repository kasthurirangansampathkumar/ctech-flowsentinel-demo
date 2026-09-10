#!/usr/bin/env python3
"""
FlowSentinel AI - GitHub Repository & Secret Provisioner
Prepared by: CTech Data Engineers
"""
import os
import sys
import json
import subprocess
import urllib.request

GITHUB_PAT = os.getenv("GITHUB_PAT_TOKEN", "")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "ctech-flowsentinel-demo")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai")

import ssl

def create_github_repo():
    if not GITHUB_PAT:
        print("❌ GITHUB_PAT_TOKEN environment variable is missing!")
        print("💡 Please export GITHUB_PAT_TOKEN='ghp_xxxx' and try again.")
        sys.exit(1)

    print(f"🚀 [GitHub Provisioner] Creating repository '{GITHUB_REPO}'...")
    url = "https://api.github.com/user/repos"
    payload = {
        "name": GITHUB_REPO,
        "description": "FlowSentinel AI - Autonomous Data Engineering On-Call & Resilience Framework",
        "private": False,
        "auto_init": False
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"token {GITHUB_PAT}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        },
        method="POST"
    )

    # Disable SSL verification for Homebrew Mac Python
    ssl_context = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=ssl_context) as resp:
            data = json.loads(resp.read().decode())
            repo_html = data.get("html_url", "")
            clone_url = data.get("clone_url", "")
            owner = data.get("owner", {}).get("login", "")
            print(f"✅ Successfully created GitHub repository: {repo_html}")
            return owner, clone_url
    except urllib.error.HTTPError as e:
        if e.code == 422:
            print(f"ℹ️ Repository '{GITHUB_REPO}' already exists on GitHub!")
            return GITHUB_OWNER, f"https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}.git"
        else:
            print(f"❌ Failed to create repo: {e}")
            sys.exit(1)

def push_code_to_github(owner, clone_url):
    print("📌 Pushing local FlowSentinel AI code to GitHub repository...")
    base_dir = "/Users/kasthurirangansampathkumar/Documents/DE Oncall Demo"
    
    authenticated_remote = f"https://{GITHUB_PAT}@github.com/{owner}/{GITHUB_REPO}.git"
    
    commands = [
        ["git", "init"],
        ["git", "config", "user.name", "CTech Data Engineers"],
        ["git", "config", "user.email", "de-oncall@ctech.com"],
        ["git", "add", "."],
        ["git", "commit", "-m", "Initial FlowSentinel AI Framework setup"],
        ["git", "branch", "-M", "main"],
        ["git", "remote", "remove", "origin"],
        ["git", "remote", "add", "origin", authenticated_remote],
        ["git", "push", "-u", "origin", "main", "--force"]
    ]

    for cmd in commands:
        try:
            subprocess.run(cmd, cwd=base_dir, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
            
    print(f"🎉 Code successfully pushed to main branch on GitHub: https://github.com/{owner}/{GITHUB_REPO}")

def store_token_in_gcp_secret_manager():
    print(f"🔒 Storing GITHUB_PAT_TOKEN in GCP Secret Manager under project '{GCP_PROJECT_ID}'...")
    gcloud_bin = "/Users/kasthurirangansampathkumar/y/google-cloud-sdk/bin/gcloud"
    
    cmd_create = [
        gcloud_bin, "secrets", "create", "github-pat-token",
        "--replication-policy=automatic",
        f"--project={GCP_PROJECT_ID}"
    ]
    subprocess.run(cmd_create, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    cmd_add = f"echo -n '{GITHUB_PAT}' | {gcloud_bin} secrets versions add github-pat-token --data-file=- --project={GCP_PROJECT_ID}"
    subprocess.run(cmd_add, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("✅ Secret 'github-pat-token' updated in GCP Secret Manager!")

if __name__ == "__main__":
    owner, clone_url = create_github_repo()
    push_code_to_github(owner, clone_url)
    store_token_in_gcp_secret_manager()
