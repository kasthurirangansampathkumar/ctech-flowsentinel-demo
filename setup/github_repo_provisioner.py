#!/usr/bin/env python3
"""
SentinelView AI - GitHub Repository & Secret Provisioner
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
import certifi

def create_github_repo():
    if not GITHUB_PAT:
        print("❌ GITHUB_PAT_TOKEN environment variable is missing!")
        print("💡 Please export GITHUB_PAT_TOKEN='ghp_xxxx' and try again.")
        sys.exit(1)

    print(f"🚀 [GitHub Provisioner] Creating repository '{GITHUB_REPO}'...")
    url = "https://api.github.com/user/repos"
    payload = {
        "name": GITHUB_REPO,
        "description": "SentinelView AI - Autonomous Data Engineering On-Call & Resilience Framework",
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

    # Verified TLS context (avoids MITM exposure of the GitHub token in transit)
    ssl_context = ssl.create_default_context(cafile=certifi.where())

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
    print("📌 Pushing local SentinelView AI code to GitHub repository...")
    base_dir = "/Users/kasthurirangansampathkumar/Documents/DE Oncall Demo"

    # Clean remote URL with NO token embedded (never persisted to .git/config).
    clean_remote = f"https://github.com/{owner}/{GITHUB_REPO}.git"

    setup_commands = [
        ["git", "init"],
        ["git", "config", "user.name", "CTech Data Engineers"],
        ["git", "config", "user.email", "de-oncall@ctech.com"],
        ["git", "add", "."],
        ["git", "commit", "-m", "Initial SentinelView AI Framework setup"],
        ["git", "branch", "-M", "main"],
        ["git", "remote", "remove", "origin"],
        ["git", "remote", "add", "origin", clean_remote],
    ]
    for cmd in setup_commands:
        try:
            subprocess.run(cmd, cwd=base_dir, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

    # Auth is passed as a one-off header on this single push invocation only —
    # it never touches disk (.git/config) or shell history, unlike a token-in-URL remote.
    import base64
    basic_auth = base64.b64encode(f"x-access-token:{GITHUB_PAT}".encode()).decode()
    push_cmd = [
        "git", "-c", f"http.extraHeader=AUTHORIZATION: basic {basic_auth}",
        "push", "-u", "origin", "main",
    ]
    result = subprocess.run(push_cmd, cwd=base_dir, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"⚠️ Push notice: {result.stderr.decode(errors='ignore').strip()[-300:]}")

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

    # Pass the secret via stdin (input=) rather than shell string interpolation,
    # so it never appears in a shell command line or process list.
    cmd_add = [
        gcloud_bin, "secrets", "versions", "add", "github-pat-token",
        "--data-file=-", f"--project={GCP_PROJECT_ID}"
    ]
    subprocess.run(cmd_add, input=GITHUB_PAT.encode(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("✅ Secret 'github-pat-token' updated in GCP Secret Manager!")

if __name__ == "__main__":
    owner, clone_url = create_github_repo()
    push_code_to_github(owner, clone_url)
    store_token_in_gcp_secret_manager()
