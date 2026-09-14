import os
import requests
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook

def notify_github_on_failure(context):
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    log_url = task_instance.log_url

    project_id = "ctech-flowsentinel-ai"
    secret_id = "github-pat-token"
    repo = "LatentView-Analytics-Ltd/ctech-flowsentinel-demo"

    try:
        sm_hook = GoogleCloudSecretManagerHook()
        token = sm_hook.access_secret(secret_id=secret_id, project_id=project_id)
    except Exception as e:
        print(f"[FlowSentinel Error] Secret Manager lookup failed: {e}")
        return

    issue_title = f"[Incident] Task Failed: {dag_id}.{task_id}"
    issue_body = (
        f"### Airflow Task Failure Detected (FlowSentinel AI)\n\n"
        f"- **DAG ID:** `{dag_id}`\n"
        f"- **Task ID:** `{task_id}`\n"
        f"- **Execution Date:** `{execution_date}`\n"
        f"- **Status:** Action Required\n"
        f"- **Log URL:** [View Airflow Log]({log_url})\n\n"
        f"#### Exception\n```text\n{str(exception)}\n```"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }

    payload = {
        "title": issue_title,
        "body": issue_body,
        "labels": ["incident", "auto-triage"]
    }

    api_url = f"https://api.github.com/repos/{repo}/issues"
    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 201:
            print(f"[FlowSentinel Success] Issue created: {response.json().get('html_url')}")
        else:
            print(f"[FlowSentinel Error] GitHub returned status {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[FlowSentinel Error] Connection failed: {e}")