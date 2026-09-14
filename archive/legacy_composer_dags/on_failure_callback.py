import requests
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook

def create_github_ticket_on_failure(context):
    # 1. Grab details about the failed task from Airflow's memory
    dag_name = context.get("task_instance").dag_id
    task_name = context.get("task_instance").task_id
    error_message = context.get("exception")
    
    # 2. Safely retrieve the GitHub password from GCP Secret Manager
    secrets_service = GoogleCloudSecretManagerHook()
    github_token = secrets_service.access_secret(
        secret_id="github-pat-token", 
        project_id="ctech-flowsentinel-demo-dev"
    )
    
    # 3. Write the issue content
    ticket_title = f"Alert: Task '{task_name}' failed in DAG '{dag_name}'"
    ticket_body = f"""### Pipeline Failure Detected
- **DAG:** `{dag_name}`
- **Task:** `{task_name}`
- **Error Details:**
```{error_message}```
"""

    # 4. Send the request to GitHub to create the issue
    api_url = "https://api.github.com/repos/ctech-flowsentinel-demo/issues"
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json"
    }
    payload = {
        "title": ticket_title,
        "body": ticket_body,
        "labels": ["incident"]
    }
    
    requests.post(api_url, headers=headers, json=payload)

# 5. Tell the DAG to run this function every time any task fails
default_args = {
    "owner": "data_engineering",
    "on_failure_callback": create_github_ticket_on_failure
}