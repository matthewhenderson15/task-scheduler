import os

import requests
from executor import TaskExecutor
from flask import Flask, jsonify, request

app = Flask(__name__)
executor = TaskExecutor()

WORKER_URL = os.getenv("WORKER_URL", "https://worker-xxxxx.run.app")
SCHEDULER_URL = os.getenv("SCHEDULER_URL", "https://tbd")


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for Cloud Run"""
    return jsonify({"status": "healthy", "service": "scheduler"}), 200


@app.route("/execute", methods=["POST"])
def execute():
    """
    Execute a Cloud Run task.

    Request body:
    {
        "workflow_id": "daily-report-2024",
        "task_id": "",
        "task_name": "",
        "task_config": {}
    }
    """
    try:
        data = request.get_json()
        workflow_id = data["workflow_id"]
        task_id = data["task_id"]
        config = data["task_config"]

        # Logic for executing the API request
        # May change
        api_request = requests.request(
            method=config.get("method"),
            params=config.get("params", None),
            headers=config.get("headers"),
            timeout=60,
        )

        if request.status_code != 200:
            return jsonify(
                {"error": f"Failed request: {str(api_request.status_code)}"}
            ), 400

        notify_scheduler(
            workflow_id,
            task_id,
        )

        return jsonify(
            {"workflow_id": workflow_id, "result": api_request.status_code}
        ), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def notify_scheduler(workflow_id: str, task_id: str, status: str, result: str = None):
    """
    Notify the scheduler of the result.


    """
    try:
        response = requests.post(
            url=f"{SCHEDULER_URL}/task-status",
            data={
                "workflow_id": workflow_id,
                "task_id": task_id,
                "task_status": status,
            },
            timeout=30,
        )

        response.raise_for_status()

    except Exception:
        print("Failed to notify")
