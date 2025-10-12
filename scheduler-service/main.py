import json
import os

from flask import Flask, jsonify, request
from google.cloud import tasks_v2
from graph import DependencyGraph, Status, Task
from state import WorkflowState

app = Flask(__name__)
state = WorkflowState()

PROJECT_ID = os.getenv("GCP_PROJECT", "your-project-id")
LOCATION = os.getenv("GCP_REGION", "us-central1")
QUEUE_NAME = os.getenv("TASK_QUEUE", "default")
WORKER_URL = os.getenv("WORKER_URL", "https://worker-xxxxx.run.app")


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for Cloud Run"""
    return jsonify({"status": "healthy", "service": "scheduler"}), 200


@app.route("/schedule", methods=["POST"])
def schedule_workflow():
    """
    Create and schedule a new workflow.

    Request body:
    {
        "workflow_id": "daily-report-2024",
        "tasks": [
            {
                "id": "task-1",
                "name": "Fetch Data",
                "priority": 1,
                "task_config": {
                    "url": "https://api.example.com/data",
                    "method": "GET"
                }
            }
        ],
        "dependencies": [
            {"parent": "fetch-data", "child": "process-data"}
        ]
    }
    """
    try:
        data = request.get_json()
        workflow_id = data["workflow_id"]

        graph = DependencyGraph()

        for task_data in data["tasks"]:
            task = Task(
                task_id=task_data["id"],
                task_name=task_data["name"],
                priority=task_data["priority"],
                task_config=task_data["task_config"],
            )
            graph.add_task(task)

        for dependency in data.get("dependencies", []):
            graph.add_dependency(dependency["child"], dependency["parent"])

        has_cycle, error_message = graph.detect_cycles()
        if has_cycle:
            return jsonify({"error": error_message}), 400

        state.create_workflow(workflow_id, graph)

        ready_tasks = graph.get_ready_tasks()
        for task in ready_tasks:
            submit_task_to_queue(workflow_id, task)

        return jsonify(
            {
                "workflow_id": workflow_id,
                "total_tasks": len(graph.tasks),
                "ready_tasks": ready_tasks,
            }
        ), 200

    except KeyError as e:
        return jsonify({"error": f"Missing field: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/task-status", methods=["POST"])
def task_complete():
    """
    Called by worker when a task completes.

    Request body:
    {
        "workflow_id": "daily-report-2024",
        "task_id": "task-1",
        "status": "COMPLETED",
        "result": {...}
    }
    """
    # TODO:
    # 1. Load workflow from Firestore
    # 2. Mark task as complete in graph
    # 3. Get newly ready tasks
    # 4. Submit them to Cloud Tasks
    # 5. Save updated graph

    return jsonify({"status": "acknowledged"}), 200


@app.route("/workflow/<workflow_id>", methods=["GET"])
def get_workflow_status(workflow_id: str):
    """
    Get current status of a workflow.

    Returns summary of all tasks and their statuses.
    """
    # TODO:
    # 1. Load workflow from Firestore
    # 2. Generate task summary
    # 3. Return status

    return jsonify({"workflow_id": workflow_id}), 200


def submit_task_to_queue(workflow_id: str, task: Task) -> None:
    """
    Submit a task to Cloud Tasks queue for execution.

    Args:
        workflow_id: Workflow this task belongs to
        task: Task to execute
    """
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)

    task.status = Status.RUNNING

    payload = {
        "workflow_id": workflow_id,
        "task_id": task.task_id,
        "task_name": task.task_name,
        "task_config": task.task_config,
    }

    cloud_task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{WORKER_URL}/execute",
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
        },
    }

    client.create_task(request={"parent": parent, "task": cloud_task})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
