from typing import Optional

from google.cloud import firestore
from graph import DependencyGraph


class WorkflowState:
    """Manages workflow state in Firestore"""

    def __init__(self):
        self.db = firestore.Client()
        self.collection = self.db.collection("workflows")

    def create_workflow(self, workflow_id: str, graph: DependencyGraph):
        """
        Save workflow graph to Firestore.

        Args:
            workflow_id: Unique workflow identifier
            graph: DependencyGraph to save
        """
        doc_ref = self.collection.document(workflow_id)
        doc_ref.set({**graph.to_dict(), "created_at": firestore.SERVER_TIMESTAMP})

    def get_workflow(self, workflow_id: str) -> Optional[DependencyGraph]:
        """
        Retrieve workflow graph from Firestore.

        Args:
            workflow_id: Workflow to retrieve

        Returns:
            DependencyGraph if found, None otherwise.
        """
        workflow_doc = self.collection.document(workflow_id).get()
        if workflow_doc.exists:
            return DependencyGraph.from_dict(workflow_doc)

        return None

    def update_task_status(self, workflow_id: str, task_id: str, status: str):
        """
        Update a single task's status.

        Args:
            workflow_id: Workflow containing the task
            task_id: Task to update
            status: New status (COMPLETED, FAILED, etc.)
        """
        self.collection.document(workflow_id).update(
            {f"tasks.{task_id}.status": status}
        )

    def delete_workflow(self, workflow_id: str):
        """
        Delete a workflow (cleanup after completion).

        Args:
            workflow_id: Workflow to delete
        """
        self.collection.document(workflow_id).delete()
