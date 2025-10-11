from __future__ import annotations

import heapq
from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import List, Optional, Tuple


class Status(Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Task:
    def __init__(
        self,
        task_id: str,
        task_name: str,
        priority: int,
        api_config: dict = None,
    ):
        self.task_id = task_id
        self.task_name = task_name
        self.priority = priority
        self.scheduled_time = datetime.now()
        self.dependencies = set()
        self.dependents = set()
        self.status = Status.PENDING
        self.api_config = api_config or {}

    def to_dict(self) -> dict:
        """Serialize for Firestore"""
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "priority": self.priority,
            "scheduled_time": self.scheduled_time.isoformat(),
            "status": self.status.value,
            "dependencies": list(self.dependencies),
            "dependents": list(self.dependents),
            "api_config": self.api_config,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Task:
        """Deserialize from Firestore"""
        task = cls(
            task_id=data["task_id"],
            task_name=data["task_name"],
            priority=data.get("priority", 0),
            api_config=data.get("api_config", {}),
        )

        task.scheduled_time = datetime.fromisoformat(data["scheduled_time"])
        task.status = Status(data["status"])
        task.dependencies = set(data.get("dependencies", []))
        task.dependents = set(data.get("dependents", []))

        return task


class DependencyGraph:
    def __init__(self):
        self.tasks = {}
        self.in_degree = {}
        self.adjacency_list = defaultdict(set)
        self.reverse_adjacency_list = defaultdict(set)

    def add_task(self, task: Task):
        """
        Adds a task to the task dictionary for creation of graph and creates in_degree entry.

        Args:
            task (Task): The task to be added identified by the task ID.
        """
        if task.task_id in self.tasks:
            raise ValueError(f"Task {task.task_id} already exists")

        self.tasks[task.task_id] = task
        self.in_degree[task.task_id] = 0

    def _add_dependency(self, dependent_task_id: str, dependency_task_id: str):
        """
        Adds a dependent task or dependency task.

        Args:
            dependent_task_id (str): The task ID that is dependent on another task (e.g., child task).
            dependency_task_id (str): The task ID that the other task depends on (e.g., parent task).
        """
        if dependent_task_id not in self.tasks or dependency_task_id not in self.tasks:
            raise ValueError("Dependent and dependency tasks must be in task list!")

        if dependency_task_id not in self.reverse_adjacency_list[dependent_task_id]:
            self.adjacency_list[dependency_task_id].add(dependent_task_id)
            self.reverse_adjacency_list[dependent_task_id].add(dependency_task_id)
            self.in_degree[dependent_task_id] += 1
            self.tasks[dependent_task_id].dependencies.add(dependency_task_id)
            self.tasks[dependency_task_id].dependents.add(dependent_task_id)

    def detect_cycles(self) -> Tuple[bool, Optional[str]]:
        """
        Detects cycles in the dependency graph using DFS with recursion stack.

        Returns:
            A tuple of whether there is a cycle and if true, the error message.
        """
        visited = set()
        recursion_stack = set()

        def dfs(task_id: str) -> bool:
            """DFS helper function to detect cycles."""
            if task_id in recursion_stack:
                return True

            if task_id in visited:
                return False

            recursion_stack.add(task_id)

            for dependent_task_id in self.adjacency_list[task_id]:
                if dfs(dependent_task_id):
                    return True

            recursion_stack.remove(task_id)
            visited.add(task_id)
            return False

        for task_id in self.tasks:
            if task_id not in visited:
                if dfs(task_id):
                    return (
                        True,
                        f"Cycle detected in dependency graph involving task: {task_id}",
                    )

        return (False, None)

    def get_ready_tasks(self) -> List[str]:
        """
        Identifies tasks that are ready to run (no pending dependencies).

        Returns:
            A list of task IDs that became ready and were added to queue
        """
        ready = []
        for task_id, task in self.tasks.items():
            if task.status == Status.PENDING and self.in_degree[task_id] == 0:
                task.status = Status.READY
                ready.append((task.scheduled_time, task.priority, task_id))

        ready.sort()
        return [task_id for _, _, task_id in ready]

    def mark_task_complete(self, task_id: str) -> List[str]:
        """
        Marks a task as completed and updates dependent tasks.

        Args:
            task_id (str): The task_id to be marked as complete.

        Returns:
            List[str]: List of newly ready task IDs that became available.
        """
        if task_id not in self.tasks:
            raise ValueError(f"Task {task_id} not found in dependency graph.")

        task: Task = self.tasks[task_id]

        if task.status != Status.RUNNING:
            raise ValueError(
                f"Task {task_id} is not in RUNNING state, current status: {task.status}"
            )

        task.status = Status.COMPLETED

        new_ready_tasks = []

        for dependent_task_id in self.adjacency_list[task_id]:
            self.in_degree[dependent_task_id] -= 1
            if (
                self.in_degree[dependent_task_id] == 0
                and self.tasks[dependent_task_id].status == Status.PENDING
            ):
                dependent_task = self.tasks[dependent_task_id]
                dependent_task.status = Status.READY
                new_ready_tasks.append(dependent_task_id)

                heap_entry = (
                    dependent_task.scheduled_time,
                    dependent_task.priority,
                    dependent_task.task_id,
                )
                heapq.heappush(self.ready_queue, heap_entry)

        return new_ready_tasks

    def mark_test_running(self, task_id: str):
        """Mark task as running"""
        if task_id in self.tasks:
            self.tasks[task_id].status = Status.RUNNING

    def mark_task_failed(self, task_id: str):
        """Mark task as failed"""
        if task_id in self.tasks:
            self.tasks[task_id].status = Status.FAILED

    def to_dict(self) -> dict:
        """Serialize graph for Firestore"""
        return {
            "tasks": {task_id: task.to_dict() for task_id, task in self.tasks.items()},
            "in_degree": self.in_degree,
            "adjacency_list": {k: list(v) for k, v in self.adjacency_list.items()},
            "reverse_adjacency_list": {
                k: list(v) for k, v in self.reverse_adjacency_list.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> DependencyGraph:
        graph = cls()

        for task_id, task_data in data["tasks"].items():
            task = Task.from_dict(task_data)
            graph.tasks[task_id] = task

        graph.in_degree = data["in_degree"]
        graph.adjacency_list = defaultdict(
            set, {k: set(v) for k, v in data["adjacency_list"].items()}
        )
        graph.reverse_adjacency_list = defaultdict(
            set, {k: set(v) for k, v in data["reverse_adjacency_list"].items()}
        )

        return graph
