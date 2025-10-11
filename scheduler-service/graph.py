from __future__ import annotations

import heapq
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union


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
        """Task serialization for Firestore tasks"""
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
        task = cls(
            task_id=data["task_id"],
            task_name=data["task_name"],
            priority=data["priority"],
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
        self.ready_queue = []

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

    def _add_task(self, task: Task) -> None:
        """
        Adds a task to the task dictionary for creation of graph and creates in_degree entry.

        Args:
            task (Task): The task to be added identified by the task ID.

        Returns:
            None

        Raises:
            ValueError: If the task is already defined in the task map.
        """
        if task.task_id in self.tasks:
            raise ValueError(
                f"{task.task_id} is already defined in the task list. Please create a unique task ID!"
            )

        self.tasks[task.task_id] = task
        self.in_degree[task.task_id] = 0

    def _add_dependency(self, dependent_task_id: str, dependency_task_id: str) -> bool:
        """
        Adds a dependent task or dependency task.

        Args:
            dependent_task_id (str): The task ID that is dependent on another task (e.g., child task).
            dependency_task_id (str): The task ID that the other task depends on (e.g., parent task).

        Returns:
            bool: Whether the dependent and dependency tasks were properly added.

        Raises:
            ValueError: If the dependent or dependency tasks are not in the task list.
        """
        if dependent_task_id not in self.tasks or dependency_task_id not in self.tasks:
            raise ValueError("Dependent and dependency tasks must be in task list!")

        if dependency_task_id not in self.reverse_adjacency_list[dependent_task_id]:
            self.adjacency_list[dependency_task_id].add(dependent_task_id)
            self.reverse_adjacency_list[dependent_task_id].add(dependency_task_id)
            self.in_degree[dependent_task_id] += 1

            self.tasks[dependent_task_id].dependencies.add(dependency_task_id)
            self.tasks[dependency_task_id].dependents.add(dependent_task_id)

        return True

    def detect_cycles(self) -> Tuple[bool, Union[str, None]]:
        """
        Detects cycles in the dependency graph using DFS with recursion stack.

        Returns:
            Tuple[bool, Union[str, None]]: (has_cycle, error_message)
                - True, error_message if cycle detected
                - False, None if no cycles found
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

    def _get_ready_tasks(self) -> List[str]:
        """
        Identifies tasks that are ready to run (no pending dependencies).

        Returns:
            List[str]: List of task IDs that became ready and were added to queue
        """
        ready_tasks = []

        for task_id, task in self.tasks.items():
            if task.status == Status.PENDING and self.in_degree[task_id] == 0:
                task.status = Status.READY
                ready_tasks.append(task_id)

                heap_entry = (
                    task.scheduled_time,
                    task.priority,
                    task.task_id,
                )
                heapq.heappush(self.ready_queue, heap_entry)

        return ready_tasks

    def _get_next_task(self) -> Optional[Task]:
        """
        Gets the next highest priority ready task from the queue.

        Returns:
            Optional[Task]: The next task to execute, or None if no ready tasks available.

        Note:
            - Does NOT mark task as RUNNING (caller's responsibility)
            - Validates task still exists and is in READY status
            - Uses heap ordering: earliest scheduled_time, then lowest priority number
            - Loops through queue to skip stale tasks, max attempts = queue length
        """
        if not self.ready_queue:
            return None

        max_attempts = len(self.ready_queue)
        attempts = 0

        while self.ready_queue and attempts < max_attempts:
            _, _, task_id = heapq.heappop(self.ready_queue)
            attempts += 1

            if task_id not in self.tasks:
                continue

            task = self.tasks[task_id]

            if task.status != Status.READY:
                continue

            if self.in_degree[task_id] > 0:
                continue

            return task

        return None

    def mark_task_complete(self, task_id: str) -> List[str]:
        """
        Marks a task as completed and updates dependent tasks.

        Args:
            task_id (str): The task_id to be marked as complete.

        Returns:
            List[str]: List of newly ready task IDs that became available.

        Raises:
            ValueError: If the task is not in the task list or is not in a RUNNING state.
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

    def retry_failed_tasks(self) -> List[str]:
        """
        Gathers tasks that failed and adds them back to the ready queue.

        Returns:
            List[str]: List of task IDs that were retried and added back to queue.
        """
        retry_tasks = []

        for task_id, task in self.tasks.items():
            if task.status == Status.FAILED:
                if self.in_degree[task_id] == 0:
                    task.status = Status.READY
                    retry_tasks.append(task_id)

                    heap_entry = (
                        task.scheduled_time,
                        task.priority,
                        task.task_id,
                    )
                    heapq.heappush(self.ready_queue, heap_entry)
                else:
                    task.status = Status.PENDING

        return retry_tasks

    def _get_task_summary(self) -> TaskSummary:
        """
        Generates a comprehensive summary of all tasks in the dependency graph.

        Returns:
            TaskSummary: Complete overview of task statuses, dependencies, and metrics
        """
        total_tasks = len(self.tasks)
        status_counts = {}
        tasks_with_dependencies = 0
        tasks_without_dependencies = 0
        blocked_tasks = 0
        pending_tasks = []
        ready_tasks = []
        running_tasks = []
        completed_tasks = []
        failed_tasks = []
        cancelled_tasks = []
        max_dependencies = 0
        most_blocked_task = None
        total_priority = 0
        tenants_set = set()

        for task_id, task in self.tasks.items():
            status_str = task.status.value
            status_counts[status_str] = status_counts.get(status_str, 0) + 1

            task_in_degree = self.in_degree[task_id]
            if task_in_degree > 0:
                tasks_with_dependencies += 1
                if task.status == Status.PENDING:
                    blocked_tasks += 1
            else:
                tasks_without_dependencies += 1

            if task_in_degree > max_dependencies:
                max_dependencies = task_in_degree
                most_blocked_task = task_id

            if task.status == Status.PENDING:
                pending_tasks.append(task_id)
            elif task.status == Status.READY:
                ready_tasks.append(task_id)
            elif task.status == Status.RUNNING:
                running_tasks.append(task_id)
            elif task.status == Status.COMPLETED:
                completed_tasks.append(task_id)
            elif task.status == Status.FAILED:
                failed_tasks.append(task_id)
            elif task.status == Status.CANCELLED:
                cancelled_tasks.append(task_id)

            total_priority += task.priority
            tenants_set.add(task.tenant_id)

        completion_percentage = (
            (len(completed_tasks) / total_tasks * 100.0) if total_tasks > 0 else 0.0
        )
        ready_queue_size = len(self.ready_queue)
        avg_priority = total_priority / total_tasks if total_tasks > 0 else 0.0

        return TaskSummary(
            total_tasks=total_tasks,
            status_counts=status_counts,
            completion_percentage=completion_percentage,
            ready_queue_size=ready_queue_size,
            tasks_with_dependencies=tasks_with_dependencies,
            tasks_without_dependencies=tasks_without_dependencies,
            blocked_tasks=blocked_tasks,
            pending_tasks=pending_tasks,
            ready_tasks=ready_tasks,
            running_tasks=running_tasks,
            completed_tasks=completed_tasks,
            failed_tasks=failed_tasks,
            cancelled_tasks=cancelled_tasks,
            most_blocked_task=most_blocked_task,
            max_dependencies=max_dependencies,
            tenants=sorted(list(tenants_set)),
            avg_priority=avg_priority,
        )


@dataclass
class TaskSummary:
    """
    Comprehensive summary of all tasks in the dependency graph.
    """

    total_tasks: int
    status_counts: Dict[str, int]
    completion_percentage: float
    ready_queue_size: int
    tasks_with_dependencies: int
    tasks_without_dependencies: int
    blocked_tasks: int
    pending_tasks: List[str]
    ready_tasks: List[str]
    running_tasks: List[str]
    completed_tasks: List[str]
    failed_tasks: List[str]
    cancelled_tasks: List[str]
    most_blocked_task: Optional[str]
    max_dependencies: int
    tenants: List[str]
    avg_priority: float


class Status(Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
