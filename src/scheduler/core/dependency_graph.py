from __future__ import annotations

import heapq
from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import List, Optional, Tuple, Union


class Task:
    def __init__(
        self,
        task_id: str,
        task_name: str,
        node_id: int,
        priority: int,
        tenant_id: str = "default",
    ):
        self.task_id = task_id
        self.task_name = task_name
        self.node_id = node_id
        self.priority = priority
        self.tenant_id = tenant_id
        self.scheduled_time = datetime.now()
        self.dependencies = set()
        self.dependents = set()
        self.status = Status.PENDING


class DependencyGraph:
    def __init__(self):
        self.tasks = {}
        self.in_degree = {}
        self.adjacency_list = defaultdict(set)
        self.reverse_adjacency_list = defaultdict(set)
        self.ready_queue = []

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
        ready_tasks = []

        for task_id, task in self.tasks.items():
            if task.status == Status.PENDING and self.in_degree[task_id] == 0:
                task.status = Status.READY
                ready_tasks.append(task_id)

                heap_entry = (
                    task.scheduled_time,
                    task.priority,
                    task.task_id,
                    task.tenant_id,
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
            _, _, task_id, _ = heapq.heappop(self.ready_queue)
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

    def mark_complete_task(self, task_id: str) -> List[Task]:
        """
        Marks a task as completed and updates dependent tasks.

        Args:
            task_id (str): The task_id to be marked as complete.

        Returns:
            List[Task]: A list of newly ready tasks based on dependent task updates.

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
                new_ready_tasks.append(dependent_task)

                heap_entry = (
                    dependent_task.scheduled_time,
                    dependent_task.priority,
                    dependent_task.task_id,
                    dependent_task.tenant_id,
                )
                heapq.heappush(self.ready_queue, heap_entry)

        return new_ready_tasks

    def retry_failed_tasks(self):
        pass

    def _get_task_summary(self):
        pass


class Status(Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
