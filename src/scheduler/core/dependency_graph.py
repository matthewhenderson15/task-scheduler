from __future__ import annotations

import heapq
from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Union


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
                return True  # Back edge found - cycle detected

            if task_id in visited:
                return False  # Already processed this subtree

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

    def _get_next_task(self):
        pass

    def _get_complete_task(self):
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
