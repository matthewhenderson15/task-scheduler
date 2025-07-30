from __future__ import annotations

import datetime
import heapq
from collections import defaultdict
from enum import Enum
from typing import List


class Task:
    def __init__(
        self,
        task_id: str,
        task_name: str,
        node_id: int,
        priority: int,
    ):
        self.task_id = task_id
        self.task_name = task_name
        self.node_id = node_id
        self.priority = priority
        self.scheduled_time = datetime.now()
        self.dependencies = set()
        self.dependents = set()
        self.status = Status.PENDING


class DependencyGraph:
    def __init__(self):
        self.tasks = {}
        self.in_degree = {}
        self.adjacency_list = defaultdict()
        self.reverse_adjacency_list = defaultdict()
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

    def _add_dependency(
        self, dependent_task_id: Task, dependency_task_id: Task
    ) -> bool:
        """
        Adds a dependent task or dependency task.

        Args:
            dependent_task_id (Task): The task that is dependent on another task (e.g., child task).
            dependency_task_id (Task): The task that is the
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

    def detect_cycles(self):
        pass

    def _get_ready_tasks(self) -> List[str]:
        ready_tasks = []

        for task_id, task in self.tasks.items():
            if task.status == Status.PENDING and self.in_degree[task_id] == 0:
                task.status = Status.READY
                ready_tasks.append(task)

                heap_entry = (
                    task.scheduled_time,
                    task.task_id,
                    task.priority,
                    task.tenant_id,
                )
                heapq.heappush(heap=self.ready_queue, item=heap_entry)
                ready_tasks.append(task_id)

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
