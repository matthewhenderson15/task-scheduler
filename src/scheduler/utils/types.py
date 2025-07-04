from dataclasses import dataclass


@dataclass
class Job:
    retries: int
    max_retries: int
    job_id: int


@dataclass
class JobQueue:
    job: Job
    priority: int
