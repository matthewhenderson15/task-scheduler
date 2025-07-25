from enum import Enum


class JobStatus(Enum):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobRunStatus(Enum):
    TIMEOUT = "timeout"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class NodeStatus(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    MAINTENANCE = "maintenance"


class WorkerStatus(Enum):
    BUSY = "busy"
    IDLE = "idle"
    ERROR = "error"


class ScheduleType(Enum):
    INTERVAL = "interval"
    CRON = "cron"
    ONE_TIME = "one_time"
