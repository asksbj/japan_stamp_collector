from etl.models import Task

class TaskRunner:
    TASK_TIMEOUT_SECS = 600
    TASK_RETRY_PERIOD = 30

    def __init__(self, task: Task) -> None:
        self._task = task

    def run(self) -> None:
        raise NotImplementedError("Task runner subclasses must implement this method")