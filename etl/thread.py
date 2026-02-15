import logging
import threading
import time
import datetime

from typing import Optional
from etl.models import Task

logging.basicConfig(level=logging.INFO)


class TaskThread(threading.Thread):
    TASK_SELECT_RETRYS = 5
    TASK_RETRY_TIME = 5

    def __init__(self, exit_flag, schedular):
        super().__init__()
        self._exit_flag = exit_flag
        self._task_runners = schedular.get_task_runners()
        self._task = None
        self._retries = 0

    def run(self):
        tid = threading.get_ident()
        logging.info(f"Thread {tid} start running")
        while not self._exit_flag.is_set() and self._retries <= self.TASK_SELECT_RETRYS:
            if self._select_task():
                logging.info(f"{tid} is working on task {self._task.id} for {self._task.owner}")
                self._run_task()
                self._retries = 0
            else:
                logging.debug(f"{tid} is idle, retry later")
                time.sleep(self.TASK_RETRY_TIME)
                self._retries += 1
        logging.info(f"No task selected, thread {tid} exits")

    def keep_alive(self):
        task = self._task
        if task:
            task_timeout_secs = self._task_runners[task.task_type].TASK_TIMEOUT_SECS
            if (task.last_update - datetime.datetime.now()).total_seconds() < (task_timeout_secs/2):
                next_update = datetime.datetime.now() + datetime.timedelta(seconds=task_timeout_secs)
                Task.update_last_update(task.id, next_update)
                task.last_update = next_update

    def cleanup(self, update_secs: Optional[int] = -1):
        if self._task:
            timestamp = datetime.datetime.now() + datetime.timedelta(seconds=update_secs)
            Task.update_last_update(self._task.id, timestamp)
            self._task = None

    def _select_task(self):
        task = Task.get_last_updated()
        if task:
            timestamp = datetime.datetime.now()
            if task.last_update < timestamp:
                task_timeout_secs = self._task_runners[task.task_type].TASK_TIMEOUT_SECS
                task.last_update = timestamp + datetime.timedelta(seconds=task_timeout_secs)
                task.save()
                self._task = task
                return True
        return False

    def _run_task(self):
        task_runner = self._task_runners[self._task.task_type]
        result = task_runner(self._task).run()

        if result < 0:
            self.cleanup(task_runner.TASK_RETRY_PERIOD)
        elif result == 0:
            self.cleanup(task_runner.TASK_TIMEOUT_SECS)
        else:
            self.cleanup()