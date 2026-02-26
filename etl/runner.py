import logging
import datetime

from etl.models import Task

class TaskRunner:
    TASK_TIMEOUT_SECS = 600
    TASK_RETRY_PERIOD = 30
    INTERVAL_DAYS = 0

    SUCCESS = 1
    NO_WORK_TO_DO = 0
    NOT_READY_FOR_WORK = -1
    FAILURE = -999

    def __init__(self, task: Task) -> None:
        self._task = task
        self._date = None

    def run(self) -> int:
        self._date = self._get_run_date()
        logging_arg = f"{self.__class__.__name__}({self._task.task_type}, {self._task.owner}, {self._date})"

        if not self._date:
            logging.info(f'NO_WORK_TO_DO {logging_arg}')
            return self.event(self.NO_WORK_TO_DO)

        try:
            self.pre_run()
        except:
            logging.error(f'FAILURE {logging_arg}')
            return self.event(self.FAILURE)

        status = self.start()
        if status == self.SUCCESS:
            self.complete()
            logging.info(f"SUCCESS {logging_arg}")
        elif status == self.NO_WORK_TO_DO:
            logging.info(f'NO_WORK_TO_DO {logging_arg}')
        elif status == self.NOT_READY_FOR_WORK:
            logging.info(f"NOT_READY_FOR WORK {logging_arg}")
        else:
            logging.info(f'FAILURE {logging_arg}')
        return self.event(status)

    def pre_run(self):
        pass

    def start(self):
        raise NotImplementedError

    def complete(self):
        self._set_completed()

    def event(self, status: str) -> str:
        logging.info(f"Running base event {status}")
        return status

    def _get_run_date(self):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        task_last_run_time = self._task.date
        if not task_last_run_time:
            return current_date

        next_run_date = datetime.datetime.strptime(task_last_run_time, '%Y-%m-%d') + datetime.timedelta(days=self.INTERVAL_DAYS)
        next_run_date = next_run_date.strftime("%Y-%m-%d")
        if next_run_date < current_date:
            return current_date
        
        return None

    def _set_completed(self):
        self._task.date = self._date
        self._task.save()