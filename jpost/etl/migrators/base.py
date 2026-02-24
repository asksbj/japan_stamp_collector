import logging
import datetime

from etl.runner import TaskRunner
from etl.models import Task


class BaseMigrator(TaskRunner):
    TASK_TIMEOUT_SECS = 24*60*60
    MIGRATE_INTERVAL_DAYS = 1

    SUCCESS = 1
    NO_WORK_TO_DO = 0
    FAILURE = -1

    def __init__(self, task: Task) -> None:
        super().__init__(task)
        self._date = None

    def run(self):
        self._date = self._get_migrate_date()
        logging_arg = f"{self.__class__.__name__}({self._task.task_type}, {self._date})"

        if not self._date:
            logging.info(f'NO_WORK_TO_DO {logging_arg}')
            return self.event(self.NO_WORK_TO_DO)

        try:
            self.pre_run()
        except:
            logging.error(f'FAILURE {logging_arg}')
            return self.event(self.FAILURE)

        status = self.migrate()
        if status == self.SUCCESS:
            self.post_run()
            logging.info(f"SUCCESS {logging_arg}")
        elif status == self.NO_WORK_TO_DO:
            logging.info(f'NO_WORK_TO_DO {logging_arg}')
        else:
            logging.info(f'FAILURE {logging_arg}')
        return self.event(status)

    def pre_run(self):
        pass

    def migrate(self):
        raise NotImplementedError

    def post_run(self):
        self._set_migrated()

    def event(self, status: str) -> str:
        logging.info(f"Running base event {status}")
        return status

    def _get_migrate_date(self):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        task_last_run_time = self._task.date
        if not task_last_run_time:
            return current_date

        next_run_date = datetime.datetime.strptime(task_last_run_time, '%Y-%m-%d') + datetime.timedelta(days=self.MIGRATE_INTERVAL_DAYS)
        next_run_date = next_run_date.strftime("%Y-%m-%d")
        if next_run_date < current_date:
            return current_date
        
        return None

    def _set_migrated(self):
        self._task.date = self._date
        self._task.save()