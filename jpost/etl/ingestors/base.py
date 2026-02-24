import logging
import datetime

from etl.runner import TaskRunner
from etl.models import Task

logging.basicConfig(level=logging.INFO)

class BaseIngestor(TaskRunner):
    SUCCESS = 1
    NO_WORK_TO_DO = 0
    NOT_READY_FOR_WORK = -1
    FAILURE = -999

    def __init__(self, task: Task) -> None:
        super().__init__(task)
        self._date = None

    def run(self):
        self._date = self._get_ingest_date()
        logging_arg = f"{self.__class__.__name__}({self._task.task_type}, {self._task.owner}, {self._date})"

        if not self._date:
            logging.info(f'NO_WORK_TO_DO {logging_arg}')
            return self.event(self.NO_WORK_TO_DO)

        try:
            self.pre_run()
        except:
            logging.error(f'FAILURE {logging_arg}')
            return self.event(self.FAILURE)

        status = self.fetch()
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

    def fetch(self):
        raise NotImplementedError

    def post_run(self):
        self._set_ingested()

    def event(self, status: str) -> str:
        logging.info("Running base event ")
        return status

    def _get_ingest_date(self):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        task_start_date = self._task.date

        if not task_start_date or task_start_date < current_date:
            return current_date
        else:
            return None

    def _set_ingested(self):
        self._task.date = self._date
        self._task.save()