from etl.scheduler import TaskScheduler
from jpost.etl.datatype import TaskType
from jpost.etl.ingestor import FukeBasicIngestor, FukeDetailIngestor, PostOfficeLocationIngestor

class JpostTaskScheduler(TaskScheduler):
    TASK_RUNNERS = {
        TaskType.INGESTOR_FUKE_BASIC: FukeBasicIngestor,
        TaskType.INGESTOR_FUKE_DETAIL: FukeDetailIngestor,
        TaskType.INGESTOR_POST_OFFICE_LOCATION: PostOfficeLocationIngestor
    }

    @classmethod
    def get_task_runners(cls):
        return cls.TASK_RUNNERS

    @classmethod
    def health_check(cls):
        pass