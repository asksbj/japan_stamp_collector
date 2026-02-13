from etl.scheduler import TaskScheduler
from jpost.etl.datatype import TaskType
from jpost.etl.ingestors.fuke import FukeBasicIngestor, FukeDetailIngestor
from jpost.etl.ingestors.post_office import PostOfficeLocationIngestor

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