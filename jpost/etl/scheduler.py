from etl.scheduler import TaskScheduler
from jpost.etl.datatype import TaskType
from jpost.etl.ingestors.fuke import FukeBasicIngestor, FukeDetailIngestor
from jpost.etl.ingestors.post_office import PostOfficeLocationIngestor
from jpost.models.jpost import Prefecture

class JPostTaskScheduler(TaskScheduler):
    TASK_OWNER_RUNNERS = {
        TaskType.INGESTOR_FUKE_BASIC: FukeBasicIngestor,
        TaskType.INGESTOR_FUKE_DETAIL: FukeDetailIngestor,
        TaskType.INGESTOR_POST_OFFICE_LOCATION: PostOfficeLocationIngestor
    }

    TASK_GLOBAL_RUNNERS = {}

    TASK_RUNNERS = {**TASK_OWNER_RUNNERS, **TASK_GLOBAL_RUNNERS}

    @classmethod
    def get_task_runners(cls):
        return cls.TASK_RUNNERS

    @classmethod
    def health_check(cls):
        prefectures = Prefecture.get_all()
        for prefecture in prefectures:
            for task_type in cls.TASK_OWNER_RUNNERS:
                cls.enable_task(task_type, prefecture)
