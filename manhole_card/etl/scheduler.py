from etl.scheduler import TaskScheduler
from models.administration import Prefecture
from manhole_card.etl.datatype import TaskType
from manhole_card.etl.ingestor import ManholeCardIngestor
from manhole_card.etl.migrator import ManholeCardMigrator


class ManholeCardTaskScheduler(TaskScheduler):
    DOMAIN = "manhole_card"

    TASK_OWNER_RUNNERS = {
        TaskType.INGESTOR_MANHOLE_CARD: ManholeCardIngestor,
    }

    TASK_GLOBAL_RUNNERS = {
        TaskType.MIGRATOR_MANHOLE_CARD: ManholeCardMigrator
    }

    TASK_RUNNERS = {**TASK_OWNER_RUNNERS, **TASK_GLOBAL_RUNNERS}

    @classmethod
    def get_task_runners(cls):
        return cls.TASK_RUNNERS

    @classmethod
    def health_check(cls):
        prefectures = Prefecture.get_all()
        for prefecture in prefectures:
            for task_type in cls.TASK_OWNER_RUNNERS:
                cls.enable_task(task_type, prefecture.en_name)

        for task_type in cls.TASK_GLOBAL_RUNNERS:
            cls.enable_task(task_type, "jp")
