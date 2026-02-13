import logging
from re import L
import sys
import datetime
import threading
import time
from typing import final

from etl.thread import TaskThread
from etl.models import Task

logging.basicConfig(level=logging.INFO)


class TaskScheduler:
    HEALTH_CHECK_PERIOD_SEC = 30

    @classmethod
    def health_check(cls):
        raise NotImplementedError

    @classmethod
    def get_task_runners(cls):
        raise NotImplementedError

    @staticmethod
    def enable_task(task_type: str, owner: str) -> None:
        last_update = datetime.datetime.now() - datetime.timedelta(minutes=30)
        task = Task.get_task_by_type_and_owner(task_type, owner)
        
        if not task:
            task = Task(task_type=task_type, owner=owner)
            task.save()
            logging.info(f"Created task {task_type} for owner {owner}")
        else:
            if task.last_update.year >= datetime.MAXYEAR:
                task.last_update = last_update
                task.save()
                logging.info(f"Enabled task {task_type} for owner {owner}")

    @staticmethod
    def disable_task(task_type: str, owner: str) -> None:
        max_time = datetime.datetime.max
        task = Task.get_task_by_type_and_owner(task_type, owner)
        if task:
            task.last_update = max_time
            task.save()

            logging.info(f"Disabled task {task_type} for owner {owner}")

    @classmethod
    def start(cls, threads: int) -> None:
        logging.info(f"{cls.__name__} start running")
        thread_list = []
        exit_flag = threading.Event()

        try:
            for _ in range(threads):
                thread = TaskThread(exit_flag, cls)
                thread.start()
                thread_list.append(thread)

            if not thread_list:
                sys.exit("Invalid argument: no threads enabled")

            while thread_list:
                for thread in thread_list:
                    if not thread.is_alive():
                        thread_list.remove(thread)
                        # sys.exit("Detect dead thread")
                    thread.keep_alive()
                cls.health_check()
                logging.info(f"Waiting for health check for {cls.HEALTH_CHECK_PERIOD_SEC} seconds")
                time.sleep(cls.HEALTH_CHECK_PERIOD_SEC)
        except:
            logging.exception("Thread Died!")
        finally:
            exit_flag.set()
            for thread in thread_list:
                thread.join()
                thread.cleanup()
            logging.critical("All threads exited")
