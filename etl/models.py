import datetime
from typing import Optional

from core.database import etl_db_manager
from models.base import BaseModel


class Task(BaseModel):
    _table_name = "task"
    _columns = ["task_type", "owner", "last_update", "date"]
    _db_manager = etl_db_manager
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.task_type = kwargs.get('task_type')
        self.owner = kwargs.get('owner')
        self.last_update = kwargs.get('last_update') or datetime.datetime.now()
        self.date = kwargs.get('date')

    @classmethod
    def get_by_id(cls, task_id: int) -> 'Task':
        if not task_id:
            return None

        query = "SELECT * FROM task WHERE id = %s"
        params = (task_id, )

        row = cls.get_db_manager().execute_query(query, params, fetch_one=True)
        task = cls.from_db(row)
        return task

    @classmethod
    def get_task_by_type_and_owner(cls, task_type: str, owner: str) -> 'Task':
        if not task_type or not owner:
            return None

        query = "SELECT * FROM task WHERE task_type = %s and owner = %s"
        params = (task_type, owner)
        row = cls.get_db_manager().execute_query(query, params, fetch_one=True)
        task = cls.from_db(row)
        return task

    @classmethod
    def get_last_updated(cls, task_type: Optional[str] = None) -> 'Task':
        if task_type:
            query = "SELECT * FROM task WHERE task_type = %s ORDER BY last_update LIMIT 1"
            params = (task_type, )
        else:
            query = "SELECT * FROM task ORDER BY last_update LIMIT 1"
            params = ()

        row = cls.get_db_manager().execute_query(query, params, fetch_one=True)
        task = cls.from_db(row)
        return task

    @classmethod
    def update_last_update(cls, task_id: int, last_update: Optional[datetime.datetime] = datetime.datetime.now()) -> None:
        if not task_id:
            return None

        query = "UPDATE task SET last_update = %s WHERE id = %s"
        params = (last_update, task_id)
        cls.get_db_manager().execute_query(query, params)

    


    
