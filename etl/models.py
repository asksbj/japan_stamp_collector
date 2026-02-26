import datetime
from typing import Optional

from core.database import etl_db_manager
from models.base import BaseModel


class Task(BaseModel):
    _table_name = "task"
    _columns = ["domain", "task_type", "owner", "last_update", "date"]
    _db_manager = etl_db_manager
    
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.domain = kwargs.get("domain")
        self.task_type = kwargs.get("task_type")
        self.owner = kwargs.get("owner")
        self.last_update = kwargs.get("last_update") or datetime.datetime.now()
        self.date = kwargs.get("date")

    @classmethod
    def get_task_by_type_and_owner(cls, task_type: str, owner: str) -> "Task":
        if not task_type or not owner:
            return None

        query = f"SELECT * FROM {cls.get_table_name()} WHERE task_type = %s and owner = %s"
        params = (task_type, owner)
        return cls.get_db_results(query, params, fetch_one=True)

    @classmethod
    def get_last_updated(cls, domain: Optional[str] = None) -> 'Task':
        if domain:
            query = f"SELECT * FROM {cls.get_table_name()} WHERE domain = %s ORDER BY last_update LIMIT 1"
            params = (domain, )
        else:
            query = f"SELECT * FROM {cls.get_table_name()} ORDER BY last_update LIMIT 1"
            params = ()

        return cls.get_db_results(query, params, fetch_one=True)

    @classmethod
    def update_last_update(
        cls, 
        task_id: int, 
        last_update: Optional[datetime.datetime] = datetime.datetime.now(),
        origin_updated_time: Optional[datetime.datetime] = None
    ) -> None:
        if not task_id:
            return None

        if not origin_updated_time:
            query = f"UPDATE {cls.get_table_name()} SET last_update = %s WHERE id = %s"
            params = (last_update, task_id)
        else:
            query = f"UPDATE {cls.get_table_name()} SET last_update = %s WHERE id = %s AND last_update = %s"
            params = (last_update, task_id, origin_updated_time)
        _, result = cls.get_db_manager().execute_query(query, params)
        return result

    


    
