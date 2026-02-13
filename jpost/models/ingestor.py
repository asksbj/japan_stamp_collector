import datetime
from enum import Enum

from core.database import db_manager
from models.base import BaseModel


class FukeIngestorRecords(BaseModel):
    _table_name = "fuke_ingestor_record"
    _columns = ["owner", "state", "date", "created_time", "last_updated"]
    _db_manager = db_manager


    class StateEnum(Enum):
        # Fuke ingest enums
        CREATED = "created"
        BASIC = "basic"
        DETAILED = "detailed"
        LOCATRED = "located"
        FINISHED = "finished"


    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.owner = kwargs.get("owner")
        self.state = kwargs.get("state") or self.StateEnum.CREATED
        self.date = kwargs.get("date")
        self.created_time = kwargs.get("created_time") or datetime.datetime.now()
        self.last_updated = kwargs.get("last_updated")

    @classmethod
    def get_by_owner(cls, owner: str) -> "FukeIngestorRecords":
        if not owner:
            return None

        query = f"SELECT * FROM {cls.get_table_name()} WHERE owner = %s"
        params = (owner, )

        return cls.get_db_results(query, params, fetch_one=True)

    @classmethod
    def get_by_owner_and_date(cls, owner: str, date: str) -> "FukeIngestorRecords":
        if not owner or not date:
            return None

        query = f"SELECT * FROM {cls.get_table_name()} WHERE owner = %s and date = %s"
        params = (owner, date)

        return cls.get_db_results(query, params, fetch_one=True)

    @classmethod
    def update_state(cls, record_id: str, origin_state: str, new_state: str):
        if not record_id:
            return None

        last_updated = datetime.datetime.now()
        query = "UPDATE fuke_ingestor_record SET state = %s, last_updated = %s WHERE id = %s and state = %s"
        params = (new_state, last_updated, record_id, origin_state)
        cls.get_db_manager().execute_query(query, params)