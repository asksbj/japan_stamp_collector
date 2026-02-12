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

        INITED = 'inited'
        BASIC = 'basic'
        DETAILED = 'detailed'
        LOCATRED = 'located'


    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.owner = kwargs.get('owner')
        self.state = kwargs.get('state') or self.StateEnum.INITED
        self.date = kwargs.get('date')
        self.created_time = kwargs.get('created_time') or datetime.datetime.now()
        self.last_update = kwargs.get('last_update')

    @classmethod
    def update_status(cls, record_id: str, original_state: str, new_state: str):
        if not record_id:
            return None

        query = "UPDATE fuke_ingestor_record SET state = %s WHERE id = %s and state = %s"
        params = (new_state, record_id, original_state)
        cls.get_db_manager().execute_query(query, params)