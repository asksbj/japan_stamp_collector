from typing import List

from core.database import BaseDBManager

class BaseModel:
    _table_name: str = None
    _columns: List[str] = None
    _db_manager: BaseDBManager = None


    @classmethod
    def get_db_manager(cls) -> BaseDBManager:
        if cls._db_manager is None:
            raise ValueError(f"Class {cls.__name__} must define _db_manager attribute")
        return cls._db_manager

    @classmethod
    def get_table_name(cls) -> str:
        if cls._table_name is None:
            cls._table_name = cls.__name__.lower()
        return cls._table_name

    @classmethod
    def get_columns(cls) -> List[str]:
        if cls._columns is None:
            raise ValueError(f"Class {cls.__name__} must define _columns attribute")
        return cls._columns

    def _get_insert_query(self) -> str:
        columns = ", ".join(self.get_columns())
        placeholders = ", ".join(["%s"] * len(self.get_columns()))
        return f"INSERT INTO {self.get_table_name()} ({columns}) VALUES ({placeholders})"

    def _get_update_query(self) -> str:
        columns = self.get_columns()
        set_clause = ", ".join([f"{col} = %s" for col in columns])
        return f"UPDATE {self.get_table_name()} SET {set_clause} WHERE id = %s"

    def _get_values_for_db(self, include_id: bool=False) -> tuple:
        values = []
        for column in self.get_columns():
            value = getattr(self, column, None)
            values.append(value)

        if include_id:
            values.append(self.id)

        return tuple(values)

    @classmethod
    def from_db(cls, row: tuple):
        if not row:
            return None

        columns = ["id"] + cls._columns
        data = dict(zip(columns, row))
        return cls(**data)

    def save(self) -> None:
        if self.id:
            self._update()
        else:
            self._insert()

    def _insert(self) -> None:
        query = self._get_insert_query()
        params = self._get_values_for_db()

        self.id = self.get_db_manager().execute_query(query, params)
    
    def _update(self) -> None:
        query = self._get_update_query()
        params = self._get_values_for_db(include_id=True)

        self.get_db_manager().execute_query(query, params)