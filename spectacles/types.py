from enum import Enum
from typing import Dict, Any, TypeVar, Optional, Tuple, Union
from pydantic import BaseModel

JsonDict = Dict[str, Any]
T = TypeVar("T")


class QueryStatusEnum(str, Enum):
    added = "added"
    running = "running"
    expired = "expired"
    complete = "complete"
    error = "error"


class ErrorSqlLocation(BaseModel):
    """Stores the line and column within a SQL query where an error occurred."""

    line: Optional[int]
    column: Optional[int]


class QueryError(BaseModel):
    """Stores the details for a SQL query error returned from the Looker API."""

    message: str
    message_details: Optional[str]
    sql_error_loc: Optional[ErrorSqlLocation]

    @property
    def full_message(self) -> str:
        return " ".join(filter(None, (self.message, self.message_details)))


class QueryResultData(BaseModel):
    id: str
    runtime: float
    sql: str
    errors: Optional[Tuple[QueryError, ...]]


class ExpiredQueryResultData(BaseModel):
    error: str


class QueryResult(BaseModel, use_enum_values=True):
    """Stores ID, query status, and error details for a completed query task."""

    status: QueryStatusEnum
    data: Union[QueryResultData, ExpiredQueryResultData, None]

    @property
    def task_id(self) -> str:
        if not isinstance(self.data, QueryResultData):
            raise TypeError("This query result doesn't contain any data")
        return self.data.id

    @property
    def runtime(self) -> float:
        if not isinstance(self.data, QueryResultData):
            raise TypeError("This query result doesn't contain any data")
        return self.data.runtime

    @property
    def sql(self) -> str:
        if not isinstance(self.data, QueryResultData):
            raise TypeError("This query result doesn't contain any data")
        return self.data.sql

    @property
    def errors(self) -> Tuple[QueryError, ...]:
        if not isinstance(self.data, QueryResultData):
            raise TypeError("This query result doesn't contain any data")
        elif self.data.errors is None:
            raise TypeError("No errors contained in this query result")
        return self.data.errors

    def get_valid_errors(self) -> Tuple[QueryError, ...]:
        WARNINGS = (
            (
                "Note: This query contains derived tables with conditional SQL for Development Mode. "
                "Query results in Production Mode might be different."
            ),
            (
                "Note: This query contains derived tables with Development Mode filters. "
                "Query results in Production Mode might be different."
            ),
        )
        return tuple(error for error in self.errors if error.message not in WARNINGS)
