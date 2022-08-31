from typing import Dict, Any, TypeVar, Optional, Tuple, Union, Literal
from pydantic import BaseModel, Field

JsonDict = Dict[str, Any]
T = TypeVar("T")


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


class PendingQueryResult(BaseModel):
    status: Literal["added", "running"]


class ExpiredQueryResult(BaseModel):
    class QueryResultData(BaseModel):
        error: str

    status: Literal["expired"]
    data: QueryResultData


class CompletedQueryResult(BaseModel):
    class QueryResultData(BaseModel):
        id: str
        runtime: float

    status: Literal["complete"]
    data: QueryResultData

    @property
    def runtime(self) -> float:
        return self.data.runtime


class ErrorQueryResult(BaseModel):
    class QueryResultData(BaseModel):
        id: str
        runtime: float
        sql: str
        errors: Optional[Tuple[QueryError, ...]]

    status: Literal["error"]
    data: QueryResultData

    @property
    def runtime(self) -> float:
        return self.data.runtime

    @property
    def sql(self) -> str:
        return self.data.sql

    @property
    def errors(self) -> Tuple[QueryError, ...]:
        if self.data.errors is None:
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


class QueryResult(BaseModel):
    """Container model to allow discriminated union on status."""

    __root__: Union[
        PendingQueryResult, ExpiredQueryResult, CompletedQueryResult, ErrorQueryResult
    ] = Field(..., discriminator="status")
