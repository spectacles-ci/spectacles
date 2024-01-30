from enum import Enum
from typing import Annotated, Any, Dict, Literal, Optional, Tuple, TypeVar, Union

from pydantic import BaseModel, Field, RootModel

JsonDict = Dict[str, Any]
T = TypeVar("T")


class SkipReason(str, Enum):
    NO_DIMENSIONS = "no_dimensions"
    UNMODIFIED = "unmodified"


class ErrorSqlLocation(BaseModel):
    """Stores the line and column within a SQL query where an error occurred."""

    line: Optional[int] = None
    column: Optional[int] = None
    character: Optional[int] = None


class QueryError(BaseModel):
    """Stores the details for a SQL query error returned from the Looker API."""

    message: str
    message_details: Optional[str] = None
    sql_error_loc: Optional[ErrorSqlLocation] = None

    @property
    def full_message(self) -> str:
        return " ".join(filter(None, (self.message, self.message_details)))


class PendingQueryResult(BaseModel):
    status: Literal["added", "running"]


class InterruptedQueryResult(BaseModel):
    status: Literal["expired", "killed"]


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
    class ErrorData(BaseModel):
        id: str
        error: str
        runtime: float = 0.0
        sql: str = ""

    class MultiErrorData(BaseModel):
        id: str
        runtime: float
        sql: Optional[str]
        errors: Optional[Tuple[QueryError, ...]]

    status: Literal["error"]
    data: Union[ErrorData, MultiErrorData]

    @property
    def runtime(self) -> float:
        return self.data.runtime

    @property
    def sql(self) -> Optional[str]:
        return self.data.sql

    @property
    def errors(self) -> Tuple[QueryError, ...]:
        if isinstance(self.data, self.ErrorData):
            return (
                QueryError(
                    message=self.data.error, message_details=None, sql_error_loc=None
                ),
            )
        elif isinstance(self.data, self.MultiErrorData):
            if self.data.errors is None:
                raise TypeError("No errors contained in this query result")
            return self.data.errors
        else:
            raise TypeError("Unexpected type for ErrorQueryResult.data")

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


QueryResult = RootModel[
    Annotated[
        Union[
            PendingQueryResult,
            InterruptedQueryResult,
            CompletedQueryResult,
            ErrorQueryResult,
        ],
        Field(..., discriminator="status"),
    ]
]
