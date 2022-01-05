from datetime import datetime
from enum import Enum
from typing import List

from pydantic import BaseModel


class Dataset(BaseModel):
    resource_group_name: str
    storage_account_name: str
    container_name: str
    folder_path: str
    file_pattern_or_name: str
    # delimited text properties
    first_row_as_header: bool
    column_delimiter: str
    quote_char: str
    escape_char: str


class PipelineRequest(BaseModel):
    source: Dataset
    join: Dataset
    columns: List[str]
    sink: Dataset


class Error(BaseModel):
    code: int
    message: str
    source: str
    date: datetime


class PipelineResponse(BaseModel):
    source: Dataset
    join: Dataset
    columns: List[str]
    sink: Dataset
    pipeline_name: str
    error: Error


class Status(str, Enum):
    IN_PROGRESS = "InProgress"
    PENDING = "Pending"
    QUEUED = "Queued"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"


class ColumnDelimiter(str, Enum):
    COMMA = ","
    SEMICOLON = ";"
    PIPE = "|"
    TAB = "\t"


class EscapeCharacter(str, Enum):
    DOUBLE_QUOTE = '"'
    BACKSLASH = "\\"
    SLASH = "/"


class QuoteCharacter(str, Enum):
    DOUBLE_QUOTE = '"'
    SINGLE_QUOTE = "'"


class StatusDetails(BaseModel):
    status: Status
    start: datetime
    end: datetime
    duration: int


class RunResponse(BaseModel):
    run_id: str
    pipeline_name: str
    status: StatusDetails
    error: Error
