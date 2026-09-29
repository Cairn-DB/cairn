"""``cairn-db``: the Cairn HTTP API from Python, sync and async (ADR 0031)."""

from ._core import (
    AuthenticationError,
    CairnError,
    Consistency,
    DeleteResult,
    Document,
    Filter,
    ForbiddenError,
    Hit,
    Id,
    InvalidInputError,
    LegHit,
    NotFoundError,
    UnavailableError,
    VectorLeg,
    WriteResult,
    and_,
    eq,
    in_,
    is_null,
    merge_tokens,
    not_,
    or_,
    range_,
)
from .client import AsyncClient, Client

__version__ = "0.3.0"

__all__ = [
    "AsyncClient", "AuthenticationError", "CairnError", "Client", "Consistency", "DeleteResult",
    "Document", "Filter", "ForbiddenError", "Hit", "Id", "InvalidInputError", "LegHit",
    "NotFoundError", "UnavailableError", "VectorLeg", "WriteResult", "and_", "eq", "in_",
    "is_null", "merge_tokens", "not_", "or_", "range_",
]
