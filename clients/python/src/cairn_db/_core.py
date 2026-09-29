"""Request building, tokens, results and errors shared by the sync and async clients."""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional, Sequence, TypedDict, Union

Id = Union[str, int]
"""A document id: your own string, or an unsigned integer below 2^63."""

Document = dict[str, Any]
"""A document: an ``id`` plus fields named as in the schema."""

Filter = dict[str, Any]
"""A filter: ``and``, ``or``, ``not``, or a condition on one field (see the helpers)."""

Consistency = Literal["linearizable", "read_your_writes", "stale"]


class VectorLeg(TypedDict, total=False):
    field: str
    values: list[float]
    ef: int


# ---------------------------------------------------------------- filters


def eq(field: str, value: Any) -> Filter:
    """``field`` equals ``value`` (on a Set field: contains it)."""
    return {"field": field, "eq": value}


def in_(field: str, values: Sequence[Any]) -> Filter:
    """``field`` equals one of ``values`` (on a Set field: contains one of them)."""
    return {"field": field, "in": list(values)}


def range_(field: str, *, gt: Any = None, gte: Any = None, lt: Any = None, lte: Any = None) -> Filter:
    """A numeric or date range."""
    f: Filter = {"field": field}
    for k, v in (("gt", gt), ("gte", gte), ("lt", lt), ("lte", lte)):
        if v is not None:
            f[k] = v
    return f


def is_null(field: str) -> Filter:
    """``field`` has no value."""
    return {"field": field, "is_null": True}


def and_(*filters: Filter) -> Filter:
    return {"and": list(filters)}


def or_(*filters: Filter) -> Filter:
    return {"or": list(filters)}


def not_(f: Filter) -> Filter:
    return {"not": f}


# ---------------------------------------------------------------- errors


class CairnError(Exception):
    """Any error from the API or the network. ``status`` is 0 for a network error."""

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


class AuthenticationError(CairnError):
    """401: missing or invalid API key."""


class ForbiddenError(CairnError):
    """403: the key lacks the role, or cannot act for that tenant."""


class NotFoundError(CairnError):
    """404."""


class InvalidInputError(CairnError):
    """400: invalid input (unknown field, wrong type, bad filter...)."""


class UnavailableError(CairnError):
    """503, or a network failure, after the retries."""


def error_for(status: int, message: str) -> CairnError:
    cls = {
        400: InvalidInputError,
        401: AuthenticationError,
        403: ForbiddenError,
        404: NotFoundError,
        0: UnavailableError,
        502: UnavailableError,
        503: UnavailableError,
        504: UnavailableError,
    }.get(status, CairnError)
    return cls(message, status)


# ---------------------------------------------------------------- results


@dataclass
class WriteResult:
    """Documents written (or ids given to a takedown by id), and the consistency token."""

    count: int
    token: str


@dataclass
class DeleteResult:
    """``deleted``: documents removed (by filter, parent or tenant); ``count``: ids given."""

    token: str
    deleted: Optional[int] = None
    count: Optional[int] = None


@dataclass
class LegHit:
    rank: int
    score: float


@dataclass
class Hit:
    id: Id
    score: float
    legs: list[Optional[LegHit]] = field(default_factory=list)
    document: Optional[Document] = None
    tenant: Optional[str] = None
    """The hit's tenant, shown to unscoped keys only."""

    @staticmethod
    def from_json(h: Mapping[str, Any]) -> "Hit":
        return Hit(
            id=h["id"],
            score=h["score"],
            legs=[LegHit(**leg) if leg else None for leg in h.get("legs", [])],
            document=h.get("document"),
            tenant=h.get("_tenant"),
        )


# ---------------------------------------------------------------- tokens and requests


def merge_tokens(*tokens: Optional[str]) -> str:
    """Per-shard maximum of consistency tokens (``shard.index,...``)."""
    best: dict[int, int] = {}
    for t in tokens:
        for part in (t or "").split(","):
            if not part:
                continue
            m = re.fullmatch(r"(\d+)\.(\d+)", part)
            if not m:
                raise InvalidInputError(f"bad consistency token {part!r}", 400)
            s, i = int(m.group(1)), int(m.group(2))
            best[s] = max(best.get(s, 0), i)
    return ",".join(f"{s}.{i}" for s, i in sorted(best.items()))


class TokenBox:
    """Token state shared by a client and its tenant views."""

    def __init__(self, value: str = "") -> None:
        self.value = value

    def observe(self, token: str) -> None:
        self.value = merge_tokens(self.value, token)


def doc_path(id: Id, after: str, consistency: Optional[str]) -> str:
    """The path of a document: digits are an integer id, so a text id of digits says so."""
    params: list[tuple[str, str]] = []
    if isinstance(id, bool) or not isinstance(id, (str, int)):
        raise InvalidInputError(f"an id is a string or an integer, not {id!r}", 400)
    if isinstance(id, int):
        if id < 0:
            raise InvalidInputError(f"an integer id is non-negative: {id}", 400)
        path = f"/v1/documents/{id}"
    else:
        path = "/v1/documents/" + urllib.parse.quote(id, safe="")
        if id.isdigit():
            params.append(("id_type", "text"))
    if after:
        params.append(("after", after))
    if consistency:
        params.append(("consistency", consistency))
    return path + ("?" + urllib.parse.urlencode(params) if params else "")


def search_body(
    *,
    k: Optional[int],
    vector: Union[VectorLeg, Sequence[VectorLeg], None],
    text: Optional[str],
    text_field: Optional[str],
    all_terms: bool,
    filter: Optional[Filter],
    fusion: Optional[Mapping[str, Any]],
    oversample: Optional[int],
    with_documents: Optional[bool],
    consistency: Optional[str],
    after: str,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if k is not None:
        body["k"] = k
    if isinstance(vector, Mapping):
        body["vector"] = dict(vector)
    elif vector:
        body["vectors"] = [dict(v) for v in vector]
    if text is not None:
        if not text_field:
            raise InvalidInputError("a text query needs text_field", 400)
        body["text"] = {"field": text_field, "query": text, "all_terms": all_terms}
    if filter is not None:
        body["filter"] = filter
    if fusion is not None:
        body["fusion"] = dict(fusion)
    if oversample is not None:
        body["oversample"] = oversample
    if with_documents is not None:
        body["with_documents"] = with_documents
    if consistency:
        body["consistency"] = consistency
    if after:
        body["after"] = after
    return body


def delete_body(
    *, ids: Optional[Sequence[Id]], filter: Optional[Filter], parent: Optional[Id], parent_field: str, after: str
) -> dict[str, Any]:
    if parent is not None:
        if ids is not None or filter is not None:
            raise InvalidInputError("parent cannot be combined with ids or filter", 400)
        body: dict[str, Any] = {"filter": eq(parent_field, parent)}
    elif ids is not None:
        if not ids:
            raise InvalidInputError("no ids", 400)
        body = {"ids": list(ids)}
        if filter is not None:
            body["filter"] = filter
    elif filter is not None:
        body = {"filter": filter}
    else:
        raise InvalidInputError("give ids, filter or parent", 400)
    if after:
        body["after"] = after
    return body


def delete_result(r: Mapping[str, Any]) -> DeleteResult:
    return DeleteResult(token=r["consistency_token"], deleted=r.get("deleted"), count=r.get("count"))
