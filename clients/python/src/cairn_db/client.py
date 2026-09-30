"""Sync and async clients for the Cairn HTTP API (ADR 0031)."""

from __future__ import annotations

import asyncio
import time
import urllib.parse
from typing import Any, Mapping, Optional, Sequence, Union

import httpx

from ._core import (
    Consistency,
    DeleteResult,
    Document,
    Filter,
    Hit,
    Id,
    NotFoundError,
    TokenBox,
    UnavailableError,
    VectorLeg,
    WriteResult,
    collection_base,
    delete_body,
    delete_result,
    doc_path,
    error_for,
    merge_tokens,
    search_body,
)


class _Base:
    def __init__(
        self,
        url: Union[str, Sequence[str]],
        api_key: Optional[str] = None,
        *,
        tenant: Optional[str] = None,
        parent_field: str = "parent",
        timeout: float = 30.0,
        retries: int = 3,
        token: Optional[str] = None,
        collection: Optional[str] = None,
        _box: Optional[TokenBox] = None,
    ) -> None:
        urls = [url] if isinstance(url, str) else list(url)
        if not urls:
            raise ValueError("no url")
        self._urls = [u.rstrip("/") for u in urls]
        self._api_key = api_key
        self._tenant = tenant
        self._parent_field = parent_field
        self._timeout = timeout
        self._retries = retries
        self._box = _box or TokenBox(merge_tokens(token) if token else "")
        self._collection = collection
        self._base = collection_base(collection)
        self._next = 0

    @property
    def token(self) -> str:
        """The consistency token covering every write and takedown of this client and its
        tenant views. Hand it to another service so that its reads reflect them."""
        return self._box.value

    def observe(self, token: str) -> None:
        """Adds a token received from elsewhere: later reads reflect those writes too."""
        self._box.observe(token)

    def _headers(self) -> dict[str, str]:
        h = {"content-type": "application/json"}
        if self._api_key:
            h["authorization"] = f"Bearer {self._api_key}"
        if self._tenant:
            h["cairn-tenant"] = self._tenant
        return h

    def _view_args(self, **changes: Any) -> dict[str, Any]:
        args: dict[str, Any] = dict(
            url=self._urls,
            api_key=self._api_key,
            tenant=self._tenant,
            parent_field=self._parent_field,
            timeout=self._timeout,
            retries=self._retries,
            collection=self._collection,
            _box=self._box,
        )
        args.update(changes)
        return args

    def _outcome(self, res: Optional[httpx.Response], err: Optional[Exception], url: str) -> Any:
        """The decoded answer, or the error to raise (retry when it is an UnavailableError)."""
        if res is None:
            return UnavailableError(f"{url}: {err}", 0)
        try:
            data = res.json() if res.content else None
        except ValueError:
            data = None
        if res.is_success:
            return data
        message = (data or {}).get("error") if isinstance(data, dict) else None
        return error_for(res.status_code, message or res.text or res.reason_phrase)


class Client(_Base):
    """Blocking client.

    >>> db = Client("http://localhost:7200", api_key=KEY)
    >>> db.upsert([{"id": "doc-1#0", "parent": "doc-1", "text": "...", "embedding": [...]}])
    >>> hits = db.search(text="nuclear", text_field="text", filter=eq("lang", "en"))
    >>> db.delete(parent="doc-1")              # the document and all its chunks
    >>> db.with_tenant("acme").get("doc-1")    # an unscoped key acting for one tenant
    >>> db.collection("notes").search(...)      # another collection, same calls
    >>> db.forget_tenant("acme")               # erase a tenant

    Reads pass the token of this client's writes and takedowns, so it reads its own writes and
    never reads back what it deleted, through any node.
    """

    def __init__(self, url: Union[str, Sequence[str]], api_key: Optional[str] = None, **kw: Any) -> None:
        shared = kw.pop("_http", None)
        super().__init__(url, api_key, **kw)
        # Views (`with_tenant`, `collection`) share their parent's connection pool: making one
        # per request in a web server costs nothing. Only the client that created the pool
        # closes it.
        self._owns_http = shared is None
        self._http = shared or httpx.Client(timeout=self._timeout)

    def close(self) -> None:
        """Closes the connection pool (a view leaves its parent's pool open)."""
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def with_tenant(self, tenant: str) -> "Client":
        """A view acting for ``tenant`` (unscoped keys), sharing this client's token."""
        return Client(**self._view_args(tenant=tenant), _http=self._http)

    def collection(self, name: str) -> "Client":
        """A view acting on collection ``name``, sharing this client's token and tenant."""
        return Client(**self._view_args(collection=name), _http=self._http)

    def create_collection(
        self,
        name: str,
        schema: Mapping[str, Any],
        *,
        shards: Optional[int] = None,
        expires_field: Optional[str] = None,
    ) -> dict[str, Any]:
        """Creates a collection (admin key); returns once it is ready on every node. With
        ``expires_field`` (a field holding each document's expiry, Unix milliseconds), expired
        documents are hidden at once and then deleted."""
        body: dict[str, Any] = {"name": name, "schema": dict(schema)}
        if shards is not None:
            body["shards"] = shards
        if expires_field:
            body["expires_field"] = expires_field
        return self._call("POST", "/v1/collections", body)

    def list_collections(self) -> list[dict[str, Any]]:
        """The live collections, ``default`` first."""
        return self._call("GET", "/v1/collections")["collections"]

    def drop_collection(self, name: str) -> None:
        """Drops a collection and deletes its data on every node (admin key)."""
        self._call("DELETE", "/v1/collections/" + urllib.parse.quote(name, safe=""))

    def _call(self, method: str, path: str, body: Any = None) -> Any:
        last: Exception = UnavailableError("no attempt", 0)
        for attempt in range(self._retries + 1):
            if attempt:
                time.sleep(0.1 * 2 ** (attempt - 1))
            url = self._urls[self._next % len(self._urls)] + path
            res, err = None, None
            try:
                res = self._http.request(method, url, headers=self._headers(), json=body)
            except httpx.HTTPError as e:
                err = e
            out = self._outcome(res, err, url)
            if not isinstance(out, Exception):
                return out
            last = out
            if not isinstance(out, UnavailableError):
                raise out
            self._next += 1
        raise last

    def upsert(self, documents: Sequence[Document]) -> WriteResult:
        """Inserts or replaces documents."""
        body: dict[str, Any] = {"documents": list(documents)}
        if self.token:
            body["after"] = self.token
        r = self._call("POST", f"{self._base}/documents", body)
        self.observe(r["consistency_token"])
        return WriteResult(count=r["count"], token=r["consistency_token"])

    def patch(self, id: Id, set: Mapping[str, Any]) -> WriteResult:
        """Changes some fields of one document (``None`` clears a field). A missing document
        is not created. ``count`` is the number of documents changed."""
        return self.patch_many([{"id": id, "set": dict(set)}])

    def patch_many(self, patches: Sequence[Mapping[str, Any]]) -> WriteResult:
        """Changes fields of several documents: ``[{"id": ..., "set": {...}}]``."""
        body: dict[str, Any] = {"patches": [dict(p) for p in patches]}
        if self.token:
            body["after"] = self.token
        r = self._call("POST", f"{self._base}/documents/patch", body)
        self.observe(r["consistency_token"])
        return WriteResult(count=r["patched"], token=r["consistency_token"])

    def get(self, id: Id, *, consistency: Optional[Consistency] = None) -> Optional[Document]:
        """One document, or ``None`` when there is none (or it was taken down)."""
        try:
            return self._call("GET", doc_path(id, self.token, consistency, self._base))
        except NotFoundError:
            return None

    def search(
        self,
        *,
        k: Optional[int] = None,
        vector: Union[VectorLeg, Sequence[VectorLeg], None] = None,
        text: Optional[str] = None,
        text_field: Optional[str] = None,
        all_terms: bool = False,
        filter: Optional[Filter] = None,
        fusion: Optional[Mapping[str, Any]] = None,
        oversample: Optional[int] = None,
        with_documents: Optional[bool] = None,
        consistency: Optional[Consistency] = None,
        group_by: Optional[str] = None,
    ) -> list[Hit]:
        """Hybrid search: vector legs, a text leg and a filter, fused. ``group_by`` returns one
        hit per value of that field (each document once, at its best chunk)."""
        body = search_body(
            k=k, vector=vector, text=text, text_field=text_field, all_terms=all_terms, filter=filter,
            fusion=fusion, oversample=oversample, with_documents=with_documents,
            consistency=consistency, after=self.token, group_by=group_by,
        )
        return [Hit.from_json(h) for h in self._call("POST", f"{self._base}/search", body)["hits"]]

    def delete(
        self,
        *,
        ids: Optional[Sequence[Id]] = None,
        filter: Optional[Filter] = None,
        parent: Optional[Id] = None,
    ) -> DeleteResult:
        """Takes documents down: by ``ids``, by ``filter`` (every document that matches when
        the deletion is applied), by ``parent`` (a document's chunks), or ``ids`` that also
        match ``filter``."""
        body = delete_body(ids=ids, filter=filter, parent=parent, parent_field=self._parent_field, after=self.token)
        r = self._call("POST", f"{self._base}/documents/delete", body)
        self.observe(r["consistency_token"])
        return delete_result(r)

    def prove_deletion(self, ids: Sequence[Id]) -> dict[str, Any]:
        """Asks every replica whether it still holds these documents, once it has applied this
        client's takedowns, and returns the node's signed report: ``{"report", "signature",
        "public_key", "algorithm"}``. ``report["verdict"]`` is ``"deleted everywhere"`` only if
        every document is gone from every replica. Needs the takedown role."""
        body: dict[str, Any] = {"ids": list(ids)}
        if self.token:
            body["after"] = self.token
        return self._call("POST", f"{self._base}/deletions/proof", body)

    def deletion_key(self) -> str:
        """The answering node's public key (base64 Ed25519) for its proofs of deletion. Fetch it
        once from each node and keep it: check proofs against it, not against the key a
        proof carries."""
        return self._call("GET", "/v1/deletions/key")["public_key"]

    def forget_tenant(self, tenant: str) -> DeleteResult:
        """Erases a tenant: every document it holds (unscoped keys with the takedown role)."""
        q = f"?after={urllib.parse.quote(self.token)}" if self.token else ""
        r = self._call("DELETE", f"{self._base}/tenants/{urllib.parse.quote(tenant, safe='')}{q}")
        self.observe(r["consistency_token"])
        return delete_result(r)

    def schema(self) -> dict[str, Any]:
        """The collection schema (reserved fields hidden)."""
        return self._call("GET", f"{self._base}/schema")


class AsyncClient(_Base):
    """The same API as :class:`Client`, with ``async`` methods."""

    def __init__(self, url: Union[str, Sequence[str]], api_key: Optional[str] = None, **kw: Any) -> None:
        shared = kw.pop("_http", None)
        super().__init__(url, api_key, **kw)
        # Views (`with_tenant`, `collection`) share their parent's connection pool: making one
        # per request in a web server costs nothing. Only the client that created the pool
        # closes it.
        self._owns_http = shared is None
        self._http = shared or httpx.AsyncClient(timeout=self._timeout)

    async def aclose(self) -> None:
        """Closes the connection pool (a view leaves its parent's pool open)."""
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def with_tenant(self, tenant: str) -> "AsyncClient":
        """A view acting for ``tenant`` (unscoped keys), sharing this client's token."""
        return AsyncClient(**self._view_args(tenant=tenant), _http=self._http)

    def collection(self, name: str) -> "AsyncClient":
        """A view acting on collection ``name``, sharing this client's token and tenant."""
        return AsyncClient(**self._view_args(collection=name), _http=self._http)

    async def create_collection(
        self,
        name: str,
        schema: Mapping[str, Any],
        *,
        shards: Optional[int] = None,
        expires_field: Optional[str] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name, "schema": dict(schema)}
        if shards is not None:
            body["shards"] = shards
        if expires_field:
            body["expires_field"] = expires_field
        return await self._call("POST", "/v1/collections", body)

    async def list_collections(self) -> list[dict[str, Any]]:
        return (await self._call("GET", "/v1/collections"))["collections"]

    async def drop_collection(self, name: str) -> None:
        await self._call("DELETE", "/v1/collections/" + urllib.parse.quote(name, safe=""))

    async def _call(self, method: str, path: str, body: Any = None) -> Any:
        last: Exception = UnavailableError("no attempt", 0)
        for attempt in range(self._retries + 1):
            if attempt:
                await asyncio.sleep(0.1 * 2 ** (attempt - 1))
            url = self._urls[self._next % len(self._urls)] + path
            res, err = None, None
            try:
                res = await self._http.request(method, url, headers=self._headers(), json=body)
            except httpx.HTTPError as e:
                err = e
            out = self._outcome(res, err, url)
            if not isinstance(out, Exception):
                return out
            last = out
            if not isinstance(out, UnavailableError):
                raise out
            self._next += 1
        raise last

    async def upsert(self, documents: Sequence[Document]) -> WriteResult:
        body: dict[str, Any] = {"documents": list(documents)}
        if self.token:
            body["after"] = self.token
        r = await self._call("POST", f"{self._base}/documents", body)
        self.observe(r["consistency_token"])
        return WriteResult(count=r["count"], token=r["consistency_token"])

    async def patch(self, id: Id, set: Mapping[str, Any]) -> WriteResult:
        return await self.patch_many([{"id": id, "set": dict(set)}])

    async def patch_many(self, patches: Sequence[Mapping[str, Any]]) -> WriteResult:
        body: dict[str, Any] = {"patches": [dict(p) for p in patches]}
        if self.token:
            body["after"] = self.token
        r = await self._call("POST", f"{self._base}/documents/patch", body)
        self.observe(r["consistency_token"])
        return WriteResult(count=r["patched"], token=r["consistency_token"])

    async def get(self, id: Id, *, consistency: Optional[Consistency] = None) -> Optional[Document]:
        try:
            return await self._call("GET", doc_path(id, self.token, consistency, self._base))
        except NotFoundError:
            return None

    async def search(
        self,
        *,
        k: Optional[int] = None,
        vector: Union[VectorLeg, Sequence[VectorLeg], None] = None,
        text: Optional[str] = None,
        text_field: Optional[str] = None,
        all_terms: bool = False,
        filter: Optional[Filter] = None,
        fusion: Optional[Mapping[str, Any]] = None,
        oversample: Optional[int] = None,
        with_documents: Optional[bool] = None,
        consistency: Optional[Consistency] = None,
        group_by: Optional[str] = None,
    ) -> list[Hit]:
        body = search_body(
            k=k, vector=vector, text=text, text_field=text_field, all_terms=all_terms, filter=filter,
            fusion=fusion, oversample=oversample, with_documents=with_documents,
            consistency=consistency, after=self.token, group_by=group_by,
        )
        r = await self._call("POST", f"{self._base}/search", body)
        return [Hit.from_json(h) for h in r["hits"]]

    async def delete(
        self,
        *,
        ids: Optional[Sequence[Id]] = None,
        filter: Optional[Filter] = None,
        parent: Optional[Id] = None,
    ) -> DeleteResult:
        body = delete_body(ids=ids, filter=filter, parent=parent, parent_field=self._parent_field, after=self.token)
        r = await self._call("POST", f"{self._base}/documents/delete", body)
        self.observe(r["consistency_token"])
        return delete_result(r)

    async def prove_deletion(self, ids: Sequence[Id]) -> dict[str, Any]:
        body: dict[str, Any] = {"ids": list(ids)}
        if self.token:
            body["after"] = self.token
        return await self._call("POST", f"{self._base}/deletions/proof", body)

    async def deletion_key(self) -> str:
        return (await self._call("GET", "/v1/deletions/key"))["public_key"]

    async def forget_tenant(self, tenant: str) -> DeleteResult:
        q = f"?after={urllib.parse.quote(self.token)}" if self.token else ""
        r = await self._call("DELETE", f"{self._base}/tenants/{urllib.parse.quote(tenant, safe='')}{q}")
        self.observe(r["consistency_token"])
        return delete_result(r)

    async def schema(self) -> dict[str, Any]:
        return await self._call("GET", f"{self._base}/schema")
