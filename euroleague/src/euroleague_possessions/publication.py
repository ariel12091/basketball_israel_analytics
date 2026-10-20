"""Session-owned publication markers; game transactions retain their own locks.

The connection factory must return a dedicated, autocommit session on port 5432.
Closing that session on a crash makes the SQL status reader report ``stale``.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
import re
from typing import Any, Callable, Iterator
from uuid import uuid4


class Publication:
    def __init__(self, connection: Any, league: str, schema: str) -> None:
        self.connection = connection
        self.league = league
        self.schema = schema
        self.run_id = str(uuid4())
        self.closed = False

    @classmethod
    def open(cls, conn_factory: Callable[[], Any], league: str,
             schema: str = "basketball_test") -> "Publication":
        if league not in {"il", "euroleague", "eurocup"}:
            raise ValueError("unknown publication league")
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", schema):
            raise ValueError("invalid publication schema")
        connection = conn_factory()
        publication = cls(connection, league, schema)
        try:
            if not connection.autocommit:
                raise ValueError("publication lock connection must use autocommit")
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT inet_server_port()")
                if cursor.fetchone()[0] != 5432:
                    raise ValueError("publication lock requires session port 5432")
                cursor.execute(
                    f"SELECT pg_advisory_lock({schema}.publication_lock_key(%s))",
                    (league,),
                )
                cursor.execute(f"SELECT {schema}.begin_publication(%s, %s::uuid)",
                               (league, publication.run_id))
            finally:
                cursor.close()
        except BaseException:
            connection.close()
            raise
        return publication

    def fence(self, cursor: Any) -> None:
        if self.closed:
            raise RuntimeError("publication is closed")
        cursor.execute(f"SELECT {self.schema}.fence_publication(%s, %s::uuid)",
                       (self.league, self.run_id))

    def close(self, ok: bool) -> None:
        if self.closed:
            return
        try:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"SELECT {self.schema}.finish_publication(%s, %s::uuid, %s)",
                    (self.league, self.run_id, bool(ok)),
                )
            finally:
                cursor.close()
        finally:
            self.closed = True
            self.connection.close()


@contextmanager
def publishing(conn_factory: Callable[[], Any], league: str,
               schema: str = "basketball_test") -> Iterator[Publication]:
    publication = Publication.open(conn_factory, league, schema)
    try:
        yield publication
    except BaseException:
        publication.close(False)
        raise
    else:
        publication.close(True)


_active: ContextVar[tuple[Publication, ...]] = ContextVar("publication", default=())


def fence_active(cursor: Any) -> None:
    """First operation after BEGIN in the batch loader's write transactions.

    Historical offline adapter tests and separately operated maintenance tools
    may use the adapter without the batch entry point. The batch entry point
    always establishes this scope before making its first write.
    """
    for publication in _active.get():
        publication.fence(cursor)


@contextmanager
def publishing_europe(conn_factory: Callable[[], Any],
                      schema: str = "basketball_test") -> Iterator[None]:
    # The loader refreshes global MVs across both competitions. Own both rows
    # throughout the run, in the same order in every process to avoid deadlocks.
    with ExitStack() as stack:
        publications = tuple(stack.enter_context(publishing(conn_factory, league, schema))
                             for league in ("eurocup", "euroleague"))
        token = _active.set(publications)
        try:
            yield
        finally:
            _active.reset(token)
