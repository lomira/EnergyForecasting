"""Shared SQLite connection lifecycle for the engine database."""

import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path


@contextmanager
def database(db_path: Path) -> Generator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as connection, connection:
        yield connection
