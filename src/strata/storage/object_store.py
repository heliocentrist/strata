from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Protocol
from urllib.parse import unquote, urlparse


class ObjectStore(Protocol):
    def open(self, uri: str) -> BinaryIO: ...

    def read_text(self, uri: str) -> str: ...

    def write_text(self, uri: str, value: str) -> str: ...

    def exists(self, uri: str) -> bool: ...

    def resolve_path(self, uri: str) -> Path: ...


class LocalObjectStore:
    def __init__(self, root: Path):
        self.root = root

    def open(self, uri: str) -> BinaryIO:
        return self.resolve_path(uri).open("rb")

    def read_text(self, uri: str) -> str:
        return self.resolve_path(uri).read_text(encoding="utf-8")

    def write_text(self, uri: str, value: str) -> str:
        path = self.resolve_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8")
        return str(path)

    def exists(self, uri: str) -> bool:
        return self.resolve_path(uri).exists()

    def resolve_path(self, uri: str) -> Path:
        parsed = urlparse(uri)
        if _is_windows_absolute_uri(uri):
            return Path(uri).resolve()
        if parsed.scheme == "file":
            return Path(unquote(parsed.path)).resolve()
        if parsed.scheme:
            raise ValueError(f"unsupported object URI scheme for local store: {parsed.scheme}")
        path = Path(uri)
        if not path.is_absolute():
            path = self.root / path
        return path.resolve()


def object_store_for_uri(uri: str, *, root: Path) -> ObjectStore:
    parsed = urlparse(uri)
    if parsed.scheme in {"", "file"} or _is_windows_absolute_uri(uri):
        return LocalObjectStore(root)
    if parsed.scheme == "s3":
        raise ValueError("s3 object store is not implemented yet")
    raise ValueError(f"unsupported object URI scheme: {parsed.scheme}")


def _is_windows_absolute_uri(uri: str) -> bool:
    return len(uri) >= 3 and uri[1] == ":" and uri[2] in {"\\", "/"}
