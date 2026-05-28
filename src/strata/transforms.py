from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from strata.models import ArtifactDocument, ArtifactEnvelope


class MissingPdfParserError(RuntimeError):
    pass


def parse_document(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md"}:
        return path.read_text(encoding="utf-8")
    if suffix == ".pdf":
        return parse_pdf(path)
    raise ValueError(f"unsupported file extension: {suffix}")


def parse_pdf(path: Path) -> str:
    try:
        import liteparse  # type: ignore[import-not-found]
    except ImportError as exc:
        raise MissingPdfParserError(
            "PDF parsing requires the configured liteparse parser dependency"
        ) from exc

    if hasattr(liteparse, "LiteParse"):
        parser = liteparse.LiteParse()  # type: ignore[attr-defined]
        result = parser.parse(path)
        if isinstance(result, str):
            return result
        if hasattr(result, "text"):
            return str(result.text)
        return str(result)

    raise MissingPdfParserError("installed liteparse package does not expose LiteParse")


def fixed_char_chunks(text: str, *, max_chars: int, overlap_chars: int) -> list[str]:
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    if overlap_chars < 0:
        raise ValueError("overlap_chars must be non-negative")
    if overlap_chars >= max_chars:
        raise ValueError("overlap_chars must be smaller than max_chars")

    chunks: list[str] = []
    start = 0
    text_len = len(text)
    while start < text_len:
        end = min(start + max_chars, text_len)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end == text_len:
            break
        start = end - overlap_chars
    return chunks


def fake_embedding(text: str, *, dimensions: int) -> list[float]:
    if dimensions <= 0:
        raise ValueError("dimensions must be positive")
    values: list[float] = []
    counter = 0
    while len(values) < dimensions:
        digest = hashlib.sha256(f"{counter}:{text}".encode()).digest()
        for idx in range(0, len(digest), 4):
            if len(values) == dimensions:
                break
            integer = int.from_bytes(digest[idx : idx + 4], "big")
            values.append((integer / 2**32) * 2 - 1)
        counter += 1
    return values


def artifact_payload(
    data: Any,
    metadata: dict[str, Any],
    artifact: ArtifactEnvelope,
) -> str:
    document = ArtifactDocument(artifact=artifact, data=data, metadata=metadata)
    return document.model_dump_json(indent=2)
