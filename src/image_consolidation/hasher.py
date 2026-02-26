"""Hash stage — SHA-256 (exact) + dHash (perceptual) for all ingested images."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import imagehash
from PIL import Image, UnidentifiedImageError
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

from .config import Config
from .db import Database


# ---------------------------------------------------------------------------
# Hashing helpers
# ---------------------------------------------------------------------------

def sha256(path: Path, chunk: int = 1 << 20) -> str:
    """Stream SHA-256 without loading the whole file into memory."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk_data := f.read(chunk):
            h.update(chunk_data)
    return h.hexdigest()


def dhash(path: Path, hash_size: int = 8) -> str | None:
    """
    Compute a 64-bit dHash as a hex string.
    Returns None if the image can't be decoded.
    """
    try:
        with Image.open(path) as img:
            return str(imagehash.dhash(img, hash_size=hash_size))
    except (UnidentifiedImageError, Exception):
        return None


def _process_file(path_str: str) -> tuple[str, str | None]:
    """Return (sha256, phash_hex_or_None) for a single path."""
    path = Path(path_str)
    fhash = sha256(path)
    phash = dhash(path)
    return fhash, phash


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_hash(db: Database, cfg: Config) -> dict:
    """
    Compute SHA-256 + dHash for all files in status='ingested'.
    Already-hashed files are skipped automatically (incremental).

    Returns a summary dict.
    """
    summary = {"hashed": 0, "errors": 0}

    for batch_rows in db.iter_files_needing_hash(batch=cfg.performance.batch_size * 4):
        updates: list[tuple[str, str | None, int]] = []

        with Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            transient=True,
        ) as progress:
            task = progress.add_task("Hashing…", total=len(batch_rows))

            with ThreadPoolExecutor(max_workers=cfg.performance.workers) as pool:
                future_to_id = {
                    pool.submit(_process_file, row["path"]): row["id"]
                    for row in batch_rows
                }
                for future in as_completed(future_to_id):
                    file_id = future_to_id[future]
                    try:
                        fhash, phash = future.result()
                        updates.append((fhash, phash, file_id))
                        summary["hashed"] += 1
                    except Exception:
                        summary["errors"] += 1
                    progress.advance(task)

        if updates:
            db.update_hashes_batch(updates)

    return summary
