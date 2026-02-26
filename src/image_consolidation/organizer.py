"""
Organizer stage — copy, move, or hard-link winning files into the output hierarchy.

Output structure:
  <output_dir>/YYYY/MM/filename          (when EXIF date is available)
  <output_dir>/unsorted/filename         (when no date is recoverable)

Sidecar files follow their master into the same output directory.
"""

from __future__ import annotations

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.progress import track

from .config import Config
from .db import Database

console = Console()


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(
    r"(\d{4})[-:_](\d{2})[-:_](\d{2})"  # YYYY-MM-DD or YYYY:MM:DD
)


def _output_path(
    src: Path,
    output_dir: Path,
    exif_date: str | None,
    mtime: float,
    structure: str,
    unsorted_dir: str,
) -> Path:
    """Compute the destination path for a file."""
    date_str = exif_date or ""
    m = _DATE_RE.search(date_str)

    if m:
        year, month, day = m.group(1), m.group(2), m.group(3)
    else:
        # Fallback to file modification time — mark as unreliable
        dt = datetime.fromtimestamp(mtime)
        year, month, day = str(dt.year), f"{dt.month:02d}", f"{dt.day:02d}"
        # No EXIF → unsorted
        if structure == "YYYY/MM":
            return output_dir / unsorted_dir / src.name
        return output_dir / unsorted_dir / src.name

    if structure == "YYYY/MM/DD":
        folder = output_dir / year / month / day
    else:
        folder = output_dir / year / month

    return folder / src.name


def _unique_path(dest: Path) -> Path:
    """Append _1, _2 … to stem if dest already exists."""
    if not dest.exists():
        return dest
    stem = dest.stem
    suffix = dest.suffix
    parent = dest.parent
    i = 1
    while True:
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def _same_device(src: Path, dst_dir: Path) -> bool:
    dst_dir.mkdir(parents=True, exist_ok=True)
    return os.stat(src).st_dev == os.stat(dst_dir).st_dev


# ---------------------------------------------------------------------------
# Transfer helpers
# ---------------------------------------------------------------------------

def _transfer(src: Path, dest: Path, mode: str, dry_run: bool) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        return
    if mode == "hardlink":
        if _same_device(src, dest.parent):
            os.link(src, dest)
            return
        # Fallback to copy if cross-device
        shutil.copy2(src, dest)
    elif mode == "move":
        shutil.move(str(src), dest)
    else:  # copy (default)
        shutil.copy2(src, dest)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_organize(db: Database, cfg: Config, dry_run: bool = False) -> dict:
    """
    Copy/move/hard-link best-version files to the output directory.

    dry_run=True → compute destinations and log them, but don't touch the filesystem.
    Returns a summary dict.
    """
    summary = {
        "organized": 0,
        "unsorted": 0,
        "skipped_already_done": 0,
        "errors": 0,
        "bytes_transferred": 0,
    }

    out_dir = cfg.output.directory
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    for batch in db.iter_best_files(batch=cfg.performance.batch_size):
        for row in track(batch, description="Organizing…", transient=True):
            src = Path(row["path"])
            if not src.exists():
                summary["errors"] += 1
                continue

            dest = _output_path(
                src=src,
                output_dir=out_dir,
                exif_date=row["exif_date"],
                mtime=row["mtime"],
                structure=cfg.output.structure,
                unsorted_dir=cfg.output.unsorted_dir,
            )
            dest = _unique_path(dest)

            try:
                _transfer(src, dest, mode=cfg.output.mode, dry_run=dry_run)
                summary["bytes_transferred"] += row["size"] or 0

                if cfg.output.unsorted_dir in str(dest):
                    summary["unsorted"] += 1
                else:
                    summary["organized"] += 1

                if not dry_run:
                    db.mark_organized(row["id"], str(dest))

                    # Move sidecars alongside their master
                    for sc_row in db.sidecars_for(row["id"]):
                        sc_src = Path(sc_row["path"])
                        sc_dest = dest.parent / sc_src.name
                        sc_dest = _unique_path(sc_dest)
                        if sc_src.exists():
                            _transfer(sc_src, sc_dest, mode=cfg.output.mode, dry_run=False)

            except Exception as e:
                console.print(f"[red]Error organizing {src}: {e}[/red]")
                summary["errors"] += 1

    db.commit()
    return summary
