"""Ingest stage — scan source directories, extract metadata, populate DB."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import exifread
from PIL import Image, UnidentifiedImageError
try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:
    pass
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

from .config import Config
from .db import Database, FileRecord
from .filename_parser import parse_filename_date


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

_EXIF_DATE_TAGS = [
    "EXIF DateTimeOriginal",
    "EXIF DateTimeDigitized",
    "Image DateTime",
]
_EXIF_DATE_FMT = "%Y:%m:%d %H:%M:%S"


def _parse_exif_date(raw: str) -> str | None:
    raw = raw.strip()
    if not raw or raw.startswith("0000"):
        return None
    try:
        dt = datetime.strptime(raw, _EXIF_DATE_FMT)
        return dt.isoformat()
    except ValueError:
        return None


def _exif_from_pillow(path: Path) -> dict:
    """Fast EXIF extraction via Pillow (~5-20ms per image)."""
    result: dict = {}
    try:
        with Image.open(path) as img:
            result["width"] = img.width
            result["height"] = img.height
            result["format"] = img.format or ""

            exif_data = img._getexif()  # type: ignore[attr-defined]
            if not exif_data:
                return result

            from PIL.ExifTags import TAGS
            tag_map = {v: k for k, v in TAGS.items()}

            for tag_name in ["DateTimeOriginal", "DateTimeDigitized", "DateTime"]:
                tag_id = tag_map.get(tag_name)
                if tag_id and tag_id in exif_data:
                    parsed = _parse_exif_date(str(exif_data[tag_id]))
                    if parsed:
                        result["exif_date"] = parsed
                        break

            make_id = tag_map.get("Make")
            model_id = tag_map.get("Model")
            if make_id and make_id in exif_data:
                result["exif_make"] = str(exif_data[make_id]).strip("\x00").strip()
            if model_id and model_id in exif_data:
                result["exif_model"] = str(exif_data[model_id]).strip("\x00").strip()
    except (UnidentifiedImageError, Exception):
        pass
    return result


def _exif_from_exifread(path: Path) -> dict:
    """Fallback EXIF via exifread — handles RAW and edge cases."""
    result: dict = {}
    try:
        with open(path, "rb") as f:
            tags = exifread.process_file(f, stop_tag="EXIF DateTimeOriginal", details=False)
        for tag_name in _EXIF_DATE_TAGS:
            if tag_name in tags:
                parsed = _parse_exif_date(str(tags[tag_name]))
                if parsed:
                    result["exif_date"] = parsed
                    break
        for key, dest in [("Image Make", "exif_make"), ("Image Model", "exif_model")]:
            if key in tags:
                result[dest] = str(tags[key]).strip()
    except Exception:
        pass
    return result


def _video_metadata_ffprobe(path: Path) -> dict:
    """Extract video metadata via ffprobe."""
    result: dict = {}
    if not shutil.which("ffprobe"):
        return result

    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-show_format", str(path)
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        data = json.loads(out)

        # Get duration from format
        fmt = data.get("format", {})
        if "duration" in fmt:
            result["duration_sec"] = float(fmt["duration"])
        
        # Get creation_time from format tags
        tags = fmt.get("tags", {})
        creation_time = tags.get("creation_time")
        if creation_time:
            # ffprobe creation_time is often ISO8601 UTC
            # e.g. 2021-05-10T12:34:56.000000Z
            try:
                # Basic ISO parse
                dt = datetime.fromisoformat(creation_time.replace("Z", "+00:00"))
                # Convert to local New York time as per user request
                from .filename_parser import LOCAL_TZ
                result["exif_date"] = dt.astimezone(LOCAL_TZ).replace(tzinfo=None).isoformat()
            except ValueError:
                pass

        # Get dimensions from first video stream
        for stream in data.get("streams", []):
            if stream.get("codec_type") == "video":
                result["width"] = int(stream.get("width", 0))
                result["height"] = int(stream.get("height", 0))
                # Rotate check
                rotate = stream.get("tags", {}).get("rotate")
                if rotate and int(rotate) in (90, 270):
                    result["width"], result["height"] = result["height"], result["width"]
                break
    except Exception:
        pass
    return result


def _extract_metadata(path: Path, cfg: Config) -> FileRecord:
    """Return a FileRecord for a single file. Never raises."""
    stat = path.stat()
    rec = FileRecord(
        path=str(path),
        source=_find_source(path, cfg),
        size=stat.st_size,
        mtime=stat.st_mtime,
        format=path.suffix.lstrip(".").upper(),
        is_video=cfg.formats.is_video(path),
    )

    meta: dict = {}
    if cfg.formats.is_image(path):
        meta = _exif_from_pillow(path)

        # If Pillow didn't get a date but it's a format exifread knows better (RAW, HEIC …)
        if "exif_date" not in meta:
            fallback = _exif_from_exifread(path)
            meta.update({k: v for k, v in fallback.items() if k not in meta})

    elif cfg.formats.is_video(path):
        meta = _video_metadata_ffprobe(path)

    # Common assignments
    rec.width = meta.get("width")
    rec.height = meta.get("height")
    rec.exif_date = meta.get("exif_date")
    rec.exif_make = meta.get("exif_make")
    rec.exif_model = meta.get("exif_model")
    rec.duration_sec = meta.get("duration_sec")
    if meta.get("format"):
        rec.format = meta["format"]

    # Fallback to filename parsing if no date found yet
    if not rec.exif_date:
        rec.exif_date = parse_filename_date(str(path))

    return rec


def _find_source(path: Path, cfg: Config) -> str:
    """Return the source root directory that contains this path."""
    for src in cfg.sources.paths:
        try:
            path.relative_to(src)
            return str(src)
        except ValueError:
            continue
    return str(path.parent)


# ---------------------------------------------------------------------------
# Sidecar detection
# ---------------------------------------------------------------------------

def _find_sidecars(path: Path, sidecar_exts: list[str]) -> list[Path]:
    """Return sidecar files adjacent to *path* (e.g. IMG_001.xmp)."""
    stem = path.stem
    parent = path.parent
    found: list[Path] = []
    for ext in sidecar_exts:
        candidates = [
            parent / f"{stem}{ext}",
            parent / f"{path.name}{ext}",  # IMG_001.jpg.xmp
        ]
        for c in candidates:
            if c.exists() and c != path:
                found.append(c)
    return found


# ---------------------------------------------------------------------------
# Directory scan
# ---------------------------------------------------------------------------

def _scan_directory(root: Path, cfg: Config) -> list[Path]:
    """Recursively collect all supported files under *root*."""
    files: list[Path] = []
    for dirpath, _dirs, filenames in os.walk(root):
        for name in filenames:
            p = Path(dirpath) / name
            if cfg.formats.is_supported(p):
                files.append(p)
    return files


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_ingest(db: Database, cfg: Config, incremental: bool = True) -> dict:
    """
    Scan all source directories and populate the database.

    incremental=True  → skip files whose (path, size, mtime) haven't changed.
    Returns a summary dict.
    """
    summary = {"scanned": 0, "new": 0, "skipped": 0, "errors": 0}

    # Collect all file paths first
    all_files: list[Path] = []
    for src in cfg.sources.paths:
        if not src.exists():
            print(f"[warn] source not found, skipping: {src}")
            continue
        all_files.extend(_scan_directory(src, cfg))

    summary["scanned"] = len(all_files)

    # Separate sidecars from primary files for processing order
    sidecar_ext_set = set(cfg.formats.sidecar_extensions)
    primary_files = [f for f in all_files if f.suffix.lower() not in sidecar_ext_set]
    sidecar_files = [f for f in all_files if f.suffix.lower() in sidecar_ext_set]

    # Preload fingerprints once so we can skip unchanged files before
    # submitting any work to the thread pool.
    fingerprints: dict[str, tuple[int, float]] = {}
    if incremental:
        fingerprints = db.load_file_fingerprints()

    def _is_unchanged(p: Path) -> bool:
        entry = fingerprints.get(str(p))
        if entry is None:
            return False
        db_size, db_mtime = entry
        try:
            st = p.stat()
            return st.st_size == db_size and abs(st.st_mtime - db_mtime) < 2.0
        except OSError:
            return False

    # Pre-filter: only submit files that need (re-)processing
    to_process: list[Path] = []
    for p in primary_files:
        if incremental and _is_unchanged(p):
            summary["skipped"] += 1
        else:
            to_process.append(p)

    batch: list[FileRecord] = []

    def _flush(force: bool = False) -> None:
        if batch and (force or len(batch) >= cfg.performance.batch_size):
            db.upsert_files_batch(batch)
            batch.clear()

    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task("Ingesting files…", total=len(to_process))

        with ThreadPoolExecutor(max_workers=cfg.performance.workers) as pool:
            futures = {
                pool.submit(_extract_metadata, p, cfg): p
                for p in to_process
            }
            for future in as_completed(futures):
                p = futures[future]
                try:
                    rec = future.result()
                    batch.append(rec)
                    summary["new"] += 1
                    _flush()
                except Exception:
                    summary["errors"] += 1
                progress.advance(task)

        _flush(force=True)

        # Now attach sidecars to their masters
        sc_task = progress.add_task("Linking sidecars…", total=len(sidecar_files))
        for sc_path in sidecar_files:
            try:
                # Find master by stripping sidecar extension
                # e.g. IMG_001.jpg.xmp → IMG_001.jpg  or  IMG_001.xmp → IMG_001.*
                if sc_path.stem.lower().endswith(
                    tuple(e.lstrip(".") for e in cfg.formats.image_extensions + cfg.formats.video_extensions)
                ):
                    master_path = sc_path.parent / sc_path.stem
                else:
                    # look for master with any supported ext
                    master_path = None
                    for ext in cfg.formats.image_extensions + cfg.formats.video_extensions:
                        candidate = sc_path.parent / (sc_path.stem + ext)
                        if candidate.exists():
                            master_path = candidate
                            break

                if master_path is not None:
                    row = db.get_file_by_path(str(master_path))
                    if row is not None:
                        db.upsert_sidecar(row["id"], str(sc_path), sc_path.suffix.lower())
            except Exception:
                summary["errors"] += 1
            progress.advance(sc_task)

        db.commit()

    return summary


def run_backfill(db: Database, cfg: Config) -> dict:
    """
    Find files in DB missing metadata (exif_date or duration_sec) 
    and re-attempt extraction without re-hashing.
    """
    summary = {"evaluated": 0, "updated": 0, "errors": 0}
    
    # Find videos missing duration or any file missing exif_date
    query = """
        SELECT * FROM files 
        WHERE (is_video = 1 AND duration_sec IS NULL)
           OR (exif_date IS NULL)
    """
    rows = db.conn.execute(query).fetchall()
    summary["evaluated"] = len(rows)

    if not rows:
        return summary

    batch: list[FileRecord] = []

    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task("Backfilling metadata…", total=len(rows))

        for row in rows:
            path = Path(row["path"])
            if not path.exists():
                progress.advance(task)
                continue

            try:
                # Re-run extraction
                new_rec = _extract_metadata(path, cfg)
                
                # Check if we actually found something new
                changed = False
                if new_rec.exif_date != row["exif_date"]:
                    changed = True
                if new_rec.duration_sec != row["duration_sec"]:
                    changed = True
                
                if changed:
                    # Update only the metadata fields to be safe
                    db.conn.execute(
                        """UPDATE files SET 
                           exif_date = ?, 
                           duration_sec = ?, 
                           width = COALESCE(width, ?), 
                           height = COALESCE(height, ?) 
                           WHERE id = ?""",
                        (new_rec.exif_date, new_rec.duration_sec, new_rec.width, new_rec.height, row["id"])
                    )
                    summary["updated"] += 1
            except Exception:
                summary["errors"] += 1
            
            progress.advance(task)
        
        db.commit()

    return summary
