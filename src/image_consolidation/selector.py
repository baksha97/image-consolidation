"""
Selector stage — within each duplicate group, pick the best version.

Score = weighted sum of:
  - Resolution        45%  (pixels / 50 MP ceiling)
  - Date uniqueness   20%  (1 / count of group members sharing the same date;
                             penalises batch-import copies all stamped with the
                             same wrong date while rewarding files with organic,
                             unique capture dates)
  - Format quality    20%  (RAW > TIFF > PNG > HEIC > JPEG)
  - EXIF completeness 10%  (has date, make, model)
  - Source priority    5%  (user-defined ranking)
"""

from __future__ import annotations

from collections import Counter

from rich.console import Console
from rich.progress import track

from .config import Config
from .db import Database

console = Console()


# ---------------------------------------------------------------------------
# Format quality weights
# ---------------------------------------------------------------------------

_FORMAT_WEIGHT: dict[str, float] = {
    # RAW — lossless, original sensor data
    "RAW": 1.0, "CR2": 1.0, "CR3": 1.0, "NEF": 1.0,
    "ARW": 1.0, "DNG": 1.0, "ORF": 1.0, "RW2": 1.0,
    "RAF": 1.0, "PEF": 1.0, "SRW": 1.0, "X3F": 1.0,
    # TIFF — lossless, common for scans
    "TIFF": 0.90, "TIF": 0.90,
    # PNG — lossless
    "PNG": 0.80,
    # HEIC/HEIF — very efficient, moderate fidelity
    "HEIC": 0.72, "HEIF": 0.72,
    # WEBP — lossy/lossless hybrid
    "WEBP": 0.65,
    # JPEG — lossy
    "JPEG": 0.60, "JPG": 0.60,
}
_FORMAT_WEIGHT_DEFAULT = 0.50
_MAX_PIXELS = 50_000_000  # 50 MP ceiling for normalisation


def compute_score(
    width: int | None,
    height: int | None,
    fmt: str,
    exif_date: str | None,
    exif_make: str | None,
    exif_model: str | None,
    source_priority: int,
    max_source_priority: int = 10,
    date_uniqueness: float = 1.0,
) -> float:
    pixels = (width or 0) * (height or 0)
    res_score = min(pixels / _MAX_PIXELS, 1.0)

    fmt_score = _FORMAT_WEIGHT.get(fmt.upper(), _FORMAT_WEIGHT_DEFAULT)

    exif_fields = [exif_date, exif_make, exif_model]
    exif_score = sum(1 for f in exif_fields if f) / len(exif_fields)

    denom = max(max_source_priority, 1)
    src_score = source_priority / denom

    return (
        res_score        * 0.45
        + date_uniqueness  * 0.20
        + fmt_score        * 0.20
        + exif_score       * 0.10
        + src_score        * 0.05
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_select(db: Database, cfg: Config) -> dict:
    """
    Score every file and mark the best version in each duplicate group.
    Singleton files (no group) are automatically marked best.

    Returns a summary dict.
    """
    summary = {"groups_scored": 0, "singletons": 0}

    max_priority = max(cfg.sources.priorities.values(), default=1) or 1

    # ------------------------------------------------------------------
    # Score and select within each duplicate group
    # ------------------------------------------------------------------
    for group_rows in track(
        db.iter_clustered_groups(), description="Scoring groups…"
    ):
        best_id: int | None = None
        best_score: float = -1.0
        scores: dict[int, float] = {}

        # Count how many files in this group share each capture date (YYYY-MM-DD).
        # A date shared by many members is a strong signal of a batch-import
        # timestamp rather than a genuine capture date.
        date_counts: Counter = Counter(
            (row["exif_date"] or "")[:10] for row in group_rows
        )

        for row in group_rows:
            dk = (row["exif_date"] or "")[:10]
            date_uniqueness = 1.0 / date_counts[dk]

            score = compute_score(
                width=row["width"],
                height=row["height"],
                fmt=row["format"] or "",
                exif_date=row["exif_date"],
                exif_make=row["exif_make"],
                exif_model=row["exif_model"],
                source_priority=cfg.source_priority(row["path"]),
                max_source_priority=max_priority,
                date_uniqueness=date_uniqueness,
            )
            scores[row["id"]] = score
            if score > best_score:
                best_score = score
                best_id = row["id"]

        if best_id is None:
            continue

        summary["groups_scored"] += 1
        for row in group_rows:
            score = scores[row["id"]]
            if row["id"] == best_id:
                db.mark_best(row["id"], score)
            else:
                db.mark_not_best(row["id"], score)

    # ------------------------------------------------------------------
    # Files that weren't in any duplicate group — mark them best too
    # ------------------------------------------------------------------
    last_id = -1
    batch_size = 2000
    while True:
        singletons = db.conn.execute(
            "SELECT id, width, height, format, exif_date, exif_make, exif_model, path, status "
            "FROM files WHERE group_id IS NULL AND status='clustered' AND id > ? "
            "ORDER BY id ASC LIMIT ?",
            (last_id, batch_size),
        ).fetchall()
        if not singletons:
            break
        for row in track(singletons, description="Marking singletons…", transient=True):
            score = compute_score(
                width=row["width"],
                height=row["height"],
                fmt=row["format"] or "",
                exif_date=row["exif_date"],
                exif_make=row["exif_make"],
                exif_model=row["exif_model"],
                source_priority=cfg.source_priority(row["path"]),
                max_source_priority=max_priority,
            )
            db.mark_best(row["id"], score)
            summary["singletons"] += 1
        last_id = singletons[-1]["id"]

    db.commit()
    return summary
