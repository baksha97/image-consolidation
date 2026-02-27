"""SQLite database layer — all pipeline state lives here."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Generator, Iterator


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA cache_size=-65536;   -- 64 MB
PRAGMA mmap_size=268435456; -- 256 MB

CREATE TABLE IF NOT EXISTS runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT NOT NULL,
    completed_at TEXT,
    config_json  TEXT,
    status       TEXT DEFAULT 'running'  -- running | completed | failed
);

CREATE TABLE IF NOT EXISTS files (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    path         TEXT NOT NULL UNIQUE,
    source       TEXT,
    size         INTEGER,
    mtime        REAL,
    file_hash    TEXT,   -- SHA-256
    phash        TEXT,   -- dHash hex (images only)
    width        INTEGER,
    height       INTEGER,
    exif_date    TEXT,   -- ISO8601 preferred; NULL if not recoverable
    exif_make    TEXT,
    exif_model   TEXT,
    format       TEXT,   -- uppercase extension sans dot (JPEG, PNG, CR2 …)
    is_video     INTEGER DEFAULT 0,
    duration_sec REAL,   -- video duration, NULL for images
    score        REAL,
    group_id     INTEGER,
    is_best      INTEGER DEFAULT 0,
    output_path  TEXT,
    status       TEXT DEFAULT 'ingested',
    -- ingested | hashed | clustered | selected | organized
    ingested_at  TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sidecars (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    master_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    path      TEXT NOT NULL UNIQUE,
    extension TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_files_file_hash  ON files(file_hash);
CREATE INDEX IF NOT EXISTS idx_files_phash      ON files(phash);
CREATE INDEX IF NOT EXISTS idx_files_group_id   ON files(group_id);
CREATE INDEX IF NOT EXISTS idx_files_status     ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_source     ON files(source);
CREATE INDEX IF NOT EXISTS idx_files_is_best    ON files(is_best);
"""


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class FileRecord:
    path: str
    source: str = ""
    size: int = 0
    mtime: float = 0.0
    file_hash: str | None = None
    phash: str | None = None
    width: int | None = None
    height: int | None = None
    exif_date: str | None = None
    exif_make: str | None = None
    exif_model: str | None = None
    format: str = ""
    is_video: bool = False
    duration_sec: float | None = None
    score: float | None = None
    group_id: int | None = None
    is_best: bool = False
    output_path: str | None = None
    status: str = "ingested"
    ingested_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    last_seen_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    id: int | None = None


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "Database":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected — call connect() first")
        return self._conn

    # ------------------------------------------------------------------
    # Run tracking
    # ------------------------------------------------------------------

    def start_run(self, config_json: str = "") -> int:
        cur = self.conn.execute(
            "INSERT INTO runs (started_at, config_json) VALUES (?, ?)",
            (datetime.utcnow().isoformat(), config_json),
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def finish_run(self, run_id: int, status: str = "completed") -> None:
        self.conn.execute(
            "UPDATE runs SET completed_at=?, status=? WHERE id=?",
            (datetime.utcnow().isoformat(), status, run_id),
        )
        self.conn.commit()

    def last_run(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT 1"
        ).fetchone()

    # ------------------------------------------------------------------
    # File records
    # ------------------------------------------------------------------

    def upsert_file(self, rec: FileRecord) -> int:
        """Insert or update a file record. Returns the row id."""
        now = datetime.utcnow().isoformat()
        cur = self.conn.execute(
            """
            INSERT INTO files (
                path, source, size, mtime, file_hash, phash,
                width, height, exif_date, exif_make, exif_model,
                format, is_video, duration_sec,
                score, group_id, is_best, output_path,
                status, ingested_at, last_seen_at
            ) VALUES (
                :path, :source, :size, :mtime, :file_hash, :phash,
                :width, :height, :exif_date, :exif_make, :exif_model,
                :format, :is_video, :duration_sec,
                :score, :group_id, :is_best, :output_path,
                :status, :ingested_at, :last_seen_at
            )
            ON CONFLICT(path) DO UPDATE SET
                source       = excluded.source,
                size         = excluded.size,
                mtime        = excluded.mtime,
                last_seen_at = excluded.last_seen_at,
                status       = CASE
                    WHEN files.status = 'organized' THEN files.status
                    ELSE excluded.status
                END
            """,
            {
                "path": rec.path,
                "source": rec.source,
                "size": rec.size,
                "mtime": rec.mtime,
                "file_hash": rec.file_hash,
                "phash": rec.phash,
                "width": rec.width,
                "height": rec.height,
                "exif_date": rec.exif_date,
                "exif_make": rec.exif_make,
                "exif_model": rec.exif_model,
                "format": rec.format,
                "is_video": int(rec.is_video),
                "duration_sec": rec.duration_sec,
                "score": rec.score,
                "group_id": rec.group_id,
                "is_best": int(rec.is_best),
                "output_path": rec.output_path,
                "status": rec.status,
                "ingested_at": rec.ingested_at or now,
                "last_seen_at": now,
            },
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def upsert_files_batch(self, records: list[FileRecord]) -> None:
        """Batch upsert for performance."""
        now = datetime.utcnow().isoformat()
        rows = [
            (
                r.path,
                r.source,
                r.size,
                r.mtime,
                r.file_hash,
                r.phash,
                r.width,
                r.height,
                r.exif_date,
                r.exif_make,
                r.exif_model,
                r.format,
                int(r.is_video),
                r.duration_sec,
                r.score,
                r.group_id,
                int(r.is_best),
                r.output_path,
                r.status,
                r.ingested_at or now,
                now,
            )
            for r in records
        ]
        self.conn.executemany(
            """
            INSERT INTO files (
                path, source, size, mtime, file_hash, phash,
                width, height, exif_date, exif_make, exif_model,
                format, is_video, duration_sec,
                score, group_id, is_best, output_path,
                status, ingested_at, last_seen_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(path) DO UPDATE SET
                source       = excluded.source,
                size         = excluded.size,
                mtime        = excluded.mtime,
                last_seen_at = excluded.last_seen_at,
                status       = CASE
                    WHEN files.status = 'organized' THEN files.status
                    ELSE excluded.status
                END
            """,
            rows,
        )
        self.conn.commit()

    def get_file_by_path(self, path: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM files WHERE path=?", (path,)).fetchone()

    def is_file_unchanged(self, path: str, size: int, mtime: float) -> bool:
        """True if the file is already in the DB with matching size+mtime."""
        row = self.conn.execute(
            "SELECT size, mtime FROM files WHERE path=?", (path,)
        ).fetchone()
        if row is None:
            return False
        # 2.0 s tolerance handles FAT32/older-NTFS 2-second mtime granularity
        return row["size"] == size and abs(row["mtime"] - mtime) < 2.0

    def load_file_fingerprints(self) -> dict[str, tuple[int, float]]:
        """Load {path: (size, mtime)} for all files in one query.

        Used by the ingest stage to pre-filter unchanged files before
        submitting expensive metadata extraction to the thread pool.
        """
        rows = self.conn.execute("SELECT path, size, mtime FROM files").fetchall()
        return {row["path"]: (row["size"], row["mtime"]) for row in rows}

    def iter_files_needing_hash(self, batch: int = 1000) -> Iterator[list[sqlite3.Row]]:
        last_id = -1
        while True:
            rows = self.conn.execute(
                "SELECT * FROM files WHERE status='ingested' AND id > ? ORDER BY id ASC LIMIT ?",
                (last_id, batch),
            ).fetchall()
            if not rows:
                break
            yield rows
            last_id = rows[-1]["id"]

    def iter_all_hashed_videos(self, batch: int = 5000) -> Iterator[list[sqlite3.Row]]:
        last_id = -1
        while True:
            rows = self.conn.execute(
                "SELECT id, file_hash FROM files WHERE file_hash IS NOT NULL AND is_video=1 AND id > ? ORDER BY id ASC LIMIT ?",
                (last_id, batch),
            ).fetchall()
            if not rows:
                break
            yield rows
            last_id = rows[-1]["id"]

    def update_hash(self, file_id: int, file_hash: str, phash: str | None) -> None:
        self.conn.execute(
            "UPDATE files SET file_hash=?, phash=?, status='hashed' WHERE id=?",
            (file_hash, phash, file_id),
        )

    def update_hashes_batch(self, rows: list[tuple[str, str | None, int]]) -> None:
        """rows = [(file_hash, phash, id), ...]"""
        self.conn.executemany(
            "UPDATE files SET file_hash=?, phash=?, status='hashed' WHERE id=?",
            rows,
        )
        self.conn.commit()

    def iter_all_hashed_images(self, batch: int = 5000) -> Iterator[list[sqlite3.Row]]:
        last_id = -1
        while True:
            rows = self.conn.execute(
                """SELECT id, phash, file_hash, exif_date, size, format
                   FROM files WHERE phash IS NOT NULL AND is_video=0 AND id > ?
                   ORDER BY id ASC LIMIT ?""",
                (last_id, batch),
            ).fetchall()
            if not rows:
                break
            yield rows
            last_id = rows[-1]["id"]

    def update_group_batch(self, rows: list[tuple[int | None, int]]) -> None:
        """rows = [(group_id, file_id), ...]"""
        self.conn.executemany(
            """UPDATE files SET group_id=?,
               status = CASE WHEN status='organized' THEN 'organized' ELSE 'clustered' END
               WHERE id=?""",
            rows,
        )
        self.conn.commit()

    def iter_clustered_groups(self) -> Iterator[list[sqlite3.Row]]:
        """Yield all files for each duplicate group using a single ordered scan."""
        cursor = self.conn.execute(
            "SELECT * FROM files WHERE group_id IS NOT NULL ORDER BY group_id ASC"
        )
        current_gid: int | None = None
        current_group: list[sqlite3.Row] = []
        for row in cursor:
            gid = row["group_id"]
            if gid != current_gid:
                if current_group:
                    yield current_group
                current_group = [row]
                current_gid = gid
            else:
                current_group.append(row)
        if current_group:
            yield current_group

    def mark_best(self, file_id: int, score: float) -> None:
        self.conn.execute(
            """UPDATE files SET is_best=1, score=?,
               status = CASE WHEN status='organized' THEN 'organized' ELSE 'selected' END
               WHERE id=?""",
            (score, file_id),
        )

    def mark_not_best(self, file_id: int, score: float) -> None:
        self.conn.execute(
            """UPDATE files SET is_best=0, score=?,
               status = CASE WHEN status='organized' THEN 'organized' ELSE 'selected' END
               WHERE id=?""",
            (score, file_id),
        )

    def commit(self) -> None:
        self.conn.commit()

    def iter_best_files(self, batch: int = 1000) -> Iterator[list[sqlite3.Row]]:
        """Files that won their group and haven't been organized yet."""
        last_id = -1
        while True:
            rows = self.conn.execute(
                """SELECT * FROM files
                   WHERE is_best=1 AND status='selected' AND id > ?
                   ORDER BY id ASC
                   LIMIT ?""",
                (last_id, batch),
            ).fetchall()
            if not rows:
                break
            yield rows
            last_id = rows[-1]["id"]

    def iter_unsorted_files_to_promote(self, unsorted_dir: str, batch: int = 1000) -> Iterator[list[sqlite3.Row]]:
        """Files that are in 'unsorted' but now have an exif_date."""
        last_id = -1
        while True:
            # Match path containing /unsorted/ and having an exif_date
            # status='organized' means it was already placed in the unsorted folder
            pattern = f"%/{unsorted_dir}/%"
            rows = self.conn.execute(
                """SELECT * FROM files
                   WHERE is_best=1 AND status='organized' 
                   AND output_path LIKE ? 
                   AND exif_date IS NOT NULL
                   AND id > ?
                   ORDER BY id ASC
                   LIMIT ?""",
                (pattern, last_id, batch),
            ).fetchall()
            if not rows:
                break
            yield rows
            last_id = rows[-1]["id"]

    def mark_organized(self, file_id: int, output_path: str) -> None:
        self.conn.execute(
            "UPDATE files SET output_path=?, status='organized' WHERE id=?",
            (output_path, file_id),
        )

    def clear_organized(self, file_id: int) -> None:
        """Reset a demoted file's organized state after its output copy is deleted."""
        self.conn.execute(
            "UPDATE files SET output_path=NULL, status='selected' WHERE id=?",
            (file_id,),
        )

    def iter_stale_organized(self) -> list:
        """Return all files that are organized in the output dir but are no longer winners."""
        return self.conn.execute(
            """SELECT id, path, output_path FROM files
               WHERE is_best=0 AND status='organized' AND output_path IS NOT NULL"""
        ).fetchall()

    # ------------------------------------------------------------------
    # Sidecars
    # ------------------------------------------------------------------

    def upsert_sidecar(self, master_id: int, path: str, ext: str) -> None:
        self.conn.execute(
            """INSERT INTO sidecars (master_id, path, extension)
               VALUES (?,?,?)
               ON CONFLICT(path) DO UPDATE SET master_id=excluded.master_id""",
            (master_id, path, ext),
        )

    def sidecars_for(self, master_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM sidecars WHERE master_id=?", (master_id,)
        ).fetchall()

    # ------------------------------------------------------------------
    # Stats helpers (used by reporter)
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        c = self.conn
        total = c.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        images = c.execute("SELECT COUNT(*) FROM files WHERE is_video=0").fetchone()[0]
        videos = c.execute("SELECT COUNT(*) FROM files WHERE is_video=1").fetchone()[0]
        groups = c.execute(
            "SELECT COUNT(DISTINCT group_id) FROM files WHERE group_id IS NOT NULL"
        ).fetchone()[0]
        duplicates = c.execute(
            """SELECT COUNT(*) FROM files
               WHERE group_id IS NOT NULL AND is_best=0"""
        ).fetchone()[0]
        organized = c.execute(
            "SELECT COUNT(*) FROM files WHERE status='organized'"
        ).fetchone()[0]
        dup_bytes = c.execute(
            """SELECT COALESCE(SUM(size),0) FROM files
               WHERE group_id IS NOT NULL AND is_best=0"""
        ).fetchone()[0]
        unsorted = c.execute(
            "SELECT COUNT(*) FROM files WHERE output_path LIKE '%/unsorted/%'"
        ).fetchone()[0]
        return {
            "total": total,
            "images": images,
            "videos": videos,
            "duplicate_groups": groups,
            "duplicate_files": duplicates,
            "duplicate_bytes": dup_bytes,
            "organized": organized,
            "unsorted": unsorted,
        }

    def source_breakdown(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT source,
                      COUNT(*) as total,
                      SUM(CASE WHEN is_best=1 THEN 1 ELSE 0 END) as kept,
                      SUM(CASE WHEN group_id IS NOT NULL AND is_best=0 THEN 1 ELSE 0 END) as dupes,
                      SUM(CASE WHEN group_id IS NULL  AND is_best=1 THEN 1 ELSE 0 END) as exclusive,
                      SUM(CASE WHEN group_id IS NOT NULL AND is_best=1 THEN 1 ELSE 0 END) as won_vs_counterpart
               FROM files GROUP BY source ORDER BY total DESC"""
        ).fetchall()

    def review_groups(self, limit: int = 50, sort_by: str = "size") -> list[dict]:
        """Return the top duplicate groups with all member files for the review report.

        sort_by:
          'size'       — groups with the most wasted bytes first (default)
          'count'      — groups with the most files first
          'suspicious' — groups where winner/loser scores are closest (least confident choices first)
        """
        order = {
            "count":      "file_count DESC, total_bytes DESC",
            "suspicious": "score_gap ASC, total_bytes DESC",
        }.get(sort_by, "total_bytes DESC")

        groups = self.conn.execute(
            f"""SELECT group_id,
                       COUNT(*) as file_count,
                       SUM(size) as total_bytes,
                       MAX(score) - MIN(score) as score_gap,
                       CASE WHEN COUNT(DISTINCT file_hash) > 1 THEN 1 ELSE 0 END as is_near_dup
                FROM files WHERE group_id IS NOT NULL
                GROUP BY group_id
                ORDER BY {order}
                LIMIT ?""",
            (limit,),
        ).fetchall()

        if not groups:
            return []

        group_ids = [g["group_id"] for g in groups]
        placeholders = ",".join("?" * len(group_ids))
        files = self.conn.execute(
            f"""SELECT id, path, format, width, height, size, exif_date,
                       exif_make, exif_model, score, is_best, file_hash, group_id
                FROM files WHERE group_id IN ({placeholders})
                ORDER BY group_id ASC, is_best DESC, score DESC""",
            group_ids,
        ).fetchall()

        by_group: dict[int, list] = {}
        for row in files:
            gid = row["group_id"]
            by_group.setdefault(gid, []).append(dict(row))

        return [
            {
                "group_id":    g["group_id"],
                "file_count":  g["file_count"],
                "total_bytes": g["total_bytes"],
                "score_gap":   g["score_gap"],
                "is_near_dup": bool(g["is_near_dup"]),
                "files":       by_group.get(g["group_id"], []),
            }
            for g in groups
        ]

    def top_duplicate_groups(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT group_id,
                      COUNT(*) as count,
                      MAX(width*height) as max_px,
                      SUM(size) as total_bytes
               FROM files WHERE group_id IS NOT NULL
               GROUP BY group_id ORDER BY total_bytes DESC LIMIT ?""",
            (limit,),
        ).fetchall()
