"""
Logic to fix EXIF mismatches among duplicate groups by identifying a 'correct' source
and copying its metadata (Date, Make, Model) to the divergent files via exiftool.
Also includes logic to sync DB dates (like from filename parsing) back to files on disk.
"""

from __future__ import annotations

import subprocess
import shutil
import sqlite3
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

from .config import Config
from .db import Database

console = Console()


def fix_exif_mismatches(
    db: Database, cfg: Config, trust_source: str | None = None, dry_run: bool = False
) -> None:
    """
    Query the database for grouped files, find EXIF mismatches.
    Determine the correct EXIF info via synthesis (earliest known date, most complete make/model)
    unless a `trust_source` is explicitly provided.
    Run exiftool to overwrite the incorrect files (including organized paths).
    Update the DB.
    """

    console.print(
        "[bold cyan]Analyzing groups for EXIF mismatches to fix...[/bold cyan]"
    )

    updates = 0
    errors = 0
    total_mismatched_groups = 0

    for group_rows in db.iter_clustered_groups():
        # Determine if there is a mismatch
        exif_signatures = set()
        for row in group_rows:
            exif_signatures.add((row["exif_date"], row["exif_make"], row["exif_model"]))

        if len(exif_signatures) <= 1:
            continue  # No mismatch

        total_mismatched_groups += 1

        # We have a mismatch. Find the source of truth.
        correct_date = None
        correct_make = None
        correct_model = None

        # 1. Try to find one from the trusted source if provided
        if trust_source:
            for row in group_rows:
                if row["source"] and trust_source in row["source"]:
                    correct_date = row["exif_date"]
                    correct_make = row["exif_make"]
                    correct_model = row["exif_model"]
                    break

        # 2. Synthesize truth: earliest date, most complete make/model
        if not correct_date and not correct_make and not correct_model:
            earliest_date = None
            for row in group_rows:
                if row["exif_date"]:
                    if not earliest_date or row["exif_date"] < earliest_date:
                        earliest_date = row["exif_date"]
                if row["exif_make"] and not correct_make:
                    correct_make = row["exif_make"]
                if row["exif_model"] and not correct_model:
                    correct_model = row["exif_model"]
            correct_date = earliest_date

        # Find all files in the group that have different EXIF data
        files_to_fix = [
            row for row in group_rows
            if (row["exif_date"], row["exif_make"], row["exif_model"])
            != (correct_date, correct_make, correct_model)
        ]

        if not files_to_fix:
            continue

        # Collect paths: source file + organized copy (output_path) if present
        paths_to_update: list[Path] = []
        for r in files_to_fix:
            paths_to_update.append(Path(r["path"]))
            if r["output_path"]:
                org_p = Path(r["output_path"])
                if org_p not in paths_to_update:
                    paths_to_update.append(org_p)

        if dry_run:
            console.print(
                f"[dim]\\[DRY RUN] Group {group_rows[0]['group_id']} -> "
                f"Date: {correct_date}, Make: {correct_make}, Model: {correct_model}[/dim]"
            )
            for p in paths_to_update:
                console.print(f"  [dim]~ {p}[/dim]")
            continue

        # Non-dry-run: verify files exist before calling exiftool
        valid_paths = [str(p) for p in paths_to_update if p.exists()]

        if not valid_paths:
            console.print(
                f"[yellow]Skipping group {group_rows[0]['group_id']}: files to fix not found on disk.[/yellow]"
            )
            continue

        # Build exiftool command.
        # Use -overwrite_original_in_place to preserve hardlinks.
        cmd = ["exiftool", "-overwrite_original_in_place", "-m"]

        if correct_date:
            # exif_date in DB is ISO8601; exiftool expects YYYY:MM:DD HH:MM:SS
            exif_time = correct_date.replace("-", ":").replace("T", " ")
            cmd.extend([f"-DateTimeOriginal={exif_time}", f"-CreateDate={exif_time}"])
        else:
            cmd.extend(["-DateTimeOriginal=", "-CreateDate="])

        if correct_make:
            cmd.append(f"-Make={correct_make}")
        else:
            cmd.append("-Make=")

        if correct_model:
            cmd.append(f"-Model={correct_model}")
        else:
            cmd.append("-Model=")

        cmd.extend(valid_paths)

        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            for r in files_to_fix:
                db.conn.execute(
                    "UPDATE files SET exif_date=?, exif_make=?, exif_model=? WHERE id=?",
                    (correct_date, correct_make, correct_model, r["id"]),
                )

            db.commit()
            updates += len(valid_paths)
        except subprocess.CalledProcessError as e:
            console.print(
                f"[red]Exiftool failed for group {group_rows[0]['group_id']}: {e.stderr}[/red]"
            )
            errors += 1

    if dry_run:
        console.print("\n[bold yellow]DRY RUN Complete - No changes made[/bold yellow]")
    else:
        console.print("\n[bold green]EXIF Fix Complete[/bold green]")

    console.print(f"Groups evaluated with mismatches: {total_mismatched_groups}")
    console.print(
        f"Files evaluated for fix:          {updates if not dry_run else '0 (Dry run)'}"
    )
    if errors > 0:
        console.print(f"Errors encountered:               {errors}")


def sync_metadata_to_disk(db: Database, cfg: Config, dry_run: bool = False) -> dict:
    """
    Push DB metadata (exif_date) back to files that lack it.
    This is especially useful for files where we parsed the date from the filename.
    """
    if not shutil.which("exiftool"):
        console.print("[red]Error: exiftool not found in PATH.[/red]")
        return {"updated": 0, "errors": 0}

    # We only care about files that have an exif_date in the DB
    query = "SELECT id, path, exif_date, is_video FROM files WHERE exif_date IS NOT NULL"
    rows = db.conn.execute(query).fetchall()

    if not rows:
        console.print("No metadata in DB to sync.")
        return {"updated": 0, "errors": 0}

    summary = {"updated": 0, "errors": 0}

    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task("Syncing metadata to disk…", total=len(rows))

        for row in rows:
            path = Path(row["path"])
            if not path.exists():
                progress.advance(task)
                continue

            exif_time = row["exif_date"].replace("-", ":").replace("T", " ")
            
            # Use exiftool with -wm cg to only write IF the tag doesn't exist.
            # -wm cg: "Create new groups, don't Overwrite existing ones"
            # However, for video, we often want to write to CreateDate or DateTimeOriginal anyway.
            # The user's request was "update both the exif data for the photo accordingly and the exif create date for videos".
            # If we want to be conservative, we use -wm cg. 
            # If we want to "force" the filename date onto the file, we just write it.
            # Let's use -wm cg to be safe (don't overwrite what might be correct EXIF).
            
            cmd = [
                "exiftool", "-overwrite_original_in_place", "-m",
                "-wm", "cg",  # Only write if tag is missing
            ]

            if row["is_video"]:
                # Videos often use CreateDate or DateTimeOriginal
                cmd.extend([f"-CreateDate={exif_time}", f"-DateTimeOriginal={exif_time}"])
            else:
                # Photos use DateTimeOriginal
                cmd.extend([f"-DateTimeOriginal={exif_time}"])

            cmd.append(str(path))

            if dry_run:
                summary["updated"] += 1
                progress.advance(task)
                continue

            if sync_single_file_metadata(path, row["exif_date"], bool(row["is_video"])):
                summary["updated"] += 1
            else:
                # If we have an error, or it wasn't updated (e.g. tag already existed)
                # the count won't increment. 
                pass
            
            progress.advance(task)

    return summary


def sync_single_file_metadata(path: Path, exif_date: str, is_video: bool) -> bool:
    """Sync a single file's metadata from DB to disk."""
    if not shutil.which("exiftool") or not path.exists():
        return False

    exif_time = exif_date.replace("-", ":").replace("T", " ")
    cmd = [
        "exiftool", "-overwrite_original_in_place", "-m",
        "-wm", "cg",  # Only write if tag is missing
    ]

    if is_video:
        cmd.extend([f"-CreateDate={exif_time}", f"-DateTimeOriginal={exif_time}"])
    else:
        cmd.extend([f"-DateTimeOriginal={exif_time}"])

    cmd.append(str(path))

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return "1 image files updated" in res.stdout
    except subprocess.CalledProcessError:
        return False
