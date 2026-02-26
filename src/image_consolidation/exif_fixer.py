"""
Logic to fix EXIF mismatches among duplicate groups by identifying a 'correct' source
and copying its metadata (Date, Make, Model) to the divergent files via exiftool.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from rich.console import Console

from .config import Config
from .db import Database

console = Console()


def fix_exif_mismatches(
    db: Database, cfg: Config, trust_source: str | None = None
) -> None:
    """
    Query the database for grouped files, find EXIF mismatches.
    Determine the correct EXIF info (from the `trust_source` else the `is_best` file).
    Run exiftool to overwrite the incorrect files.
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
        truth_row = None

        # 1. Try to find one from the trusted source if provided
        if trust_source:
            for row in group_rows:
                if row["source"] and trust_source in row["source"]:
                    truth_row = row
                    break

        # 2. Fallback to the 'is_best' file chosen by the selector stage
        if not truth_row:
            for row in group_rows:
                if row["is_best"] == 1:
                    truth_row = row
                    break

        # 3. Fallback to the first file if no is_best is set somehow
        if not truth_row:
            truth_row = group_rows[0]

        correct_date = truth_row["exif_date"]
        correct_make = truth_row["exif_make"]
        correct_model = truth_row["exif_model"]

        # Find all files in the group that have different EXIF data
        files_to_fix = []
        for row in group_rows:
            if (row["exif_date"], row["exif_make"], row["exif_model"]) != (
                correct_date,
                correct_make,
                correct_model,
            ):
                files_to_fix.append(row)

        if not files_to_fix:
            continue

        # Build exiftool command
        # Use -overwrite_original_in_place to preserve hardlinks!
        cmd = ["exiftool", "-overwrite_original_in_place", "-m"]

        if correct_date:
            # exif_date in DB is ISO8601, exiftool expects YYYY:MM:DD HH:MM:SS
            # e.g "2024-05-18T10:30:00" -> "2024:05:18 10:30:00"
            exif_time = correct_date.replace("-", ":").replace("T", " ")
            cmd.extend([f"-DateTimeOriginal={exif_time}", f"-CreateDate={exif_time}"])
        else:
            # If the correct file has NO date, we should strip it from the others so they match
            cmd.extend(["-DateTimeOriginal=", "-CreateDate="])

        if correct_make:
            cmd.append(f"-Make={correct_make}")
        else:
            cmd.append("-Make=")

        if correct_model:
            cmd.append(f"-Model={correct_model}")
        else:
            cmd.append("-Model=")

        files_paths = [Path(r["path"]) for r in files_to_fix]

        # We need to make sure the paths exist before asking exiftool to patch them
        valid_paths = [str(p) for p in files_paths if p.exists()]

        if not valid_paths:
            console.print(
                f"[yellow]Skipping group {truth_row['group_id']}: files to fix not found on disk.[/yellow]"
            )
            continue

        cmd.extend(valid_paths)

        # Execute Exiftool
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Update DB for these files to reflect the new truth
            for r in files_to_fix:
                db.conn.execute(
                    "UPDATE files SET exif_date=?, exif_make=?, exif_model=? WHERE id=?",
                    (correct_date, correct_make, correct_model, r["id"]),
                )

            updates += len(valid_paths)
        except subprocess.CalledProcessError as e:
            console.print(
                f"[red]Exiftool failed for group {truth_row['group_id']}: {e.stderr}[/red]"
            )
            errors += 1

        # Commit DB updates per group
        db.commit()

    console.print("\n[bold green]EXIF Fix Complete[/bold green]")
    console.print(f"Groups evaluated with mismatches: {total_mismatched_groups}")
    console.print(f"Files fixed on disk & BD:         {updates}")
    if errors > 0:
        console.print(f"Errors encountered:               {errors}")
