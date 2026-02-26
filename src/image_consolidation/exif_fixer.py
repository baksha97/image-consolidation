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
                # Find earliest non-null date
                if row["exif_date"]:
                    if not earliest_date or row["exif_date"] < earliest_date:
                        earliest_date = row["exif_date"]
                        
                # Keep accumulating make/model if missing
                if row["exif_make"] and not correct_make:
                    correct_make = row["exif_make"]
                if row["exif_model"] and not correct_model:
                    correct_model = row["exif_model"]
                    
            correct_date = earliest_date

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
        
        # Also include the organized versions in the destination folder if they exist
        # We know one of the files in this group is the "best" and might have been organized
        for index, row in enumerate(files_to_fix):
            # We don't have organized_path in the standard query, we might need to check if 
            # it was transferred (though it's usually hardlinked). We'll trust the selector 
            # hardlinked/copied it. If it was copied, it needs an update. 
            pass

        # To be safe, let's query the DB for the organized path of the `is_best` file in this group
        res = db.conn.execute("SELECT organized_path FROM files WHERE group_id=? AND is_best=1", (group_rows[0]["group_id"],)).fetchone()
        if res and res["organized_path"]:
            org_p = Path(res["organized_path"])
            if org_p not in files_paths:
                files_paths.append(org_p)

        # We need to make sure the paths exist before asking exiftool to patch them
        valid_paths = [str(p) for p in files_paths if p.exists()]

        if not valid_paths:
            console.print(
                f"[yellow]Skipping group {group_rows[0]['group_id']}: files to fix not found on disk.[/yellow]"
            )
            continue

        cmd.extend(valid_paths)

        if dry_run:
            console.print(f"[dim]\[DRY RUN] Group {group_rows[0]['group_id']} -> Date: {correct_date}, Make: {correct_make}, Model: {correct_model}[/dim]")
            for p in valid_paths:
                 console.print(f"  [dim]~ {p}[/dim]")
            continue

        # Execute Exiftool
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Update DB for these files to reflect the new truth
            for r in files_to_fix:
                db.conn.execute(
                    "UPDATE files SET exif_date=?, exif_make=?, exif_model=? WHERE id=?",
                    (correct_date, correct_make, correct_model, r["id"]),
                )

            updates += len(valid_paths)
        except subprocess.CalledProcessError as e:
            console.print(
                f"[red]Exiftool failed for group {group_rows[0]['group_id']}: {e.stderr}[/red]"
            )
            errors += 1

        # Commit DB updates per group
        db.commit()

    if dry_run:
        console.print("\n[bold yellow]DRY RUN Complete - No changes made[/bold yellow]")
    else:
        console.print("\n[bold green]EXIF Fix Complete[/bold green]")
        
    console.print(f"Groups evaluated with mismatches: {total_mismatched_groups}")
    console.print(f"Files evaluated for fix:          {updates if not dry_run else '0 (Dry run)'}")
    if errors > 0:
        console.print(f"Errors encountered:               {errors}")
