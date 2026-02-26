"""
Identifier for EXIF mismatches among duplicate groups.
Generates an MD and JSON report indicating which files in a group have divergent EXIF data.
"""

from __future__ import annotations

import collections
import json
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console

from .db import Database
from .config import Config

console = Console()


def check_exif_mismatches(db: Database, cfg: Config, output_dir: Path) -> None:
    """
    Query the database for all clustered duplicate groups, compare their EXIF data,
    and generate a JSON and Markdown report identifying any mismatches.
    """
    mismatches = []

    # Track statistics
    total_groups = 0
    mismatched_groups = 0

    console.print("Scanning duplicate groups for EXIF mismatches...")

    for group_rows in db.iter_clustered_groups():
        total_groups += 1

        # group_rows is a list of sqlite3.Row
        exif_signatures = collections.defaultdict(list)

        group_id = group_rows[0]["group_id"]

        for row in group_rows:
            # Create a signature from the relevant EXIF fields
            sig = (row["exif_date"], row["exif_make"], row["exif_model"])
            exif_signatures[sig].append(dict(row))

        if len(exif_signatures) > 1:
            mismatched_groups += 1

            # Format this mismatch
            mismatch_entry = {"group_id": group_id, "variants": []}

            for sig, files in exif_signatures.items():
                mismatch_entry["variants"].append(
                    {
                        "exif_date": sig[0],
                        "exif_make": sig[1],
                        "exif_model": sig[2],
                        "files": [
                            {
                                "id": f["id"],
                                "path": f["path"],
                                "source": f["source"],
                                "size": f["size"],
                                "mtime": f["mtime"],
                            }
                            for f in files
                        ],
                    }
                )

            mismatches.append(mismatch_entry)

    # Now generate the output
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%d_%H%M%S")
    report_dir = output_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    json_path = report_dir / f"exif_mismatches_{ts}.json"
    md_path = report_dir / f"exif_mismatches_{ts}.md"

    # ----------------------
    # Write JSON
    # ----------------------
    payload = {
        "generated_at": now.isoformat(),
        "total_groups_checked": total_groups,
        "mismatched_groups": mismatched_groups,
        "mismatches": mismatches,
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # ----------------------
    # Write Markdown
    # ----------------------
    lines = [
        "# EXIF Mismatch Report",
        "",
        f"> Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}  ",
        f"> Total Duplicate Groups Checked: {total_groups:,}  ",
        f"> Groups with EXIF Mismatches: {mismatched_groups:,}",
        "",
        "---",
        "",
    ]

    if mismatched_groups == 0:
        lines.append("No EXIF mismatches found in duplicate groups!")
        lines.append("")
    else:
        for m in mismatches:
            lines.append(f"## Group ID: {m['group_id']}")
            lines.append("")

            for i, variant in enumerate(m["variants"], 1):
                # Format variant header
                v_date = variant["exif_date"] or "NULL"
                v_make = variant["exif_make"] or "NULL"
                v_model = variant["exif_model"] or "NULL"

                lines.append(f"### Variant {i}")
                lines.append(f"- **Date:** `{v_date}`")
                lines.append(f"- **Make:** `{v_make}`")
                lines.append(f"- **Model:** `{v_model}`")
                lines.append("")
                lines.append("| ID | Source | Path | Size |")
                lines.append("|----|--------|------|------|")
                for f in variant["files"]:
                    lines.append(
                        f"| {f['id']} | `{f['source']}` | `{f['path']}` | {f['size']:,} |"
                    )
                lines.append("")

            lines.append("---")
            lines.append("")

    md_path.write_text("\n".join(lines), encoding="utf-8")

    console.print("\n[bold green]EXIF Check Complete![/bold green]")
    console.print(f"Total groups checked: {total_groups:,}")
    console.print(f"Mismatched groups:    {mismatched_groups:,}")
    if mismatched_groups > 0:
        console.print(f"\n[green]JSON report written to:[/green] {json_path}")
        console.print(f"[green]Markdown report written to:[/green] {md_path}")
