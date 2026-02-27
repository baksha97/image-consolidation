"""
Reporter — generates an incremental Markdown + JSON report after each run.

"Incremental" means the report shows:
  • What happened *this* run (new files, new dupes found, files organized)
  • Cumulative totals from the full database state

Report written to: <output_dir>/reports/run_<run_id>_<timestamp>.{md,json}
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console

from .db import Database

console = Console()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n //= 1024
    return f"{n:.1f} PiB"


def _pct(part: int, total: int) -> str:
    if total == 0:
        return "—"
    return f"{part / total * 100:.1f}%"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_report(
    db: Database,
    run_id: int,
    run_summary: dict,
    output_dir: Path,
    run_started: datetime,
) -> Path:
    """
    Write a Markdown report and a companion JSON file.

    run_summary keys (all optional, fill in what you have):
        ingest   → {"scanned", "new", "skipped", "errors"}
        hash     → {"hashed", "errors"}
        dedupe   → {"exact_groups", "near_groups", "duplicate_files"}
        select   → {"groups_scored", "singletons"}
        organize → {"organized", "unsorted", "errors", "bytes_transferred"}

    Returns the Path to the markdown report.
    """
    now = datetime.now(timezone.utc)
    run_started_aware = run_started if run_started.tzinfo else run_started.replace(tzinfo=timezone.utc)
    elapsed = now - run_started_aware

    stats = db.stats()
    sources = db.source_breakdown()
    top_groups = db.top_duplicate_groups()

    report_dir = output_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    ts = now.strftime("%Y%m%d_%H%M%S")
    md_path = report_dir / f"run_{run_id:04d}_{ts}.md"
    json_path = report_dir / f"run_{run_id:04d}_{ts}.json"

    ingest_s  = run_summary.get("ingest",   {})
    hash_s    = run_summary.get("hash",     {})
    dedupe_s  = run_summary.get("dedupe",   {})
    select_s  = run_summary.get("select",   {})
    org_s     = run_summary.get("organize", {})

    # ------------------------------------------------------------------
    # Markdown
    # ------------------------------------------------------------------
    lines: list[str] = [
        f"# Image Consolidation — Run #{run_id}",
        "",
        f"> Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}  ",
        f"> Elapsed:   {str(elapsed).split('.')[0]}",
        "",
        "---",
        "",
        "## This Run",
        "",
        "| Stage    | Metric                    | Value |",
        "|----------|---------------------------|-------|",
    ]

    if ingest_s:
        lines += [
            f"| Ingest   | Files scanned             | {ingest_s.get('scanned', '—'):,} |",
            f"| Ingest   | New / updated             | {ingest_s.get('new', '—'):,} |",
            f"| Ingest   | Skipped (unchanged)       | {ingest_s.get('skipped', '—'):,} |",
            f"| Ingest   | Errors                    | {ingest_s.get('errors', '—'):,} |",
        ]
    if hash_s:
        lines += [
            f"| Hash     | Files hashed              | {hash_s.get('hashed', '—'):,} |",
            f"| Hash     | Errors                    | {hash_s.get('errors', '—'):,} |",
        ]
    if dedupe_s:
        lines += [
            f"| Dedupe   | Exact-dup groups found    | {dedupe_s.get('exact_groups', '—'):,} |",
            f"| Dedupe   | Near-dup groups found     | {dedupe_s.get('near_groups', '—'):,} |",
            f"| Dedupe   | Duplicate files identified| {dedupe_s.get('duplicate_files', '—'):,} |",
        ]
    if select_s:
        lines += [
            f"| Select   | Groups scored             | {select_s.get('groups_scored', '—'):,} |",
            f"| Select   | Singleton (unique) files  | {select_s.get('singletons', '—'):,} |",
        ]
    if org_s:
        lines += [
            f"| Organize | Files organized           | {org_s.get('organized', '—'):,} |",
            f"| Organize | Files → unsorted/         | {org_s.get('unsorted', '—'):,} |",
            f"| Organize | Bytes transferred         | {_fmt_bytes(org_s.get('bytes_transferred', 0))} |",
            f"| Organize | Errors                    | {org_s.get('errors', '—'):,} |",
        ]

    lines += [
        "",
        "---",
        "",
        "## Cumulative Database State",
        "",
        f"| Metric                       | Value |",
        f"|------------------------------|-------|",
        f"| Total files                  | {stats['total']:,} |",
        f"| — Images                     | {stats['images']:,} |",
        f"| — Videos                     | {stats['videos']:,} |",
        f"| Duplicate groups             | {stats['duplicate_groups']:,} |",
        f"| Duplicate files (losers)     | {stats['duplicate_files']:,} |",
        f"| Space recoverable            | {_fmt_bytes(stats['duplicate_bytes'])} |",
        f"| Files organized              | {stats['organized']:,} |",
        f"| Files in unsorted/           | {stats['unsorted']:,} |",
        "",
    ]

    # Per-source table
    if sources:
        lines += [
            "## Source Breakdown",
            "",
            "| Source | Total | Kept (best) | Duplicates |",
            "|--------|-------|-------------|------------|",
        ]
        for row in sources:
            lines.append(
                f"| `{row['source']}` | {row['total']:,} | {row['kept']:,} | {row['dupes']:,} |"
            )
        lines.append("")

        lines += [
            "## Source Uniqueness",
            "",
            "> **Exclusive** — no counterpart found in any other source (true unique content).  ",
            "> **Won** — exists in multiple sources; this source's copy scored highest.  ",
            "> **Lost** — exists in multiple sources; another source's copy was preferred.",
            "",
            "| Source | Exclusive | Won vs. counterpart | Lost to counterpart |",
            "|--------|-----------|---------------------|---------------------|",
        ]
        for row in sources:
            lines.append(
                f"| `{row['source']}` "
                f"| {row['exclusive']:,} "
                f"| {row['won_vs_counterpart']:,} "
                f"| {row['dupes']:,} |"
            )
        lines.append("")

    # Top duplicate groups
    if top_groups:
        lines += [
            "## Top Duplicate Groups (by wasted space)",
            "",
            "| Group ID | Files | Max resolution (px) | Total size |",
            "|----------|-------|---------------------|------------|",
        ]
        for row in top_groups:
            max_px = f"{row['max_px']:,}" if row['max_px'] is not None else "—"
            lines.append(
                f"| {row['group_id']} | {row['count']} "
                f"| {max_px} | {_fmt_bytes(row['total_bytes'])} |"
            )
        lines.append("")

    lines += [
        "---",
        "_Report generated by [image-consolidation](https://github.com/your/repo)_",
        "",
    ]

    md_path.write_text("\n".join(lines), encoding="utf-8")

    # ------------------------------------------------------------------
    # JSON
    # ------------------------------------------------------------------
    payload = {
        "run_id": run_id,
        "generated_at": now.isoformat(),
        "elapsed_seconds": elapsed.total_seconds(),
        "this_run": run_summary,
        "cumulative": stats,
        "sources": [dict(r) for r in sources],  # includes exclusive, won_vs_counterpart
        "top_duplicate_groups": [dict(r) for r in top_groups],
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    console.print(f"\n[green]Report written to:[/green] {md_path}")
    console.print(f"[green]JSON data:[/green]          {json_path}")

    return md_path
