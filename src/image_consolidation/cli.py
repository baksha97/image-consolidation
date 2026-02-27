"""
imgc — image-consolidation CLI (Typer)

Commands:
  imgc run       Full pipeline (ingest → hash → dedupe → select → organize → report)
  imgc ingest    Scan sources and populate DB
  imgc hash      Compute SHA-256 + perceptual hashes
  imgc dedupe    Cluster duplicates
  imgc select    Score and mark best versions
  imgc organize  Copy/move/link files to output hierarchy
  imgc report    Generate a report from current DB state
  imgc status    Show DB summary

All commands default to incremental mode — already-processed files are skipped.
Use --fresh to force a full rerun of the ingest stage.
"""

from __future__ import annotations

import sys
import shutil
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table
from PIL import Image

from .config import Config
from .db import Database
from .ingest import run_ingest, run_backfill
from .hasher import run_hash
from .deduplicator import run_dedupe
from .selector import run_select
from .organizer import run_organize
from .reporter import generate_report, generate_dup_review
from .exif_checker import check_exif_mismatches
from .exif_fixer import fix_exif_mismatches, sync_metadata_to_disk

console = Console()

app = typer.Typer(
    name="imgc",
    help="Ingest, deduplicate, and organize your photo/video library at scale.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)


def _check_prerequisites(cfg: Config) -> None:
    """Fail fast if required external tools or library features are missing."""
    missing = []

    # 1. ffprobe check
    if cfg.formats.include_videos and not shutil.which("ffprobe"):
        missing.append(
            "ffprobe is not in your PATH. It is required for extracting video metadata.\n"
            "   [bold]Fix:[/bold] Install ffmpeg (e.g., 'brew install ffmpeg' or 'sudo apt install ffmpeg')."
        )

    # 2. exiftool check
    if not shutil.which("exiftool"):
        missing.append(
            "exiftool is not in your PATH. It is required for writing metadata back to files.\n"
            "   [bold]Fix:[/bold] Install exiftool (e.g., 'brew install exiftool' or 'sudo apt install libimage-exiftool-perl')."
        )

    # 3. Pillow HEIC support check
    heic_extensions = {".heic", ".heif"}
    if any(ext in cfg.formats.image_extensions for ext in heic_extensions):
        supported = Image.registered_extensions()
        has_heic = ".heic" in supported or ".heif" in supported
        
        # Also try to import pillow_heif to be sure
        try:
            import pillow_heif
            has_heic = True
        except ImportError:
            pass

        if not has_heic:
            missing.append(
                "Pillow does not have HEIC support enabled. 'pillow-heif' is required.\n"
                "   [bold]Fix:[/bold] Run 'uv sync' or 'pip install pillow-heif' to install the dependency."
            )

    if missing:
        console.rule("[bold red]Prerequisite Check Failed[/bold red]", style="red")
        for msg in missing:
            console.print(f"• {msg}\n")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# Enums for constrained options
# ---------------------------------------------------------------------------


class OrganizeMode(str, Enum):
    copy = "copy"
    move = "move"
    hardlink = "hardlink"


class ReviewSort(str, Enum):
    size       = "size"
    count      = "count"
    suspicious = "suspicious"


class FolderStructure(str, Enum):
    year_month = "YYYY/MM"
    year_month_day = "YYYY/MM/DD"


# ---------------------------------------------------------------------------
# Shared config builder
# ---------------------------------------------------------------------------


def _load_config(
    config_path: Path | None,
    db_path: Path,
    sources: list[Path] | None = None,
    output: Path | None = None,
    workers: int | None = None,
    mode: OrganizeMode | None = None,
    hardlink: bool = False,
    phash_threshold: int | None = None,
    exact_only: bool = False,
    no_videos: bool = False,
    structure: FolderStructure | None = None,
    source_priority: list[str] | None = None,
) -> Config:
    if config_path:
        cfg = Config.from_toml(config_path)
    elif sources:
        cfg = Config.default_with_sources(
            [str(s) for s in sources], str(output or "output")
        )
    else:
        cfg = Config()

    cfg.db_path = db_path.expanduser().resolve()

    if workers is not None:
        cfg.performance.workers = workers
    if output is not None:
        cfg.output.directory = output.expanduser().resolve()
    if hardlink:
        cfg.output.mode = "hardlink"  # type: ignore[assignment]
    elif mode is not None:
        cfg.output.mode = mode.value  # type: ignore[assignment]
    if phash_threshold is not None:
        cfg.dedupe.phash_threshold = phash_threshold
    if exact_only:
        cfg.dedupe.exact_only = True
    if no_videos:
        cfg.formats.include_videos = False
    if structure is not None:
        cfg.output.structure = structure.value  # type: ignore[assignment]

    for sp in source_priority or []:
        if "=" in sp:
            prefix, _, score_str = sp.partition("=")
            try:
                cfg.sources.priorities[prefix.strip()] = int(score_str.strip())
            except ValueError:
                pass

    return cfg


# ---------------------------------------------------------------------------
# Shared option defaults (Annotated aliases)
# ---------------------------------------------------------------------------

DbOpt = Annotated[
    Path,
    typer.Option("--db", help="Path to the SQLite state database.", show_default=True),
]
ConfigOpt = Annotated[
    Optional[Path],
    typer.Option(
        "--config", exists=True, help="TOML config file (overrides CLI flags)."
    ),
]
WorkersOpt = Annotated[
    Optional[int],
    typer.Option("--workers", help="Parallel worker threads (overrides config)."),
]
DryRunOpt = Annotated[
    bool,
    typer.Option("--dry-run", help="Preview changes without touching the filesystem."),
]
FreshOpt = Annotated[
    bool, typer.Option("--fresh", help="Re-ingest all files, ignoring previous state.")
]
SourcePriOpt = Annotated[
    Optional[list[str]],
    typer.Option(
        "--source-priority",
        metavar="PATH=SCORE",
        help="Assign priority to a source, e.g. /Volumes/Lightroom=10 (repeatable).",
    ),
]


# ---------------------------------------------------------------------------
# run — full pipeline
# ---------------------------------------------------------------------------


@app.command()
def run(
    sources: Annotated[
        Optional[list[Path]], typer.Argument(help="Source directories to scan.")
    ] = None,
    output: Annotated[
        Optional[Path], typer.Option("-o", "--output", help="Output directory.")
    ] = None,
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
    workers: WorkersOpt = None,
    dry_run: DryRunOpt = False,
    mode: Annotated[
        Optional[OrganizeMode], typer.Option(help="File transfer mode.")
    ] = None,
    hardlink: Annotated[
        bool, typer.Option("--hardlink", help="Shorthand for --mode hardlink.")
    ] = False,
    phash_threshold: Annotated[
        Optional[int], typer.Option(help="Hamming distance threshold for near-dupes.")
    ] = None,
    exact_only: Annotated[
        bool,
        typer.Option(
            "--exact-only", help="Skip perceptual hashing (exact dupes only)."
        ),
    ] = False,
    no_videos: Annotated[
        bool, typer.Option("--no-videos", help="Ignore video files.")
    ] = False,
    structure: Annotated[
        Optional[FolderStructure], typer.Option(help="Output folder structure.")
    ] = None,
    source_priority: SourcePriOpt = None,
    skip_report: Annotated[
        bool, typer.Option("--skip-report", help="Don't generate a report at the end.")
    ] = False,
) -> None:
    """[bold]Run the full pipeline[/bold]: ingest → hash → dedupe → select → organize → report."""
    cfg = _load_config(
        config_path=config,
        db_path=db,
        sources=sources,
        output=output,
        workers=workers,
        mode=mode,
        hardlink=hardlink,
        phash_threshold=phash_threshold,
        exact_only=exact_only,
        no_videos=no_videos,
        structure=structure,
        source_priority=source_priority,
    )

    if not cfg.sources.paths:
        console.print(
            "[red]Error:[/red] No source directories specified. Pass paths as arguments or use --config."
        )
        raise typer.Exit(code=1)

    _run_pipeline(cfg, dry_run=dry_run, skip_report=skip_report)


def _run_pipeline(cfg: Config, dry_run: bool, skip_report: bool) -> None:
    _check_prerequisites(cfg)
    run_started = datetime.utcnow()
    run_summary: dict = {}

    with Database(cfg.db_path) as db:
        run_id = db.start_run(cfg.model_dump_json())
        try:
            console.rule("[bold blue]Stage 1/5 — Ingest[/bold blue]")
            s = run_ingest(db, cfg, incremental=True)
            run_summary["ingest"] = s
            _print_summary(s)

            console.rule("[bold blue]Stage 1.5 — Backfill Metadata[/bold blue]")
            s = run_backfill(db, cfg)
            run_summary["backfill"] = s
            _print_summary(s)

            console.rule("[bold blue]Stage 2/5 — Hash[/bold blue]")
            s = run_hash(db, cfg)
            run_summary["hash"] = s
            _print_summary(s)

            console.rule("[bold blue]Stage 3/5 — Deduplicate[/bold blue]")
            s = run_dedupe(db, cfg)
            run_summary["dedupe"] = s
            _print_summary(s)

            console.rule("[bold blue]Stage 4/5 — Select best[/bold blue]")
            s = run_select(db, cfg)
            run_summary["select"] = s
            _print_summary(s)

            console.rule("[bold blue]Stage 5/5 — Organize[/bold blue]")
            if dry_run:
                console.print(
                    "[yellow]DRY RUN — no files will be moved/copied.[/yellow]"
                )
            s = run_organize(db, cfg, dry_run=dry_run)
            run_summary["organize"] = s
            _print_summary(s)

            if not skip_report:
                console.rule("[bold blue]Report[/bold blue]")
                generate_report(
                    db=db,
                    run_id=run_id,
                    run_summary=run_summary,
                    output_dir=cfg.output.directory,
                    run_started=run_started,
                )

            db.finish_run(run_id, "completed")

        except KeyboardInterrupt:
            db.finish_run(run_id, "failed")
            console.print("\n[yellow]Interrupted — run marked as failed.[/yellow]")
            raise
        except Exception as e:
            db.finish_run(run_id, "failed")
            console.print(f"[red]Pipeline failed: {e}[/red]")
            raise


def _print_summary(summary: dict) -> None:
    t = Table(show_header=False, box=None, padding=(0, 2))
    t.add_column("key", style="dim")
    t.add_column("value", style="bold")
    for k, v in summary.items():
        t.add_row(k, str(v))
    console.print(t)


# ---------------------------------------------------------------------------
# Individual stage commands
# ---------------------------------------------------------------------------


@app.command()
def ingest(
    sources: Annotated[
        Optional[list[Path]], typer.Argument(help="Source directories to scan.")
    ] = None,
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
    workers: WorkersOpt = None,
    no_videos: Annotated[bool, typer.Option("--no-videos")] = False,
    source_priority: SourcePriOpt = None,
    fresh: FreshOpt = False,
) -> None:
    """Scan source directories and populate the database."""
    cfg = _load_config(
        config,
        db,
        sources=sources,
        workers=workers,
        no_videos=no_videos,
        source_priority=source_priority,
    )
    with Database(cfg.db_path) as db_conn:
        _print_summary(run_ingest(db_conn, cfg, incremental=not fresh))


@app.command()
def backfill(
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
) -> None:
    """Re-extract metadata (dates, durations) for existing DB entries without re-hashing."""
    cfg = _load_config(config, db)
    with Database(cfg.db_path) as db_conn:
        _print_summary(run_backfill(db_conn, cfg))


@app.command(name="hash")
def hash_cmd(
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
    workers: WorkersOpt = None,
) -> None:
    """Compute SHA-256 + perceptual hashes for ingested files."""
    cfg = _load_config(config, db, workers=workers)
    with Database(cfg.db_path) as db_conn:
        _print_summary(run_hash(db_conn, cfg))


@app.command()
def dedupe(
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
    phash_threshold: Annotated[
        Optional[int], typer.Option(help="Hamming distance threshold.")
    ] = None,
    exact_only: Annotated[bool, typer.Option("--exact-only")] = False,
) -> None:
    """Cluster duplicates using exact and perceptual hashing."""
    cfg = _load_config(
        config, db, phash_threshold=phash_threshold, exact_only=exact_only
    )
    with Database(cfg.db_path) as db_conn:
        _print_summary(run_dedupe(db_conn, cfg))


@app.command()
def select(
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
    source_priority: SourcePriOpt = None,
) -> None:
    """Score files and mark the best version in each duplicate group."""
    cfg = _load_config(config, db, source_priority=source_priority)
    with Database(cfg.db_path) as db_conn:
        _print_summary(run_select(db_conn, cfg))


@app.command()
def organize(
    output: Annotated[Optional[Path], typer.Option("-o", "--output")] = None,
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
    dry_run: DryRunOpt = False,
    mode: Annotated[Optional[OrganizeMode], typer.Option()] = None,
    hardlink: Annotated[bool, typer.Option("--hardlink")] = False,
    structure: Annotated[Optional[FolderStructure], typer.Option()] = None,
) -> None:
    """Copy/move/link selected files to the output directory."""
    cfg = _load_config(
        config, db, output=output, mode=mode, hardlink=hardlink, structure=structure
    )
    with Database(cfg.db_path) as db_conn:
        _print_summary(run_organize(db_conn, cfg, dry_run=dry_run))


@app.command()
def report(
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Output directory (for report placement)."),
    ] = None,
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
) -> None:
    """Generate a Markdown + JSON report from current DB state."""
    cfg = _load_config(config, db, output=output)
    with Database(cfg.db_path) as db_conn:
        last = db_conn.last_run()
        run_id = last["id"] if last else 0
        generate_report(
            db=db_conn,
            run_id=run_id,
            run_summary={},
            output_dir=cfg.output.directory,
            run_started=datetime.utcnow(),
        )


@app.command(name="review-dupes")
def review_dupes(
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Output directory (for report placement)."),
    ] = None,
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
    limit: Annotated[
        int,
        typer.Option("--limit", help="Max number of duplicate groups to include.", min=1),
    ] = 50,
    sort: Annotated[
        ReviewSort,
        typer.Option("--sort", help="'size' = most wasted space first (default); 'count' = most copies first; 'suspicious' = closest scores first (least confident choices)."),
    ] = ReviewSort.size,
) -> None:
    """Generate a Markdown report listing each duplicate group with EXIF data for spot-checking."""
    cfg = _load_config(config, db, output=output)
    with Database(cfg.db_path) as db_conn:
        generate_dup_review(
            db=db_conn,
            output_dir=cfg.output.directory,
            limit=limit,
            sort_by=sort.value,
        )


@app.command(name="check-exif")
def check_exif(
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
) -> None:
    """Identify EXIF mismatches among duplicate groups and generate a report."""
    cfg = _load_config(config, db)
    with Database(cfg.db_path) as db_conn:
        check_exif_mismatches(db_conn, cfg, cfg.output.directory)


@app.command(name="fix-exif")
def fix_exif(
    trust_source: Annotated[
        Optional[str],
        typer.Option(
            help="Prioritize this source path prefix for the correct EXIF data."
        ),
    ] = None,
    dry_run: DryRunOpt = False,
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
) -> None:
    """Fix EXIF mismatches among duplicate groups by copying metadata from the best source."""
    cfg = _load_config(config, db)
    with Database(cfg.db_path) as db_conn:
        fix_exif_mismatches(db_conn, cfg, trust_source, dry_run)


@app.command(name="sync-metadata")
def sync_metadata(
    dry_run: DryRunOpt = False,
    config: ConfigOpt = None,
    db: DbOpt = Path("imgc.db"),
) -> None:
    """Push DB metadata (exif_date) back to files that lack it (e.g. filename dates)."""
    cfg = _load_config(config, db)
    with Database(cfg.db_path) as db_conn:
        _print_summary(sync_metadata_to_disk(db_conn, cfg, dry_run))


@app.command()
def status(
    db: DbOpt = Path("imgc.db"),
) -> None:
    """Show a summary of the current database state."""
    db_file = db.expanduser().resolve()
    if not db_file.exists():
        console.print(f"[red]Database not found:[/red] {db_file}")
        raise typer.Exit(code=1)

    with Database(db_file) as db_conn:
        stats = db_conn.stats()
        sources = db_conn.source_breakdown()

        t = Table(title="Database State")
        t.add_column("Metric")
        t.add_column("Value", justify="right")
        for k, v in stats.items():
            t.add_row(
                k.replace("_", " ").title(), f"{v:,}" if isinstance(v, int) else str(v)
            )
        console.print(t)

        if sources:
            st = Table(title="Sources")
            st.add_column("Source")
            st.add_column("Total", justify="right")
            st.add_column("Kept", justify="right")
            st.add_column("Dupes", justify="right")
            for row in sources:
                st.add_row(
                    row["source"],
                    f"{row['total']:,}",
                    f"{row['kept']:,}",
                    f"{row['dupes']:,}",
                )
            console.print(st)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    app()


if __name__ == "__main__":
    main()
