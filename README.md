# image-consolidation

Clean up and organize large photo/video libraries (~1 TB) into a deduplicated, date-ordered hierarchy.

```
Sources → Ingest → Hash → Deduplicate → Select best → Organize → Report
```

All pipeline state lives in a local SQLite database. Runs are **incremental by default** — unchanged files are skipped automatically on every subsequent run.

---

## Quickstart

```bash
# Install
git clone https://github.com/your/image-consolidation
cd image-consolidation
uv sync

# Full pipeline (dry-run first)
uv run imgc run ~/Pictures /Volumes/Backup --output ~/Photos-Organized --dry-run

# Run for real
uv run imgc run ~/Pictures /Volumes/Backup --output ~/Photos-Organized

# Re-run later (new files only — already processed files are skipped)
uv run imgc run ~/Pictures /Volumes/Backup --output ~/Photos-Organized
```

Output structure:
```
~/Photos-Organized/
├── 2023/
│   ├── 01/  ← EXIF date used when available
│   └── 06/
├── 2024/
└── unsorted/  ← files with no recoverable date
```

A Markdown + JSON report is written to `~/Photos-Organized/reports/` after each run.

---

## Installation

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11+.

```bash
uv sync          # install all dependencies
uv run imgc --help
```

---

## Commands

| Command | Description |
|---------|-------------|
| `imgc run [SOURCES]` | Full pipeline end-to-end |
| `imgc ingest [SOURCES]` | Scan sources, extract metadata |
| `imgc hash` | Compute SHA-256 + perceptual hashes |
| `imgc dedupe` | Cluster exact and near-duplicates |
| `imgc select` | Score and pick best version per group |
| `imgc organize` | Copy/move/link files to output |
| `imgc report` | Generate report from current DB state |
| `imgc status` | Show DB summary table |

Every command accepts `--help` for full option details.

---

## Key Options

```bash
# Use a config file
imgc run --config config.toml

# Hard-link instead of copy (zero extra disk space, same filesystem only)
imgc run ~/Pictures --output ~/Organized --hardlink

# Move files instead of copying
imgc run ~/Pictures --output ~/Organized --mode move

# Skip perceptual hashing (only catch bit-identical duplicates — much faster)
imgc run ~/Pictures --output ~/Organized --exact-only

# Tune near-duplicate sensitivity (default 8; lower = stricter)
imgc run ~/Pictures --output ~/Organized --phash-threshold 6

# Prefer files from a specific source when picking best version
imgc run ~/Pictures /Volumes/Backup --output ~/Organized \
  --source-priority ~/Pictures=10 \
  --source-priority /Volumes/Backup=3

# YYYY/MM/DD subfolder structure
imgc run ~/Pictures --output ~/Organized --structure YYYY/MM/DD

# Force full re-ingest (ignore incremental state)
imgc run ~/Pictures --output ~/Organized --fresh

# Preview without touching the filesystem
imgc run ~/Pictures --output ~/Organized --dry-run
```

---

## Config File

Copy `config.example.toml` and adjust:

```bash
cp config.example.toml config.toml
imgc run --config config.toml
```

See [`config.example.toml`](config.example.toml) for all available options with comments.

---

## How It Works

### Deduplication (two-pass)

1. **Exact** — SHA-256 hash groups. Zero false positives.
2. **Perceptual** — 64-bit dHash loaded into a FAISS binary index. Finds re-saves, crops, and minor edits within a configurable Hamming distance (default ≤ 8 bits).

### Best-version scoring

Within each duplicate group, files are scored and the highest wins:

| Factor | Weight | Details |
|--------|--------|---------|
| Resolution | 50% | width × height, capped at 50 MP |
| Format quality | 25% | RAW=1.0 › TIFF=0.9 › PNG=0.8 › HEIC=0.72 › JPEG=0.6 |
| EXIF completeness | 15% | has date + make + model |
| Source priority | 10% | user-defined via `--source-priority` |

### Sidecar files

`.xmp`, `.dop`, `.pp3`, `.aae`, `.thm` sidecars are detected automatically and moved alongside their master file.

---

## Performance

Tested on ~1 TB / ~500 K images with 12 worker threads:

| Stage | Throughput |
|-------|-----------|
| Ingest + EXIF | 100–300 files/sec |
| SHA-256 + dHash | 500–1 000 files/sec |
| FAISS clustering | thousands of queries/sec |
| Organize (copy) | disk I/O bound |

Subsequent incremental runs complete the ingest stage in seconds for unchanged files.

---

## Supported Formats

**Images:** JPEG, PNG, TIFF, HEIC/HEIF, WebP, RAW (CR2, CR3, NEF, ARW, DNG, ORF, RW2, RAF, PEF, SRW, X3F)

**Videos:** MP4, MOV, AVI, MKV, M4V, 3GP, MTS, M2TS, WMV

**Sidecars:** XMP, DOP, PP3, AAE, THM
