# image-consolidation

Ingest, deduplicate, and organize large photo/video libraries (tested at ~1 TB / 90 K+ files) into a clean, date-ordered hierarchy. All pipeline state lives in a local SQLite database. Runs are **incremental by default** — unchanged files are skipped automatically on re-runs.

```
Sources → Ingest → Hash → Deduplicate → Select best → Organize → Report
```

---

## Quickstart

**Prerequisites**

```bash
brew install ffmpeg exiftool          # macOS
# sudo apt install ffmpeg libimage-exiftool-perl  # Debian/Ubuntu
uv sync                               # Python deps (requires uv)
```

**First run**

```bash
# Dry-run to preview what will happen
uv run imgc run ~/Pictures /Volumes/Backup -o ~/Photos-Organized --dry-run

# Run for real
uv run imgc run ~/Pictures /Volumes/Backup -o ~/Photos-Organized
```

**Subsequent runs** (new files only — everything else is skipped)

```bash
uv run imgc run ~/Pictures /Volumes/Backup -o ~/Photos-Organized
```

**Output structure**

```
~/Photos-Organized/
├── 2022/06/   ← EXIF capture date
├── 2023/01/
├── 2024/
├── unsorted/  ← no recoverable date
└── reports/   ← Markdown + JSON report after each run
```

---

## Commands

| Command | Description |
|---------|-------------|
| `imgc run [SOURCES]` | Full pipeline end-to-end |
| `imgc ingest [SOURCES]` | Scan sources and extract metadata |
| `imgc backfill` | Re-extract metadata for existing DB entries without re-hashing |
| `imgc hash` | Compute SHA-256 + perceptual hashes |
| `imgc dedupe` | Cluster exact and near-duplicates |
| `imgc select` | Score and mark best version per group |
| `imgc organize` | Copy/move/link winners to output; prune demoted copies |
| `imgc report` | Generate Markdown + JSON report from current DB state |
| `imgc review-dupes` | Produce a per-group duplicate review report for spot-checking |
| `imgc status` | Show DB summary table |
| `imgc check-exif` | Identify EXIF mismatches within duplicate groups |
| `imgc fix-exif` | Propagate correct EXIF from best-version to duplicates |
| `imgc sync-metadata` | Push DB dates back to files that lack embedded EXIF |

Every command accepts `--help`.

---

## Key Options

```bash
# Config file (recommended for repeated use)
uv run imgc run --config config.toml

# Hard-link instead of copy — zero extra space, same filesystem only
uv run imgc run ~/Pictures -o ~/Organized --hardlink

# Move files instead of copying
uv run imgc run ~/Pictures -o ~/Organized --mode move

# Exact duplicates only (skip perceptual hashing — much faster)
uv run imgc run ~/Pictures -o ~/Organized --exact-only

# Tune near-duplicate sensitivity (default 5; lower = stricter)
uv run imgc run ~/Pictures -o ~/Organized --phash-threshold 3

# Prefer files from a specific source when picking the best version
uv run imgc run ~/Pictures /Volumes/Backup -o ~/Organized \
  --source-priority ~/Pictures=10 \
  --source-priority /Volumes/Backup=3

# YYYY/MM/DD subfolder structure instead of YYYY/MM
uv run imgc run ~/Pictures -o ~/Organized --structure YYYY/MM/DD

# Force full re-ingest (ignore incremental state)
uv run imgc run ~/Pictures -o ~/Organized --fresh

# Dry-run: plan without touching any files
uv run imgc run ~/Pictures -o ~/Organized --dry-run
```

**Duplicate review report**

```bash
# Top 50 groups by wasted space (default)
uv run imgc review-dupes -o ~/Photos-Organized

# More groups, sorted by least-confident selection (tied scores first)
uv run imgc review-dupes --limit 200 --sort suspicious

# Most copies per group first
uv run imgc review-dupes --limit 100 --sort count
```

---

## Config File

Copy `config.example.toml` and adjust for your setup:

```bash
cp config.example.toml config.toml
uv run imgc run --config config.toml
```

See [`config.example.toml`](config.example.toml) for all options with inline comments.

---

## How It Works

### Pipeline stages

| Stage | What happens |
|-------|-------------|
| **Ingest** | Walks source directories; extracts EXIF (Pillow → exifread fallback), video metadata via ffprobe, sidecar detection. Incremental: files with matching path + size + mtime are skipped. |
| **Backfill** | Re-extracts metadata for existing records where data is missing, without re-hashing. |
| **Hash** | SHA-256 for all files; 64-bit dHash (perceptual) for images. MPO files are excluded from perceptual hashing (dual-lens format produces false collisions). |
| **Deduplicate** | Two-pass clustering — see below. |
| **Select** | Scores every file; marks the highest-scoring file in each group as the winner. |
| **Organize** | Copies/moves/links winners into `YYYY/MM/` hierarchy. Prunes any output-directory copies that belong to demoted losers. |
| **Report** | Writes `reports/run_NNNN_TIMESTAMP.{md,json}` with per-stage stats, source breakdown, and top duplicate groups. |

### Deduplication (two-pass)

**Pass 1 — Exact:** Group files by SHA-256. Zero false positives. Applies to both images and videos.

**Pass 2 — Perceptual (images only):** All 64-bit dHashes are loaded into a FAISS binary flat index and queried for nearest neighbours within a configurable Hamming distance (default ≤ 5 bits). Three guards prevent false-positive merges before any union is recorded:

| Guard | Default | Purpose |
|-------|---------|---------|
| Date span | 30 days | Files whose EXIF dates are more than 30 days apart are not merged |
| Size ratio | 0.90 | Files where the smaller is less than 90% the size of the larger are not merged |
| MPO isolation | always on | MPO (dual-lens) files only match other MPO files |

After FAISS clustering, a **super-cluster validation** pass finds the medoid of each large group and expels any member whose hash distance to the medoid exceeds the threshold, catching chain-linked false positives.

### Best-version scoring

Within each duplicate group the file with the highest composite score is kept. All others are marked as losers and their output copies are pruned on the next organize run.

| Factor | Weight | Notes |
|--------|--------|-------|
| Resolution | 45% | `width × height`, capped at 50 MP |
| Date uniqueness | 20% | `1 ÷ (files in group sharing this date)` — heavily penalises batch-import copies all stamped with the same wrong date |
| Format quality | 20% | RAW = 1.0 · TIFF = 0.9 · PNG = 0.8 · HEIC = 0.72 · JPEG = 0.6 |
| EXIF completeness | 10% | Fraction of date / make / model present |
| Source priority | 5% | User-defined via `--source-priority` or config |

**Date uniqueness** is the key defence against a common Google Takeout / cloud-backup pattern where a large batch of photos is re-imported with today's date, creating many duplicates that all share the same wrong timestamp. Files with an organic, unique capture date automatically outscore them.

### Output directory ownership

The organize stage **owns** the output directory. On each run it:

1. Places newly-selected winners at their correct `YYYY/MM/filename` path.
2. Promotes files from `unsorted/` that have since gained a recoverable date.
3. **Prunes** output copies of files that were demoted since the last run (i.e. a better version was found). The original source file is checked first — if it no longer exists (move mode or externally deleted), the output copy is preserved as the sole surviving copy.

Empty directories left behind by pruning are removed automatically.

### Sidecar files

`.xmp`, `.dop`, `.pp3`, `.aae`, `.thm` sidecars are detected at ingest and moved alongside their master file during organize.

---

## Reports

Each run writes two files to `<output>/reports/`:

- **`run_NNNN_TIMESTAMP.md`** — human-readable Markdown with per-stage metrics, source breakdown, and the top 20 duplicate groups by wasted space
- **`run_NNNN_TIMESTAMP.json`** — machine-readable version of the same data (raw byte counts)

The **duplicate review report** (`imgc review-dupes`) writes a separate `dup_review_TIMESTAMP.md` listing every group with a table of all members, their EXIF dates, camera, dimensions, format, size, and score — useful for spot-checking selection decisions before deleting the source.

### Visual Duplicate Review Gallery

For a side-by-side visual comparison of winners vs duplicates:

```bash
# Generate HTML gallery (paginated, 50 groups per page)
uv run imgc gallery -o ~/Photos-Organized

# More groups per page, sorted by most copies first
uv run imgc gallery --per-page 100 --sort count

# Generate during full pipeline run
uv run imgc run ~/Pictures -o ~/Photos-Organized --gallery

# Limit gallery size for large libraries
uv run imgc run ~/Pictures -o ~/Photos-Organized --gallery --gallery-limit 200
```

The gallery creates a self-contained HTML file at `<output>/reports/dup_gallery_TIMESTAMP.html` with:

- **Paginated view** — Only loads 50 groups at a time for fast initial render
- **Side-by-side comparison** — Winner (green border) vs duplicates (red border)
- **Thumbnails with lightbox** — Click to view full image
- **Relative paths** — Works when copied/mounted on other systems
- **Metadata comparison** — Size, resolution, format, date, camera, score
- **Toggle options** — Hide thumbnails or duplicates to focus on winners only

---

## Performance

Tested at ~91 K files / ~780 GiB with 12 worker threads:

| Stage | Notes |
|-------|-------|
| Ingest + EXIF | 100–300 files/sec; subsequent incremental runs skip unchanged files in seconds |
| SHA-256 + dHash | 500–1 000 files/sec (I/O bound) |
| FAISS clustering | Milliseconds for 60 K image vectors |
| Organize (copy) | Disk I/O bound |

SQLite is tuned with WAL mode, 64 MiB page cache, and 256 MiB mmap.

---

## Supported Formats

**Images:** JPEG, PNG, TIFF, HEIC/HEIF, WebP, MPO, RAW (CR2, CR3, NEF, ARW, DNG, ORF, RW2, RAF, PEF, SRW, X3F)

**Videos:** MP4, MOV, AVI, MKV, M4V, 3GP, MTS, M2TS, WMV

**Sidecars:** XMP, DOP, PP3, AAE, THM

> HEIC support requires `pillow-heif`, installed automatically via `uv sync`. If you see a prerequisite warning at startup, run `uv sync` to ensure all optional dependencies are present.
