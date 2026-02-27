"""
Deduplicate stage — two-pass approach:

Pass 1 (exact): group files by SHA-256. O(n) using a dict.
Pass 2 (near):  load all pHashes into a FAISS BinaryFlat index,
                query each hash for neighbours within Hamming distance ≤ threshold,
                and assign shared group_ids via Union-Find.

Files that are unique (no duplicates found) get group_id = their own DB id,
so the selector stage can treat every file uniformly as "best in its group".
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date as _date

import numpy as np
from rich.console import Console
from rich.progress import track

from .config import Config
from .db import Database

console = Console()


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------


class UnionFind:
    def __init__(self) -> None:
        self._parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        self._parent.setdefault(x, x)
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])  # path compression
        return self._parent[x]

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra

    def isolate(self, x: int) -> None:
        """Remove x from its group so it becomes a singleton root."""
        self._parent[x] = x

    def groups(self, ids: list[int]) -> dict[int, list[int]]:
        """Return {root_id: [member_ids]}."""
        result: dict[int, list[int]] = defaultdict(list)
        for i in ids:
            result[self.find(i)].append(i)
        return dict(result)


# ---------------------------------------------------------------------------
# Hex hash → numpy uint8 array (8 bytes = 64 bits)
# ---------------------------------------------------------------------------


def _hex_to_vec(hex_str: str) -> np.ndarray:
    # imagehash outputs a 16-char hex for 8×8 dHash
    padded = hex_str.zfill(16)
    return np.frombuffer(bytes.fromhex(padded), dtype=np.uint8)


# ---------------------------------------------------------------------------
# Super-cluster post-validation
# ---------------------------------------------------------------------------


def _expel_outliers(
    uf: UnionFind,
    id_to_phash: dict[int, str | None],
    threshold: int,
    min_group_size: int = 3,
) -> int:
    """
    For each group with ≥ min_group_size members, find its medoid (the member
    with the most neighbours within threshold) and expel any member whose
    Hamming distance to the medoid exceeds the threshold.

    Expelled members are isolated back to singleton roots in the UnionFind so
    they receive no group_id and are treated as unique files by the selector.

    Returns the total number of members expelled.
    """
    try:
        import faiss  # type: ignore[import]
    except ImportError:
        return 0

    expelled_total = 0
    groups = uf.groups([i for i in id_to_phash if id_to_phash.get(i)])

    for root, members in groups.items():
        if len(members) < min_group_size:
            continue

        valid = [(m, id_to_phash[m]) for m in members if id_to_phash.get(m)]
        if len(valid) < min_group_size:
            continue

        ids = [v[0] for v in valid]
        vecs = np.array([_hex_to_vec(v[1]) for v in valid], dtype=np.uint8)  # type: ignore[arg-type]

        # Build a small FAISS index for this group
        idx = faiss.IndexBinaryFlat(64)
        idx.add(vecs)

        # Find the medoid: member with the most neighbours within threshold
        k = min(len(ids), 20)
        dists, _ = idx.search(vecs, k)
        neighbor_counts = (dists <= threshold).sum(axis=1)
        medoid_pos = int(np.argmax(neighbor_counts))
        medoid_vec = vecs[medoid_pos : medoid_pos + 1]

        # Compute each member's distance to the medoid via XOR popcount
        xor = vecs ^ medoid_vec  # broadcast: shape (n, 8)
        hamming = np.unpackbits(xor, axis=1).sum(axis=1)  # shape (n,)

        for i, mid in enumerate(ids):
            if int(hamming[i]) > threshold:
                uf.isolate(mid)
                expelled_total += 1

    return expelled_total


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_dedupe(db: Database, cfg: Config) -> dict:
    """
    Assign group_ids to duplicate/near-duplicate files.
    Returns a summary dict.
    """
    summary = {
        "exact_groups": 0,
        "near_groups": 0,
        "duplicate_files": 0,
    }

    uf = UnionFind()

    # ------------------------------------------------------------------
    # Pass 1 — exact hash grouping
    # ------------------------------------------------------------------
    console.print("[bold cyan]Pass 1:[/bold cyan] exact duplicate detection…")
    hash_to_ids: dict[str, list[int]] = defaultdict(list)

    # Collect all hashed files (batched to avoid huge memory usage)
    all_ids: list[int] = []
    id_to_phash: dict[int, str | None] = {}
    id_to_date: dict[int, str | None] = {}
    id_to_size: dict[int, int | None] = {}
    id_to_format: dict[int, str] = {}

    for batch in db.iter_all_hashed_images():
        for row in batch:
            file_id = row["id"]
            fhash = row["file_hash"]
            all_ids.append(file_id)
            id_to_phash[file_id] = row["phash"]
            id_to_date[file_id] = row["exif_date"]
            id_to_size[file_id] = row["size"]
            id_to_format[file_id] = (row["format"] or "").upper()
            if fhash:
                hash_to_ids[fhash].append(file_id)

    # Also collect hashed videos for exact (SHA-256) dedup
    console.print("[bold cyan]Pass 1b:[/bold cyan] collecting hashed videos…")
    for batch in db.iter_all_hashed_videos():
        for row in batch:
            file_id = row["id"]
            fhash = row["file_hash"]
            all_ids.append(file_id)
            if fhash:
                hash_to_ids[fhash].append(file_id)

    for fhash, ids in hash_to_ids.items():
        if len(ids) > 1:
            summary["exact_groups"] += 1
            summary["duplicate_files"] += len(ids) - 1
            for i in ids[1:]:
                uf.union(ids[0], i)

    # ------------------------------------------------------------------
    # Pass 2 — perceptual hash (FAISS BinaryFlat)
    # ------------------------------------------------------------------
    if not cfg.dedupe.exact_only:
        console.print(
            "[bold cyan]Pass 2:[/bold cyan] near-duplicate detection (FAISS)…"
        )

        # Build FAISS index from valid pHashes
        valid_ids = [i for i in all_ids if id_to_phash.get(i)]
        if valid_ids:
            try:
                import faiss  # type: ignore[import]

                vectors = np.vstack(
                    [
                        _hex_to_vec(id_to_phash[i])  # type: ignore[arg-type]
                        for i in valid_ids
                    ]
                ).astype(np.uint8)

                index = faiss.IndexBinaryFlat(64)  # 64-bit hashes
                index.add(vectors)

                # Search for k=10 nearest neighbours per vector
                k = min(10, len(valid_ids))
                threshold = cfg.dedupe.phash_threshold

                distances, indices = index.search(vectors, k)

                max_span = cfg.dedupe.max_date_span_days
                min_ratio = cfg.dedupe.min_size_ratio

                for i, (dists, nbrs) in enumerate(zip(distances, indices)):
                    src_id = valid_ids[i]
                    src_fmt = id_to_format.get(src_id, "")
                    src_date = id_to_date.get(src_id)
                    src_size = id_to_size.get(src_id)

                    for dist, nbr_idx in zip(dists, nbrs):
                        if nbr_idx == i or nbr_idx < 0:
                            continue
                        if dist > threshold:
                            continue

                        nbr_id = valid_ids[nbr_idx]

                        # MPO guard: MPO embeds two JPEG channels; its dHash
                        # can collide with unrelated single-lens photos.
                        nbr_fmt = id_to_format.get(nbr_id, "")
                        if (src_fmt == "MPO") != (nbr_fmt == "MPO"):
                            continue

                        # Date-span guard: photos taken more than N days apart
                        # are almost certainly not duplicates of each other.
                        if max_span > 0:
                            nbr_date = id_to_date.get(nbr_id)
                            if src_date and nbr_date:
                                try:
                                    d1 = _date.fromisoformat(src_date[:10])
                                    d2 = _date.fromisoformat(nbr_date[:10])
                                    if abs((d1 - d2).days) > max_span:
                                        continue
                                except ValueError:
                                    pass

                        # Size-ratio guard: a >10% size difference between two
                        # images at the same resolution suggests different content.
                        if min_ratio > 0.0:
                            nbr_size = id_to_size.get(nbr_id)
                            if src_size and nbr_size and src_size > 0 and nbr_size > 0:
                                if min(src_size, nbr_size) / max(src_size, nbr_size) < min_ratio:
                                    continue

                        uf.union(src_id, nbr_id)

                # Count distinct groups that contain ≥2 FAISS-matched members
                near_roots: set[int] = set()
                for vid in valid_ids:
                    root = uf.find(vid)
                    near_roots.add(root)
                # Subtract groups that were already formed by exact matching
                # (approximation: any multi-member group counts)
                summary["near_groups"] = sum(
                    1 for r in near_roots
                    if sum(1 for v in valid_ids if uf.find(v) == r) > 1
                )

                # Post-validation: expel chain-linked false positives
                expelled = _expel_outliers(uf, id_to_phash, threshold)
                if expelled:
                    console.print(
                        f"[yellow]Super-cluster validation: expelled {expelled:,} "
                        f"false-positive members from oversized groups.[/yellow]"
                    )

            except ImportError:
                console.print(
                    "[yellow]faiss-cpu not installed — skipping perceptual dedup.[/yellow]\n"
                    "Install with: uv add faiss-cpu"
                )

    # ------------------------------------------------------------------
    # Write group assignments back to DB
    # ------------------------------------------------------------------
    console.print("Writing group assignments…")
    groups = uf.groups(all_ids)

    updates: list[tuple[int | None, int]] = []
    for root, members in track(groups.items(), description="Saving groups…"):
        is_dup_group = len(members) > 1
        for mid in members:
            # Singletons get a group_id only if they're actually in a dup group
            gid = root if is_dup_group else None
            updates.append((gid, mid))
        if len(updates) >= 2000:
            db.update_group_batch(updates)
            updates.clear()

    if updates:
        db.update_group_batch(updates)

    # Files that stayed status='hashed' (no group) still need to be marked clustered
    db.conn.execute("UPDATE files SET status='clustered' WHERE status='hashed'")
    db.commit()

    return summary
