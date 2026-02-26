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

    for batch in db.iter_all_hashed_images():
        for row in batch:
            file_id = row["id"]
            fhash = row["file_hash"]
            all_ids.append(file_id)
            id_to_phash[file_id] = row["phash"]
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

                for i, (dists, nbrs) in enumerate(zip(distances, indices)):
                    src_id = valid_ids[i]
                    for dist, nbr_idx in zip(dists, nbrs):
                        if nbr_idx == i or nbr_idx < 0:
                            continue
                        if dist <= threshold:
                            nbr_id = valid_ids[nbr_idx]
                            if uf.find(src_id) != uf.find(nbr_id):
                                summary["near_groups"] += 1
                                uf.union(src_id, nbr_id)

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
