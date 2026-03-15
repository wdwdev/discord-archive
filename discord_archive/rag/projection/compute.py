"""Global UMAP projection pipeline for the Semantic Galaxy visualizer.

Reads all embeddings from LanceDB, reduces dimensionality with
GPU PCA (4096→50) streamed in batches, then runs UMAP (50→3)
to produce 3D coordinates.  Results are exported as per-guild
binary files.

Usage:
    uv run --extra rag --extra galaxy python -m discord_archive.rag.projection
"""

from __future__ import annotations

import logging
import struct
from pathlib import Path

import lancedb
import numpy as np
import torch
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

logger = logging.getLogger(__name__)
console = Console()

LANCEDB_DIR = "data/lancedb"
CHUNKS_TABLE = "chunks"
OUTPUT_DIR = Path("data/projections")

# Binary file format constants
PROJECTION_MAGIC = b"GLXY"
PROJECTION_VERSION = 1

# Pipeline parameters
PCA_DIM = 200
UMAP_COMPONENTS = 3
UMAP_N_NEIGHBORS = 30
UMAP_MIN_DIST = 0.2
UMAP_SUBSAMPLE = 1_000_000
GALAXY_SCALE = 100.0
READ_BATCH_SIZE = 50_000
RANDOM_SEED = 42

# Parametric UMAP (PyTorch MLP) parameters
PUMAP_HIDDEN = [256, 256, 128]
PUMAP_EPOCHS = 50
PUMAP_BATCH_SIZE = 65536
PUMAP_LR = 1e-3


def run() -> None:
    """Run the full projection pipeline."""
    console.rule("[bold blue]Semantic Galaxy Projection Pipeline")

    console.print("[bold]Step 1:[/bold] Connecting to LanceDB...")
    db = lancedb.connect(LANCEDB_DIR)
    table = db.open_table(CHUNKS_TABLE)
    total_rows = table.count_rows()
    console.print(f"  Found [green]{total_rows:,}[/green] vectors")

    console.print(f"\n[bold]Step 2:[/bold] GPU PCA 4096 → {PCA_DIM} (streaming)...")
    projected, chunk_ids, channel_ids, guild_ids = _gpu_pca(table, total_rows)
    console.print(f"  Output shape: {projected.shape}")

    console.print(f"\n[bold]Step 3:[/bold] UMAP {PCA_DIM} → {UMAP_COMPONENTS}...")
    positions_3d = _compute_umap(projected)
    console.print(f"  Output shape: {positions_3d.shape}")

    console.print("\n[bold]Step 4:[/bold] Normalizing positions...")
    positions_3d = _normalize_positions(positions_3d)

    console.print("\n[bold]Step 5:[/bold] Exporting per-guild binary files...")
    _export_per_guild(positions_3d, chunk_ids, channel_ids, guild_ids)

    console.rule("[bold green]Done!")


def _lance_batches(table: lancedb.table.Table) -> ...:
    """Yield batches from LanceDB as a generator."""
    lance_ds = table.to_lance()
    yield from lance_ds.to_batches(
        columns=["chunk_id", "vector", "channel_id", "guild_id"],
        batch_size=READ_BATCH_SIZE,
    )


def _gpu_pca(
    table: lancedb.table.Table,
    total_rows: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Three-pass GPU PCA: mean → covariance → project, streaming from LanceDB.

    Pass 1: Compute mean vector on GPU.
    Pass 2: Accumulate covariance matrix on GPU, then eigendecompose
             to get top PCA_DIM components.
    Pass 3: Project all vectors and collect metadata.

    GPU memory: ~64MB (4096×4096 cov) + batch (~800MB for 50k×4096).
    Peak RAM: ~1.1GB output array + metadata.
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    console.print(f"  Device: [cyan]{device}[/cyan]")
    d = 4096

    # Pass 1: compute mean
    console.print("  [bold]Pass 1/3:[/bold] Computing mean...")
    mean_acc = torch.zeros(d, dtype=torch.float64, device=device)
    count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Mean", total=total_rows)
        for batch in _lance_batches(table):
            n = batch.num_rows
            flat = np.array(batch.column("vector").values.to_numpy())
            vecs_gpu = torch.from_numpy(flat.reshape(n, d)).to(device)
            mean_acc += vecs_gpu.sum(dim=0).double()
            count += n
            progress.update(task, advance=n)

    mean = (mean_acc / count).float()  # (4096,)

    # Pass 2: covariance matrix + eigendecomposition
    console.print("  [bold]Pass 2/3:[/bold] Computing covariance...")
    cov = torch.zeros(d, d, dtype=torch.float64, device=device)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Covariance", total=total_rows)
        for batch in _lance_batches(table):
            n = batch.num_rows
            flat = np.array(batch.column("vector").values.to_numpy())
            centered = torch.from_numpy(flat.reshape(n, d)).to(device).float() - mean
            cov += (centered.T @ centered).double()
            progress.update(task, advance=n)

    cov /= count - 1

    console.print(f"  Eigendecomposition (top {PCA_DIM} components)...")
    eigenvalues, eigenvectors = torch.linalg.eigh(cov.float())
    # eigh returns ascending order — take last PCA_DIM and reverse
    components = eigenvectors[:, -PCA_DIM:].flip(dims=[1])  # (4096, PCA_DIM)
    top_eigenvalues = eigenvalues[-PCA_DIM:].flip(dims=[0])

    explained_ratio = (top_eigenvalues.sum() / eigenvalues.sum()).item()
    console.print(f"  Variance explained: [green]{explained_ratio:.1%}[/green]")

    del cov, eigenvalues, eigenvectors  # free GPU memory

    # Pass 3: project + collect metadata
    console.print("  [bold]Pass 3/3:[/bold] Projecting...")
    projected = np.empty((total_rows, PCA_DIM), dtype=np.float32)
    chunk_ids = np.empty(total_rows, dtype=np.int64)
    channel_ids = np.empty(total_rows, dtype=np.int64)
    guild_ids = np.empty(total_rows, dtype=np.int64)
    offset = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("PCA project", total=total_rows)
        for batch in _lance_batches(table):
            n = batch.num_rows
            flat = np.array(batch.column("vector").values.to_numpy())
            centered = torch.from_numpy(flat.reshape(n, d)).to(device).float() - mean
            proj = (centered @ components).cpu().numpy()

            projected[offset : offset + n] = proj
            chunk_ids[offset : offset + n] = batch.column("chunk_id").to_numpy()
            channel_ids[offset : offset + n] = batch.column("channel_id").to_numpy()
            guild_ids[offset : offset + n] = batch.column("guild_id").to_numpy()

            offset += n
            progress.update(task, advance=n)

    if offset < total_rows:
        projected = projected[:offset]
        chunk_ids = chunk_ids[:offset]
        channel_ids = channel_ids[:offset]
        guild_ids = guild_ids[:offset]

    return projected, chunk_ids, channel_ids, guild_ids


def _compute_umap(data: np.ndarray) -> np.ndarray:
    """Parametric UMAP: learn a neural network mapping from high-dim to 3D.

    1. Fit standard UMAP on a subsample to get the fuzzy simplicial set
       (kNN graph + edge weights).
    2. Train a PyTorch MLP on the subsample using UMAP's edge-wise
       cross-entropy loss (attractive + repulsive).
    3. Inference the MLP on ALL points (GPU batch, very fast).

    This gives much better results than umap.transform() because the
    MLP learns a smooth function that generalises, rather than the
    per-point approximate kNN lookup that transform() uses.
    """
    import torch.nn as nn
    from scipy.sparse import coo_matrix
    from umap import UMAP

    n = data.shape[0]
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    console.print(f"  Device: [cyan]{device}[/cyan]")

    # Step 1: Fit UMAP on subsample to get graph
    console.print(
        f"  [bold]Step 3a:[/bold] Fitting UMAP graph on "
        f"[cyan]{min(n, UMAP_SUBSAMPLE):,}[/cyan] subsample..."
    )
    rng = np.random.RandomState(RANDOM_SEED)
    if n > UMAP_SUBSAMPLE:
        sample_idx = rng.choice(n, UMAP_SUBSAMPLE, replace=False)
        sample_data = data[sample_idx]
    else:
        sample_idx = np.arange(n)
        sample_data = data

    umap = UMAP(
        n_components=UMAP_COMPONENTS,
        n_neighbors=UMAP_N_NEIGHBORS,
        min_dist=UMAP_MIN_DIST,
        n_jobs=-1,
        verbose=True,
    )
    embedding_init = umap.fit_transform(sample_data).astype(np.float32)
    console.print(f"  UMAP fit done, embedding shape: {embedding_init.shape}")

    # Extract fuzzy graph (symmetric, sparse)
    graph: coo_matrix = umap.graph_.tocoo()
    edge_from = graph.row.astype(np.int64)
    edge_to = graph.col.astype(np.int64)
    edge_weight = graph.data.astype(np.float32)

    # Filter to upper triangle to avoid duplicate edges
    mask = edge_from < edge_to
    edge_from = edge_from[mask]
    edge_to = edge_to[mask]
    edge_weight = edge_weight[mask]
    console.print(f"  Graph edges: [green]{len(edge_from):,}[/green]")

    # Step 2: Train MLP
    console.print(f"  [bold]Step 3b:[/bold] Training parametric model ({PUMAP_EPOCHS} epochs)...")

    class UMAPNet(nn.Module):
        def __init__(self, in_dim: int, out_dim: int, hidden: list[int]):
            super().__init__()
            layers: list[nn.Module] = []
            prev = in_dim
            for h in hidden:
                layers.extend([nn.Linear(prev, h), nn.BatchNorm1d(h), nn.GELU()])
                prev = h
            layers.append(nn.Linear(prev, out_dim))
            self.net = nn.Sequential(*layers)

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            return self.net(x)

    model = UMAPNet(sample_data.shape[1], UMAP_COMPONENTS, PUMAP_HIDDEN).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=PUMAP_LR)

    # Precompute UMAP repulsion parameter
    a, b = umap._a, umap._b  # fitted curve params for min_dist

    # Move data to GPU
    sample_tensor = torch.from_numpy(sample_data).to(device)
    edge_from_t = torch.from_numpy(edge_from).to(device)
    edge_to_t = torch.from_numpy(edge_to).to(device)
    edge_weight_t = torch.from_numpy(edge_weight).to(device)

    n_edges = len(edge_from)
    n_sample = len(sample_data)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Training", total=PUMAP_EPOCHS)
        for epoch in range(PUMAP_EPOCHS):
            model.train()

            # Forward pass on all sample points
            emb = model(sample_tensor)

            # Attractive loss: pull connected points together
            diff_pos = emb[edge_from_t] - emb[edge_to_t]
            dist_sq_pos = (diff_pos * diff_pos).sum(dim=1)
            # UMAP kernel: 1 / (1 + a * d^(2b))
            p_pos = 1.0 / (1.0 + a * dist_sq_pos.pow(b))
            attract_loss = -(edge_weight_t * torch.log(p_pos + 1e-6)).mean()

            # Repulsive loss: push random non-connected pairs apart
            neg_from = torch.randint(0, n_sample, (n_edges,), device=device)
            neg_to = torch.randint(0, n_sample, (n_edges,), device=device)
            diff_neg = emb[neg_from] - emb[neg_to]
            dist_sq_neg = (diff_neg * diff_neg).sum(dim=1)
            p_neg = 1.0 / (1.0 + a * dist_sq_neg.pow(b))
            repel_loss = -(torch.log(1.0 - p_neg + 1e-6)).mean()

            loss = attract_loss + repel_loss

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            progress.update(task, advance=1, description=f"Training (loss={loss.item():.4f})")

    console.print(f"  Final loss: [green]{loss.item():.4f}[/green]")

    # Step 3: Inference on ALL points
    console.print(f"  [bold]Step 3c:[/bold] Projecting all [cyan]{n:,}[/cyan] points...")
    model.eval()
    result = np.empty((n, UMAP_COMPONENTS), dtype=np.float32)
    infer_batch = 100_000

    with torch.no_grad(), Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Projecting", total=n)
        for i in range(0, n, infer_batch):
            end = min(i + infer_batch, n)
            batch = torch.from_numpy(data[i:end]).to(device)
            result[i:end] = model(batch).cpu().numpy()
            progress.update(task, advance=end - i)

    return result


def _normalize_positions(positions: np.ndarray) -> np.ndarray:
    """Center and scale positions to fit within GALAXY_SCALE."""
    center = positions.mean(axis=0)
    positions -= center
    max_dist = np.linalg.norm(positions, axis=1).max()
    if max_dist > 0:
        positions *= GALAXY_SCALE / max_dist
    return positions.astype(np.float32)


def _export_per_guild(
    positions: np.ndarray,
    chunk_ids: np.ndarray,
    channel_ids: np.ndarray,
    guild_ids: np.ndarray,
) -> None:
    """Export binary projection files, one per guild.

    Binary format (little-endian):
        Header (16 bytes):
            magic:      bytes[4]  = "GLXY"
            version:    uint32
            num_points: uint32
            reserved:   uint32    = 0
        Body:
            positions:   float32[num_points * 3]  (x, y, z interleaved)
            chunk_ids:   int64[num_points]
            channel_ids: int64[num_points]
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    unique_guilds = np.unique(guild_ids)
    console.print(f"  {len(unique_guilds)} guilds to export")

    for gid in unique_guilds:
        mask = guild_ids == gid
        n = int(mask.sum())

        filepath = OUTPUT_DIR / f"{gid}.bin"
        with open(filepath, "wb") as f:
            f.write(PROJECTION_MAGIC)
            f.write(struct.pack("<III", PROJECTION_VERSION, n, 0))
            f.write(positions[mask].tobytes())
            f.write(chunk_ids[mask].tobytes())
            f.write(channel_ids[mask].tobytes())

        size_mb = filepath.stat().st_size / (1024 * 1024)
        console.print(f"    {gid}: [green]{n:,}[/green] points, [cyan]{size_mb:.1f} MB[/cyan]")
