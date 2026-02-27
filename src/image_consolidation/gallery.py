"""
Gallery generator — creates an HTML file for visual review of duplicate groups.

Features:
  • Side-by-side comparison of winner vs duplicates
  • Pagination (50 groups per page by default)
  • Relative paths for portability across systems
  • Lazy-loaded thumbnails
  • Metadata comparison (size, resolution, EXIF)
"""

from __future__ import annotations

import base64
import html
import json
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from rich.console import Console
from PIL import Image

from .db import Database

console = Console()


# ---------------------------------------------------------------------------
# HTML Template
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Duplicate Review Gallery</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            color: #333;
            line-height: 1.6;
        }}
        .header {{
            background: #2c3e50;
            color: white;
            padding: 1rem 2rem;
            position: sticky;
            top: 0;
            z-index: 100;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header h1 {{ font-size: 1.5rem; font-weight: 500; }}
        .header-stats {{
            font-size: 0.875rem;
            opacity: 0.9;
            margin-top: 0.25rem;
        }}
        .controls {{
            background: white;
            padding: 1rem 2rem;
            border-bottom: 1px solid #ddd;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }}
        .pagination {{
            display: flex;
            gap: 0.5rem;
            align-items: center;
            flex-wrap: wrap;
        }}
        .pagination button {{
            padding: 0.5rem 1rem;
            border: 1px solid #ddd;
            background: white;
            cursor: pointer;
            border-radius: 4px;
            font-size: 0.875rem;
        }}
        .pagination button:hover {{ background: #f0f0f0; }}
        .pagination button:disabled {{
            opacity: 0.5;
            cursor: not-allowed;
        }}
        .pagination button.active {{
            background: #3498db;
            color: white;
            border-color: #3498db;
        }}
        .page-info {{
            font-size: 0.875rem;
            color: #666;
        }}
        .view-options {{
            display: flex;
            gap: 1rem;
            align-items: center;
        }}
        .view-options label {{
            font-size: 0.875rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            cursor: pointer;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}
        .group {{
            background: white;
            border-radius: 8px;
            margin-bottom: 2rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            overflow: hidden;
            display: none;
        }}
        .group.active {{ display: block; }}
        .group-header {{
            background: #ecf0f1;
            padding: 1rem 1.5rem;
            border-bottom: 1px solid #ddd;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .group-title {{
            font-size: 1.1rem;
            font-weight: 600;
            color: #2c3e50;
        }}
        .group-meta {{
            font-size: 0.875rem;
            color: #666;
        }}
        .group-type {{
            display: inline-block;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .group-type.exact {{ background: #e8f5e9; color: #2e7d32; }}
        .group-type.near {{ background: #fff3e0; color: #ef6c00; }}
        .files-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 1.5rem;
            padding: 1.5rem;
        }}
        .file-card {{
            border: 2px solid #e0e0e0;
            border-radius: 6px;
            overflow: hidden;
            transition: border-color 0.2s;
        }}
        .file-card:hover {{ border-color: #3498db; }}
        .file-card.winner {{
            border-color: #27ae60;
            background: #f8fff9;
        }}
        .file-card.winner::before {{
            content: "WINNER";
            display: block;
            background: #27ae60;
            color: white;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.25rem 0.75rem;
            text-align: center;
        }}
        .file-card.duplicate::before {{
            content: "DUPLICATE";
            display: block;
            background: #e74c3c;
            color: white;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.25rem 0.75rem;
            text-align: center;
        }}
        .thumbnail {{
            width: 100%;
            height: 200px;
            object-fit: contain;
            background: #f8f9fa;
            cursor: pointer;
        }}
        .file-info {{
            padding: 1rem;
            font-size: 0.875rem;
        }}
        .file-path {{
            font-family: monospace;
            font-size: 0.75rem;
            color: #666;
            word-break: break-all;
            margin-bottom: 0.75rem;
            max-height: 3em;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        .metadata {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 0.5rem;
        }}
        .meta-item {{
            display: flex;
            flex-direction: column;
        }}
        .meta-label {{
            font-size: 0.7rem;
            color: #999;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .meta-value {{
            font-weight: 500;
            color: #333;
        }}
        .meta-value.size {{
            color: #e74c3c;
            font-weight: 600;
        }}
        .meta-value.resolution {{
            color: #3498db;
            font-weight: 600;
        }}
        .score {{
            font-size: 0.875rem;
            font-weight: 600;
            color: #27ae60;
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid #eee;
        }}
        .lightbox {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.9);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }}
        .lightbox.active {{ display: flex; }}
        .lightbox img {{
            max-width: 90%;
            max-height: 90%;
            object-fit: contain;
        }}
        .lightbox-close {{
            position: absolute;
            top: 1rem;
            right: 1rem;
            color: white;
            font-size: 2rem;
            cursor: pointer;
            background: none;
            border: none;
        }}
        .empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            color: #666;
        }}
        .hidden {{ display: none !important; }}
        .path-config {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 1rem 2rem;
            margin: 0;
        }}
        .path-config h3 {{
            margin-bottom: 0.75rem;
            color: #856404;
            font-size: 1rem;
        }}
        .path-fields {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1.5rem;
        }}
        .path-field {{
            display: flex;
            flex-direction: column;
        }}
        .path-field label {{
            font-size: 0.875rem;
            font-weight: 600;
            color: #856404;
            margin-bottom: 0.25rem;
        }}
        .path-field input {{
            padding: 0.5rem;
            border: 1px solid #ced4da;
            border-radius: 4px;
            font-family: monospace;
            font-size: 0.875rem;
        }}
        .path-field small {{
            font-size: 0.75rem;
            color: #6c757d;
            margin-top: 0.25rem;
        }}
        .source-path {{
            color: #0c5460;
            background: #d1ecf1;
            padding: 0.25rem 0.5rem;
            border-radius: 3px;
            font-size: 0.75rem;
            margin-bottom: 0.5rem;
            word-break: break-all;
        }}
        .output-path {{
            color: #155724;
            background: #d4edda;
            padding: 0.25rem 0.5rem;
            border-radius: 3px;
            font-size: 0.75rem;
            margin-bottom: 0.5rem;
            word-break: break-all;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📷 Duplicate Review Gallery</h1>
        <div class="header-stats">
            {total_groups:,} duplicate groups · {total_dupes:,} duplicate files · Generated {timestamp}
        </div>
    </div>
    
    <div class="path-config">
        <h3>📁 Path Configuration</h3>
        <div class="path-fields">
            <div class="path-field">
                <label for="outputBase">Output Directory:</label>
                <input type="text" id="outputBase" value="{output_dir}" readonly>
                <small>Base path for organized photos (shown as relative paths)</small>
            </div>
            <div class="path-field">
                <label for="sourceBase">Source Directory (optional):</label>
                <input type="text" id="sourceBase" value="" placeholder="e.g., /mnt/backup/photos">
                <small>Enter to show full source paths instead of relative</small>
            </div>
        </div>
    </div>
    
    <div class="controls">
        <div class="pagination" id="pagination"></div>
        <div class="page-info" id="pageInfo"></div>
        <div class="view-options">
            <label>
                <input type="checkbox" id="showThumbnails" checked>
                Show thumbnails
            </label>
            <label>
                <input type="checkbox" id="showDuplicates" checked>
                Show duplicates
            </label>
        </div>
    </div>
    
    <div class="container" id="gallery">
        {groups_html}
    </div>
    
    <div class="lightbox" id="lightbox" onclick="closeLightbox()">
        <button class="lightbox-close">&times;</button>
        <img id="lightbox-img" src="" alt="">
    </div>
    
    <script>
        const ITEMS_PER_PAGE = {items_per_page};
        const totalGroups = {total_groups};
        const totalPages = Math.ceil(totalGroups / ITEMS_PER_PAGE);
        let currentPage = 1;
        
        function init() {{
            renderPagination();
            showPage(1);
            setupEventListeners();
        }}
        
        function renderPagination() {{
            const pagination = document.getElementById('pagination');
            pagination.innerHTML = '';
            
            // Prev button
            const prevBtn = document.createElement('button');
            prevBtn.textContent = '← Prev';
            prevBtn.onclick = () => showPage(currentPage - 1);
            prevBtn.disabled = currentPage === 1;
            pagination.appendChild(prevBtn);
            
            // Page buttons (show max 10)
            const startPage = Math.max(1, currentPage - 4);
            const endPage = Math.min(totalPages, startPage + 9);
            
            for (let i = startPage; i <= endPage; i++) {{
                const btn = document.createElement('button');
                btn.textContent = i;
                btn.className = i === currentPage ? 'active' : '';
                btn.onclick = () => showPage(i);
                pagination.appendChild(btn);
            }}
            
            // Next button
            const nextBtn = document.createElement('button');
            nextBtn.textContent = 'Next →';
            nextBtn.onclick = () => showPage(currentPage + 1);
            nextBtn.disabled = currentPage === totalPages;
            pagination.appendChild(nextBtn);
            
            // Page info
            const startItem = (currentPage - 1) * ITEMS_PER_PAGE + 1;
            const endItem = Math.min(currentPage * ITEMS_PER_PAGE, totalGroups);
            document.getElementById('pageInfo').textContent = 
                `Group ${{startItem}}-${{endItem}} of ${{totalGroups}}`;
        }}
        
        function showPage(page) {{
            if (page < 1 || page > totalPages) return;
            currentPage = page;
            
            // Hide all groups
            document.querySelectorAll('.group').forEach(g => g.classList.remove('active'));
            
            // Show groups for current page
            const start = (page - 1) * ITEMS_PER_PAGE;
            const end = start + ITEMS_PER_PAGE;
            for (let i = start; i < end && i < totalGroups; i++) {{
                const group = document.getElementById('group-' + i);
                if (group) group.classList.add('active');
            }}
            
            renderPagination();
            window.scrollTo(0, 0);
        }}
        
        function setupEventListeners() {{
            document.getElementById('showThumbnails').addEventListener('change', (e) => {{
                document.querySelectorAll('.thumbnail').forEach(img => {{
                    img.style.display = e.target.checked ? 'block' : 'none';
                }});
            }});
            
            document.getElementById('showDuplicates').addEventListener('change', (e) => {{
                document.querySelectorAll('.file-card.duplicate').forEach(card => {{
                    card.style.display = e.target.checked ? 'block' : 'none';
                }});
            }});
        }}
        
        // Path configuration - dynamically resolve paths based on user input
        function resolvePath(relPath, basePath) {{
            if (!relPath) return '';
            if (!basePath) return relPath;
            // Remove leading ./ or / from relPath to avoid double slashes
            relPath = relPath.replace(/^\\.\\/|^\\//, '');
            return basePath.replace(/\\/$/, '') + '/' + relPath;
        }}
        
        function updatePaths() {{
            const sourceBase = document.getElementById('sourceBase').value.trim();
            const outputBase = document.getElementById('outputBase').value.trim();
            
            document.querySelectorAll('.file-card').forEach(card => {{
                const sourceRel = card.getAttribute('data-source-rel');
                const outputRel = card.getAttribute('data-output-rel');
                
                // Update source path display
                const sourcePathEl = card.querySelector('.source-path');
                if (sourcePathEl && sourceRel) {{
                    const displayPath = resolvePath(sourceRel, sourceBase);
                    sourcePathEl.textContent = '📁 Source: ' + displayPath;
                }}
                
                // Update output path display
                const outputPathEl = card.querySelector('.output-path');
                if (outputPathEl && outputRel) {{
                    const displayPath = resolvePath(outputRel, outputBase);
                    outputPathEl.textContent = '✅ Kept: ' + displayPath;
                }}
                
                // Update image src for thumbnails that failed to load
                const img = card.querySelector('.thumbnail');
                if (img && img.getAttribute('data-needs-resolution') === 'true' && outputRel) {{
                    img.src = resolvePath(outputRel, outputBase);
                    img.removeAttribute('data-needs-resolution');
                }}
            }});
        }}
        
        document.getElementById('sourceBase').addEventListener('input', updatePaths);
        document.getElementById('outputBase').addEventListener('input', updatePaths);
        
        function openLightbox(card) {{
            const sourceRel = card.getAttribute('data-source-rel');
            const outputRel = card.getAttribute('data-output-rel');
            const sourceBase = document.getElementById('sourceBase').value.trim();
            const outputBase = document.getElementById('outputBase').value.trim();
            
            // Prefer source path if available, otherwise output path
            const relPath = sourceRel || outputRel;
            const basePath = sourceRel ? sourceBase : outputBase;
            const fullPath = resolvePath(relPath, basePath);
            
            document.getElementById('lightbox-img').src = fullPath;
            document.getElementById('lightbox').classList.add('active');
        }}
        
        function closeLightbox() {{
            document.getElementById('lightbox').classList.remove('active');
        }}
        
        document.addEventListener('keydown', (e) => {{
            if (e.key === 'Escape') closeLightbox();
            if (e.key === 'ArrowLeft' && currentPage > 1) showPage(currentPage - 1);
            if (e.key === 'ArrowRight' && currentPage < totalPages) showPage(currentPage + 1);
        }});
        
        init();
    </script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"


def _get_relative_path(from_dir: Path, target_path: str) -> str:
    """Get relative path from output dir to target file."""
    try:
        target = Path(target_path)
        if target.is_absolute():
            rel = Path(target_path).relative_to(from_dir)
            return str(rel)
    except ValueError:
        pass
    return target_path


def _create_thumbnail(path: str, max_size: int = 200) -> str | None:
    """
    Create a base64-encoded thumbnail for an image.
    Returns None if thumbnail creation fails.
    """
    try:
        img_path = Path(path)
        if not img_path.exists():
            return None
        
        # Skip videos for now (could extract frame later)
        ext = img_path.suffix.lower()
        if ext in ('.mp4', '.mov', '.avi', '.mkv', '.m4v', '.3gp', '.mts', '.m2ts', '.wmv'):
            return None
        
        with Image.open(img_path) as img:
            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')
            
            # Resize maintaining aspect ratio
            img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            # Save to bytes
            buf = BytesIO()
            img.save(buf, format='JPEG', quality=85, optimize=True)
            buf.seek(0)
            
            return base64.b64encode(buf.read()).decode()
    except Exception:
        return None


def _render_file_card(file_data: dict, output_dir: Path, is_winner: bool) -> str:
    """Render a single file card HTML."""
    role_class = "winner" if is_winner else "duplicate"
    
    # Source path (original location)
    source_path = file_data.get('path', '')
    source_rel = _get_relative_path(output_dir, source_path)
    
    # Output path (organized location) - may be same as source if not organized
    output_path = file_data.get('output_path') or source_path
    output_rel = _get_relative_path(output_dir, output_path)
    
    # Try to create thumbnail from the actual file location
    thumbnail_b64 = _create_thumbnail(source_path)
    if thumbnail_b64:
        img_src = f"data:image/jpeg;base64,{thumbnail_b64}"
    else:
        # Use data attribute for path - JS will resolve based on configuration
        img_src = ""
    
    # Store paths as data attributes for dynamic resolution
    path_attrs = f'data-source-rel="{html.escape(source_rel or source_path)}" data-output-rel="{html.escape(output_rel or output_path)}"'
    
    # Format metadata
    size = _fmt_bytes(file_data.get('size', 0) or 0)
    width = file_data.get('width')
    height = file_data.get('height')
    dims = f"{width}×{height}" if width and height else "—"
    fmt = file_data.get('format', '—') or '—'
    date = (file_data.get('exif_date') or '—')[:10]
    camera = ' / '.join(filter(None, [file_data.get('exif_make'), file_data.get('exif_model')])) or '—'
    score = f"{file_data.get('score', 0):.4f}" if file_data.get('score') is not None else '—'
    
    # Build path display HTML with data attributes for dynamic resolution
    source_display = html.escape(source_rel or source_path)
    output_display = html.escape(output_rel or output_path)
    
    paths_html = f"""<div class="source-path" data-rel-path="{html.escape(source_rel or source_path)}" title="Source: {source_display}">
    📁 Source: {source_display}
</div>"""
    
    if is_winner and output_path != source_path:
        paths_html += f"""<div class="output-path" data-rel-path="{html.escape(output_rel or output_path)}" title="Output: {output_display}">
    ✅ Kept: {output_display}
</div>"""
    
    return f"""<div class="file-card {role_class}" {path_attrs}>
    <img class="thumbnail" src="{img_src}" alt="" loading="lazy" data-needs-resolution="true">
    <div class="file-info">
        {paths_html}
        <div class="metadata">
            <div class="meta-item">
                <span class="meta-label">Format</span>
                <span class="meta-value">{html.escape(fmt)}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Size</span>
                <span class="meta-value size">{html.escape(size)}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Resolution</span>
                <span class="meta-value resolution">{html.escape(dims)}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Date</span>
                <span class="meta-value">{html.escape(date)}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Camera</span>
                <span class="meta-value">{html.escape(camera)}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Score</span>
                <span class="meta-value">{html.escape(score)}</span>
            </div>
        </div>
    </div>
</div>"""


def _render_group(group: dict, group_index: int, output_dir: Path) -> str:
    """Render a single duplicate group HTML."""
    group_id = group['group_id']
    file_count = group['file_count']
    total_bytes = _fmt_bytes(group.get('total_bytes', 0) or 0)
    is_near_dup = group.get('is_near_dup', False)
    group_type = 'near' if is_near_dup else 'exact'
    group_type_label = 'Near-duplicate' if is_near_dup else 'Exact duplicate'
    
    files_html = []
    
    # Sort files: winners first, then by score descending
    files = sorted(group['files'], key=lambda f: (-f.get('is_best', 0), -(f.get('score') or 0)))
    
    for f in files:
        is_winner = f.get('is_best', False)
        files_html.append(_render_file_card(f, output_dir, is_winner))
    
    return f"""<div class="group" id="group-{group_index}">
    <div class="group-header">
        <div>
            <div class="group-title">Group {group_id}</div>
            <div class="group-meta">{file_count} files · {total_bytes} total</div>
        </div>
        <span class="group-type {group_type}">{group_type_label}</span>
    </div>
    <div class="files-grid">
        {''.join(files_html)}
    </div>
</div>"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_gallery(
    db: Database,
    output_dir: Path,
    items_per_page: int = 50,
    sort_by: str = "size",
    limit: int | None = None,
) -> Path:
    """
    Generate an HTML gallery for reviewing duplicate groups.
    
    Args:
        db: Database instance
        output_dir: Output directory (for relative paths)
        items_per_page: Number of groups per page
        sort_by: 'size' | 'count' | 'suspicious' - how to sort groups
        limit: Maximum number of groups to include (None = all)
    
    Returns:
        Path to the generated HTML file
    """
    console.print("[bold cyan]Generating duplicate review gallery…[/bold cyan]")
    
    # Get all duplicate groups
    total_groups = db.stats()["duplicate_groups"]
    if limit:
        total_groups = min(total_groups, limit)
    
    # Fetch groups from DB
    groups = db.review_groups(limit=total_groups, sort_by=sort_by)
    
    if not groups:
        console.print("[yellow]No duplicate groups found.[/yellow]")
        return output_dir / "reports" / "dup_gallery.html"
    
    console.print(f"Rendering {len(groups):,} groups with pagination ({items_per_page} per page)…")
    
    # Render all groups
    groups_html = []
    for i, group in enumerate(groups):
        groups_html.append(_render_group(group, i, output_dir))
    
    # Calculate total duplicates
    total_dupes = sum(g['file_count'] - 1 for g in groups)
    
    # Generate final HTML
    html_content = HTML_TEMPLATE.format(
        total_groups=len(groups),
        total_dupes=total_dupes,
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        items_per_page=items_per_page,
        groups_html='\n'.join(groups_html),
        output_dir=str(output_dir),
    )
    
    # Write to file
    report_dir = output_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%d_%H%M%S")
    html_path = report_dir / f"dup_gallery_{ts}.html"
    
    html_path.write_text(html_content, encoding="utf-8")
    
    console.print(f"[green]Gallery written to:[/green] {html_path}")
    console.print(f"[dim]Open in browser: file://{html_path.absolute()}[/dim]")
    
    return html_path
