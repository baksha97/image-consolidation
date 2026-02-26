"""Configuration model — loaded from a TOML file or built from CLI flags."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class SourceConfig(BaseModel):
    paths: list[Path] = Field(default_factory=list)
    # Map of path-prefix string → integer priority (higher = preferred when deduping)
    priorities: dict[str, int] = Field(default_factory=dict)

    @field_validator("paths", mode="before")
    @classmethod
    def expand_paths(cls, v: list) -> list[Path]:
        return [Path(p).expanduser().resolve() for p in v]


class OutputConfig(BaseModel):
    directory: Path = Path("output")
    structure: Literal["YYYY/MM", "YYYY/MM/DD"] = "YYYY/MM"
    mode: Literal["copy", "move", "hardlink"] = "copy"
    unsorted_dir: str = "unsorted"

    @field_validator("directory", mode="before")
    @classmethod
    def expand_dir(cls, v) -> Path:
        return Path(v).expanduser().resolve()


class DedupeConfig(BaseModel):
    phash_threshold: int = Field(8, ge=0, le=64)
    exact_only: bool = False  # skip perceptual hash, only catch bit-identical files


class FormatsConfig(BaseModel):
    include_videos: bool = True
    video_mode: Literal["metadata", "skip"] = "metadata"

    image_extensions: list[str] = Field(default_factory=lambda: [
        ".jpg", ".jpeg", ".png", ".tiff", ".tif",
        ".heic", ".heif", ".webp",
        ".raw", ".cr2", ".cr3", ".nef", ".arw", ".dng", ".orf", ".rw2", ".raf",
        ".pef", ".srw", ".x3f",
    ])
    video_extensions: list[str] = Field(default_factory=lambda: [
        ".mp4", ".mov", ".avi", ".mkv", ".m4v",
        ".3gp", ".mts", ".m2ts", ".wmv",
    ])
    sidecar_extensions: list[str] = Field(default_factory=lambda: [
        ".xmp", ".dop", ".pp3", ".aae", ".thm",
    ])

    @model_validator(mode="after")
    def normalize_extensions(self) -> "FormatsConfig":
        self.image_extensions = [e.lower() for e in self.image_extensions]
        self.video_extensions = [e.lower() for e in self.video_extensions]
        self.sidecar_extensions = [e.lower() for e in self.sidecar_extensions]
        return self

    def is_image(self, path: Path) -> bool:
        return path.suffix.lower() in self.image_extensions

    def is_video(self, path: Path) -> bool:
        return self.include_videos and path.suffix.lower() in self.video_extensions

    def is_sidecar(self, path: Path) -> bool:
        return path.suffix.lower() in self.sidecar_extensions

    def is_supported(self, path: Path) -> bool:
        return self.is_image(path) or self.is_video(path) or self.is_sidecar(path)


class PerformanceConfig(BaseModel):
    workers: int = Field(12, ge=1, le=128)
    batch_size: int = Field(500, ge=1)


class Config(BaseModel):
    sources: SourceConfig = Field(default_factory=SourceConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    dedupe: DedupeConfig = Field(default_factory=DedupeConfig)
    formats: FormatsConfig = Field(default_factory=FormatsConfig)
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig)
    db_path: Path = Path("imgc.db")

    @field_validator("db_path", mode="before")
    @classmethod
    def expand_db(cls, v) -> Path:
        return Path(v).expanduser().resolve()

    @classmethod
    def from_toml(cls, path: Path) -> "Config":
        with open(path, "rb") as f:
            data = tomllib.load(f)
        return cls.model_validate(data)

    @classmethod
    def default_with_sources(cls, sources: list[str], output: str) -> "Config":
        return cls(
            sources=SourceConfig(paths=sources),  # type: ignore[arg-type]
            output=OutputConfig(directory=output),  # type: ignore[arg-type]
        )

    def source_priority(self, path: str) -> int:
        """Return the highest matching priority for a file path."""
        p = Path(path)
        best = 0
        for prefix, score in self.sources.priorities.items():
            try:
                p.relative_to(prefix)
                best = max(best, score)
            except ValueError:
                pass
        return best
