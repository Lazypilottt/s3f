"""s3f tool definition."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from bench import PROJECT_ROOT

if TYPE_CHECKING:
    from bench.types import Backend, DownloadCmd, ListCmd, UploadCmd


def _build_s3f() -> Path:
    """Build s3f in release mode, return binary path."""
    print("  building s3f (release)...")
    subprocess.run(
        ["cargo", "build", "--release", "--bin", "s3f", "--quiet"],
        cwd=str(PROJECT_ROOT),
        check=True,
    )
    return PROJECT_ROOT / "target" / "release" / "s3f"


@dataclass
class S3fTool:
    name: str = "s3f"
    _bin: Path | None = field(default=None, repr=False)

    def upload_cmd(self, backend: Backend, params: UploadCmd) -> list[str]:
        binary = self._ensure_built()
        cmd = [
            str(binary),
            "-e",
            backend.endpoint,
            "-r",
            backend.region,
            "upload",
            str(params.data_dir),
            "-b",
            params.bucket,
            "-p",
            params.prefix,
            "-q",
        ]
        # Only pass -w/-c when the harness requests specific values (sweeps).
        # Regular benchmarks leave them at 0 → s3f uses its built-in defaults.
        if params.workers > 0:
            cmd.extend(["-w", str(params.workers)])
        if params.concurrency > 0:
            cmd.extend(["-c", str(params.concurrency)])
        return cmd

    def download_cmd(self, backend: Backend, params: DownloadCmd) -> list[str]:
        binary = self._ensure_built()
        return [
            str(binary),
            "-e",
            backend.endpoint,
            "-r",
            backend.region,
            "download",
            "-b",
            params.bucket,
            "-p",
            params.prefix,
            "-d",
            str(params.dest_dir),
            "-q",
        ]

    def list_cmd(self, backend: Backend, params: ListCmd) -> list[str]:
        binary = self._ensure_built()
        return [
            str(binary),
            "-e",
            backend.endpoint,
            "-r",
            backend.region,
            "ls",
            "-b",
            params.bucket,
            "-p",
            params.prefix,
            "-q",
        ]

    def setup(self, _backends: list[Backend]) -> None:
        self._bin = _build_s3f()

    def _ensure_built(self) -> Path:
        if self._bin is None:
            self._bin = _build_s3f()
        return self._bin


tool = S3fTool()
