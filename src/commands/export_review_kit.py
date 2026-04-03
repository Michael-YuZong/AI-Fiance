"""Export the portable external review system kit."""

from __future__ import annotations

import argparse
import shutil
import tarfile
from pathlib import Path

from src.utils.config import resolve_project_path


KIT_SOURCE = "kits/external_review_system"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export the portable external review kit to a target directory.")
    parser.add_argument("--out", default="tmp/external_review_system_kit", help="Target directory for the exported kit")
    parser.add_argument("--force", action="store_true", help="Overwrite the target directory if it already exists")
    parser.add_argument("--archive", action="store_true", help="Also create a .tar.gz archive next to the exported directory")
    return parser


def _export_tree(source: Path, target: Path, *, force: bool) -> None:
    if target.exists():
        if not force:
            raise SystemExit(f"目标目录已存在：`{target}`。如需覆盖，请加 `--force`。")
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target)


def _write_archive(target: Path) -> Path:
    archive_path = target.parent / f"{target.name}.tar.gz"
    with tarfile.open(archive_path, "w:gz") as handle:
        handle.add(target, arcname=target.name)
    return archive_path


def main() -> None:
    args = build_parser().parse_args()
    source = resolve_project_path(KIT_SOURCE)
    if not source.exists():
        raise SystemExit(f"找不到 portable review kit 源目录：`{source}`。")

    target = resolve_project_path(args.out)
    _export_tree(source, target, force=args.force)
    print(f"Exported portable review kit to: {target}")

    if args.archive:
        archive_path = _write_archive(target)
        print(f"Archive written to: {archive_path}")


if __name__ == "__main__":
    main()
