import logging
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def pl_cnpj_to_numeric(col: str = "cnpj") -> pl.Expr:
    """Strip non-digits from CNPJ and cast to UInt64."""
    return pl.col(col).str.replace_all(r"\D", "").cast(pl.UInt64)


def pl_cnpj_to_formatted(col: str = "cnpj") -> pl.Expr:
    """Format numeric CNPJ as XX.XXX.XXX/XXXX-XX."""
    s = pl.col(col).cast(pl.String).str.zfill(14)
    return pl.concat_str(
        [
            s.str.slice(0, 2),
            pl.lit("."),
            s.str.slice(2, 3),
            pl.lit("."),
            s.str.slice(5, 3),
            pl.lit("/"),
            s.str.slice(8, 4),
            pl.lit("-"),
            s.str.slice(12, 2),
        ]
    )


_backup_root = Path("data/01_raw_backup")


def backup_file(file_path: Path, backup_root: Path | None = None) -> Path:
    """Backup file to date-based directory, preserving relative path structure.

    Skips if backup already exists for same day.
    """
    root = backup_root or _backup_root

    date_str = datetime.now().strftime("%Y-%m-%d")
    backup_dir = root / date_str
    backup_dir.mkdir(parents=True, exist_ok=True)

    parts = file_path.parts
    try:
        raw_idx = parts.index("01_raw")
        relative_parts = parts[raw_idx + 1 :]
        relative_path = (
            Path(*relative_parts) if relative_parts else Path(file_path.name)
        )
    except (ValueError, IndexError):
        relative_path = Path(file_path.name)

    backup_path = backup_dir / relative_path
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    if backup_path.exists():
        logger.debug(f"Backup already exists for {file_path.name}, skipping")
        return backup_path

    shutil.copy2(file_path, backup_path)
    logger.debug(f"Backed up {file_path.name} to {backup_path}")
    return backup_path


def restore_file(backup_path: Path, original_path: Path) -> None:
    """Copy backup file back to original location."""
    shutil.copy2(backup_path, original_path)
    logger.info(f"Restored {original_path.name} from backup")
