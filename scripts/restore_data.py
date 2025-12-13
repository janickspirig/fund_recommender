#!/usr/bin/env python3
"""Restore raw CVM data from the latest backup folder.

Restores only files that were backed up (i.e., files that had quote issues
and were fixed by the validation hooks). The backup contains the original
files before fixes were applied.

Usage:
    python scripts/restore_data.py

Run from the project root directory.
"""

import shutil
from pathlib import Path


def find_latest_backup(backup_base: Path) -> Path | None:
    """Find the most recent backup folder by date."""
    if not backup_base.exists():
        return None

    backup_dirs = [d for d in backup_base.iterdir() if d.is_dir()]
    if not backup_dirs:
        return None

    return sorted(backup_dirs, reverse=True)[0]


def main():
    backup_base = Path("data/01_raw_backup")
    dst_base = Path("data/01_raw/cvm/data/monthly")
    blc_dirs = [
        "blc_1",
        "blc_2",
        "blc_3",
        "blc_4",
        "blc_5",
        "blc_6",
        "blc_7",
        "blc_8",
        "pl",
    ]

    if not backup_base.exists():
        print(f"ERROR: Backup folder not found: {backup_base}")
        print("Make sure you run this script from the project root directory.")
        return

    latest_backup = find_latest_backup(backup_base)
    if latest_backup is None:
        print(f"ERROR: No backup folders found in {backup_base}")
        return

    src_base = latest_backup / "cvm" / "data"
    if not src_base.exists():
        print(f"ERROR: Backup data not found at: {src_base}")
        return

    print(f"Restoring from backup: {latest_backup.name}")
    print(f"Destination: {dst_base}")

    print("\nRestoring files (overwriting fixed versions with originals)...")
    copied = 0
    for blc_dir in blc_dirs:
        src_dir = src_base / blc_dir
        dst_dir = dst_base / blc_dir

        if not src_dir.exists():
            continue

        dst_dir.mkdir(parents=True, exist_ok=True)
        dir_count = 0
        for csv_file in src_dir.glob("*.csv"):
            shutil.copy2(csv_file, dst_dir / csv_file.name)
            copied += 1
            dir_count += 1
        if dir_count > 0:
            print(f"  Restored {dir_count} files to {blc_dir}/")

    if copied > 0:
        print(f"\nDone! Restored {copied} files from {latest_backup.name}")
        print("\nNow run: kedro run")
        print("(The validation hooks will re-apply fixes to these files)")
    else:
        print("\nNo files to restore (backup may be empty or already restored)")


if __name__ == "__main__":
    main()
