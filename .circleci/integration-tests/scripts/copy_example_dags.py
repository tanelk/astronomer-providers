import argparse
import shutil
from pathlib import Path


def copy_example_dags(source_dir: str, dest_dir: str):
    """Copy example dags from source_dir to dest_dir."""
    source_dir = Path(source_dir)
    dest_dir = Path(dest_dir)
    assert source_dir.exists() and dest_dir.is_dir()
    assert dest_dir.exists() and dest_dir.is_dir()

    # Get all files except __init__.py
    all_example_dags_files = list(source_dir.rglob("example_dags/[!__init__.py]*"))
    assert len(all_example_dags_files) > 0, "No example dags found in source directory"

    print(f"Found '{len(all_example_dags_files)}' example dags in source directory")

    for ex_file in all_example_dags_files:
        if ex_file.is_file():
            shutil.copy(str(ex_file), str(dest_dir))
        print("File Path: ", ex_file.absolute())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy example dags from source_dir to dest_dir.")
    parser.add_argument("source_dir", type=Path, help="Source directory containing example DAGs")
    parser.add_argument("dest_dir", type=Path, help="Destination directory")
    args = parser.parse_args()
    copy_example_dags(args.source_dir, args.dest_dir)
