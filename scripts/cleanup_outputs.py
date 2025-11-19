#!/usr/bin/env python3
"""
Cleanup script for MapReduce intermediate and output files.
Removes all files from intermediate/ and output/ directories while preserving .gitkeep files.
"""

import os
import sys
from pathlib import Path

# Default paths (relative to project root)
SHARED_DIR = Path("shared")
INTERMEDIATE_DIR = SHARED_DIR / "intermediate"
OUTPUT_DIR = SHARED_DIR / "output"

def cleanup_directory(directory: Path, dry_run: bool = False):
    """
    Remove all files from a directory except .gitkeep.
    
    Args:
        directory: Path to the directory to clean
        dry_run: If True, only show what would be deleted without actually deleting
    
    Returns:
        tuple: (files_deleted, bytes_freed)
    """
    if not directory.exists():
        print(f"  ⚠️  Directory does not exist: {directory}")
        return 0, 0
    
    files_deleted = 0
    bytes_freed = 0
    
    for item in directory.iterdir():
        # Skip .gitkeep files
        if item.name == ".gitkeep":
            continue
        
        # Skip directories (like __pycache__)
        if item.is_dir():
            continue
        
        if item.is_file():
            file_size = item.stat().st_size
            if dry_run:
                print(f"    Would delete: {item.name} ({file_size} bytes)")
                files_deleted += 1
                bytes_freed += file_size
            else:
                try:
                    item.unlink()
                    files_deleted += 1
                    bytes_freed += file_size
                except Exception as e:
                    print(f"    ❌ Error deleting {item.name}: {e}")
    
    return files_deleted, bytes_freed

def format_size(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def main():
    """Main cleanup function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Clean up MapReduce intermediate and output files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Clean both directories (interactive)
  %(prog)s --all              # Clean both directories (non-interactive)
  %(prog)s --intermediate     # Clean only intermediate files
  %(prog)s --output           # Clean only output files
  %(prog)s --dry-run          # Show what would be deleted without deleting
        """
    )
    
    parser.add_argument(
        '--intermediate', '-i',
        action='store_true',
        help='Clean only intermediate files'
    )
    parser.add_argument(
        '--output', '-o',
        action='store_true',
        help='Clean only output files'
    )
    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Clean both directories without prompting'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    
    args = parser.parse_args()
    
    # Determine which directories to clean
    clean_intermediate = args.intermediate or (not args.output and (args.all or not args.intermediate))
    clean_output = args.output or (not args.intermediate and (args.all or not args.output))
    
    if not clean_intermediate and not clean_output:
        print("No directories selected for cleanup.")
        return
    
    print("=" * 70)
    print("MapReduce Output Cleanup")
    print("=" * 70)
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - No files will be deleted")
        print()
    
    total_files = 0
    total_bytes = 0
    
    # Clean intermediate directory
    if clean_intermediate:
        print(f"\n📁 Cleaning: {INTERMEDIATE_DIR}")
        if not args.dry_run and not args.all:
            response = input(f"  Delete all files in {INTERMEDIATE_DIR}? (y/n): ")
            if response.lower() != 'y':
                print("  Skipped.")
                clean_intermediate = False
        
        if clean_intermediate:
            files, bytes_freed = cleanup_directory(INTERMEDIATE_DIR, dry_run=args.dry_run)
            total_files += files
            total_bytes += bytes_freed
            if args.dry_run:
                print(f"  Would delete {files} files")
            else:
                print(f"  ✓ Deleted {files} files ({format_size(bytes_freed)})")
    
    # Clean output directory
    if clean_output:
        print(f"\n📁 Cleaning: {OUTPUT_DIR}")
        if not args.dry_run and not args.all:
            response = input(f"  Delete all files in {OUTPUT_DIR}? (y/n): ")
            if response.lower() != 'y':
                print("  Skipped.")
                clean_output = False
        
        if clean_output:
            files, bytes_freed = cleanup_directory(OUTPUT_DIR, dry_run=args.dry_run)
            total_files += files
            total_bytes += bytes_freed
            if args.dry_run:
                print(f"  Would delete {files} files")
            else:
                print(f"  ✓ Deleted {files} files ({format_size(bytes_freed)})")
    
    # Summary
    print("\n" + "=" * 70)
    if args.dry_run:
        print(f"DRY RUN: Would delete {total_files} files ({format_size(total_bytes)})")
    else:
        print(f"✓ Cleanup complete: {total_files} files deleted ({format_size(total_bytes)})")
    print("=" * 70)
    
    # Verify .gitkeep files are preserved
    print("\n📋 Verifying .gitkeep files are preserved:")
    for directory in [INTERMEDIATE_DIR, OUTPUT_DIR]:
        gitkeep = directory / ".gitkeep"
        if gitkeep.exists():
            print(f"  ✓ {gitkeep} exists")
        else:
            print(f"  ⚠️  {gitkeep} not found (directory may be empty)")

if __name__ == "__main__":
    main()

