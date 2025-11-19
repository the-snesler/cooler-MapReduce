#!/usr/bin/env python3
"""
Generate benchmark input files by replicating story.txt to different sizes.
"""

import os
from pathlib import Path

# Configuration
SHARED_DIR = Path("shared")
SAMPLES_DIR = SHARED_DIR / "samples"
INPUT_DIR = SHARED_DIR / "input"
SOURCE_FILE = SAMPLES_DIR / "story.txt"

# Target sizes (approximate)
TARGETS = [
    ("story_medium.txt", 981 * 1024),   # ~1MB
    ("story_large.txt", 9.6 * 1024 * 1024),   # ~10MB
    ("story_xlarge.txt", 48 * 1024 * 1024),   # ~50MB
]

def generate_file(output_path: Path, target_size: int, source_content: bytes):
    """
    Generate a file by replicating source content until target size is reached.
    
    Args:
        output_path: Path where the output file should be written
        target_size: Target file size in bytes
        source_content: The content to replicate
    """
    print(f"Generating {output_path.name} (target: {target_size / (1024*1024):.1f} MB)...")
    
    source_size = len(source_content)
    if source_size == 0:
        raise ValueError("Source file is empty!")
    
    # Calculate how many full replications we need
    replications = int(target_size / source_size)
    
    # Write the file
    with open(output_path, 'wb') as f:
        # Write full replications
        for _ in range(replications):
            f.write(source_content)
        
        # Add partial replication if needed to reach target size
        remaining = int(target_size - (replications * source_size))
        if remaining > 0:
            f.write(source_content[:remaining])
    
    actual_size = output_path.stat().st_size
    print(f"  ✓ Created: {output_path.name} ({actual_size / (1024*1024):.2f} MB, {replications} replications)")
    return actual_size

def main():
    """Generate all benchmark input files."""
    print("=" * 70)
    print("Generating Benchmark Input Files")
    print("=" * 70)
    
    # Ensure directories exist
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Read source file
    if not SOURCE_FILE.exists():
        print(f"❌ Source file not found: {SOURCE_FILE}")
        print(f"   Please ensure {SOURCE_FILE} exists.")
        return 1
    
    print(f"\n📄 Source file: {SOURCE_FILE}")
    source_content = SOURCE_FILE.read_bytes()
    source_size = len(source_content)
    print(f"   Size: {source_size} bytes")
    
    # Generate each target file
    print(f"\n📝 Generating files in {INPUT_DIR}...")
    total_size = 0
    
    for filename, target_size in TARGETS:
        output_path = INPUT_DIR / filename
        
        # Skip if file already exists and is approximately the right size
        if output_path.exists():
            existing_size = output_path.stat().st_size
            if abs(existing_size - target_size) < target_size * 0.1:  # Within 10%
                print(f"  ⏭️  Skipping {filename} (already exists, size: {existing_size / (1024*1024):.2f} MB)")
                total_size += existing_size
                continue
        
        try:
            actual_size = generate_file(output_path, target_size, source_content)
            total_size += actual_size
        except Exception as e:
            print(f"  ❌ Error generating {filename}: {e}")
            return 1
    
    print("\n" + "=" * 70)
    print(f"✓ Generation complete!")
    print(f"  Total size: {total_size / (1024*1024):.2f} MB")
    print(f"  Files created in: {INPUT_DIR}")
    print("=" * 70)
    
    # List generated files
    print("\n📋 Generated files:")
    for filename, _ in TARGETS:
        filepath = INPUT_DIR / filename
        if filepath.exists():
            size = filepath.stat().st_size
            print(f"  ✓ {filename}: {size / (1024*1024):.2f} MB")
        else:
            print(f"  ❌ {filename}: NOT FOUND")
    
    return 0

if __name__ == "__main__":
    exit(main())

