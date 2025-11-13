#!/usr/bin/env python3
"""
Quick utility script to check if map tasks are producing intermediate files.

Usage:
    python3 scripts/check_map_output.py [--shared-dir /path/to/shared] [--job-id job_123]
"""

import os
import sys
import pickle
import glob
import argparse
from pathlib import Path

def check_intermediate_files(shared_dir: str, job_id: str = None):
    """Check and display intermediate files created by map tasks."""
    intermediate_dir = os.path.join(shared_dir, 'intermediate')
    
    if not os.path.exists(intermediate_dir):
        print(f"❌ Intermediate directory does not exist: {intermediate_dir}")
        print(f"   Make sure map tasks have been executed.")
        return False
    
    # Find all pickle files
    if job_id:
        pattern = f"{job_id}_map_*_part_*.pickle"
    else:
        pattern = "*_map_*_part_*.pickle"
    
    files = glob.glob(os.path.join(intermediate_dir, pattern))
    
    if not files:
        print(f"❌ No intermediate files found!")
        print(f"   Pattern: {pattern}")
        print(f"   Directory: {intermediate_dir}")
        print(f"\n   This could mean:")
        print(f"   1. Map tasks haven't been executed yet")
        print(f"   2. Map tasks failed to create files")
        print(f"   3. Files were cleaned up")
        return False
    
    print(f"✅ Found {len(files)} intermediate file(s)\n")
    
    # Group by job and task
    jobs = {}
    for file_path in sorted(files):
        file_name = os.path.basename(file_path)
        
        # Parse: job_id_map_task_id_part_partition_id.pickle
        parts = file_name.split('_')
        if len(parts) >= 4 and parts[-2] == 'part':
            # Extract job_id (everything before _map_)
            map_idx = file_name.find('_map_')
            if map_idx > 0:
                file_job_id = file_name[:map_idx]
                task_part = file_name[map_idx+5:]  # Skip '_map_'
                task_id = task_part.split('_part_')[0]
                partition_id = task_part.split('_part_')[1].split('.')[0]
                
                if file_job_id not in jobs:
                    jobs[file_job_id] = {}
                if task_id not in jobs[file_job_id]:
                    jobs[file_job_id][task_id] = []
                
                jobs[file_job_id][task_id].append((partition_id, file_path))
    
    # Display results
    for job_id, tasks in sorted(jobs.items()):
        print(f"Job: {job_id}")
        print(f"  Map tasks: {len(tasks)}")
        
        total_records = 0
        for task_id, partitions in sorted(tasks.items()):
            print(f"\n  Task {task_id}:")
            print(f"    Partitions: {len(partitions)}")
            
            for partition_id, file_path in sorted(partitions):
                file_size = os.path.getsize(file_path)
                
                try:
                    with open(file_path, 'rb') as f:
                        data = pickle.load(f)
                    record_count = len(data)
                    total_records += record_count
                    
                    print(f"      Partition {partition_id}: {record_count} records, {file_size} bytes")
                    
                    if record_count > 0:
                        # Show sample
                        sample = data[:3] if len(data) >= 3 else data
                        print(f"        Sample: {sample}")
                        
                except Exception as e:
                    print(f"      Partition {partition_id}: ERROR - {e}")
        
        print(f"\n  Total records for job {job_id}: {total_records}")
        print()
    
    return True


def verify_map_function(shared_dir: str, job_file_path: str):
    """Verify that a map function can be loaded and executed."""
    print(f"\n{'='*60}")
    print("Verifying Map Function")
    print(f"{'='*60}")
    
    if not os.path.exists(job_file_path):
        print(f"❌ Job file not found: {job_file_path}")
        return False
    
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("test_job", job_file_path)
        if spec is None or spec.loader is None:
            print(f"❌ Failed to load job file: {job_file_path}")
            return False
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        if not hasattr(module, 'map_fn'):
            print(f"❌ Job file missing map_fn function")
            return False
        
        print(f"✅ Job file loaded successfully")
        print(f"   File: {job_file_path}")
        print(f"   Has map_fn: {hasattr(module, 'map_fn')}")
        print(f"   Has reduce_fn: {hasattr(module, 'reduce_fn')}")
        
        # Test map function with sample input
        print(f"\n   Testing map_fn with sample input:")
        test_input = "hello world test"
        print(f"   Input: '{test_input}'")
        
        results = list(module.map_fn("test_key", test_input))
        print(f"   Output: {results}")
        
        if len(results) == 0:
            print(f"   ⚠️  WARNING: map_fn returned no results!")
        else:
            print(f"   ✅ map_fn produced {len(results)} key-value pairs")
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading/executing map function: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Check if map tasks are producing intermediate files'
    )
    parser.add_argument(
        '--shared-dir', 
        type=str, 
        default='shared',
        help='Path to shared directory (default: shared)'
    )
    parser.add_argument(
        '--job-id', 
        type=str,
        help='Filter by specific job ID'
    )
    parser.add_argument(
        '--verify-map',
        type=str,
        help='Verify map function in a job file (provide path to job file)'
    )
    
    args = parser.parse_args()
    
    # Convert to absolute path
    shared_dir = os.path.abspath(args.shared_dir)
    
    print(f"Checking map output in: {shared_dir}")
    print(f"{'='*60}\n")
    
    # Verify map function if requested
    if args.verify_map:
        verify_map_function(shared_dir, args.verify_map)
        print()
    
    # Check intermediate files
    success = check_intermediate_files(shared_dir, args.job_id)
    
    if success:
        print(f"\n✅ Map tasks appear to be working correctly!")
        print(f"   Intermediate files are being created.")
    else:
        print(f"\n❌ Map tasks may not be working correctly.")
        print(f"   No intermediate files found.")
    
    sys.exit(0 if success else 1)


