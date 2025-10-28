#!/bin/bash
# Helper script to upload data to shared storage

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <local_path> <shared_path>"
    echo "Example: $0 mydata.txt /shared/input/mydata.txt"
    exit 1
fi

LOCAL_PATH=$1
SHARED_PATH=$2

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Resolve the shared path
FULL_PATH="${PROJECT_ROOT}${SHARED_PATH}"

# Create directory if it doesn't exist
mkdir -p "$(dirname "$FULL_PATH")"

# Copy the file
cp "$LOCAL_PATH" "$FULL_PATH"

if [ $? -eq 0 ]; then
    echo "Successfully uploaded $LOCAL_PATH to $SHARED_PATH"
else
    echo "Error uploading file"
    exit 1
fi
