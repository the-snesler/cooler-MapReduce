#!/bin/bash
# Non-interactive benchmark runner
cd "$(dirname "$0")"
python3 << 'PYTHON_EOF'
import subprocess
import json
import time
import os
import sys
from datetime import datetime
from pathlib import Path
import csv

# Use the same configuration from benchmark.py but skip interactive prompts
exec(open("benchmark.py").read().replace("runs_per_benchmark = int(input", "runs_per_benchmark = 1 #int(input").replace('response = input', 'response = "y" #input'))
PYTHON_EOF
