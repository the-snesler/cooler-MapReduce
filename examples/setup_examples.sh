#!/bin/bash
# examples/setup_examples.sh

set -e

echo "Setting up example datasets..."

# Create examples directory structure
mkdir -p examples/data

# Download Shakespeare text if not exists
if [ ! -f examples/data/shakespeare.txt ]; then
    echo "Downloading Shakespeare text..."
    curl -o examples/data/shakespeare.txt https://www.gutenberg.org/files/100/100-0.txt
fi

# Generate synthetic dataset for testing
echo "Generating synthetic dataset..."
python3 <<EOF
import random

words = ['the', 'quick', 'brown', 'fox', 'jumps', 'over', 'lazy', 'dog', 'hello', 'world']

with open('examples/data/synthetic.txt', 'w') as f:
    for i in range(10000):
        line = ' '.join(random.choices(words, k=10))
        f.write(line + '\n')

print("Generated synthetic dataset")
EOF

echo "Example setup complete!"
