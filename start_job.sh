#!/bin/bash

cd "/home/webadm/process/iz_to_iz_automation"

# Path to your virtual environment
VENV_PATH="/home/webadm/venv"

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Check if size argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <SMALL|LARGE>"
    exit 1
fi

# Start python script with size argument
python3 -u start_process.py -size "$1"