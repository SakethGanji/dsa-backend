#!/bin/bash
# Run the DSA application from the project root

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run the application
python3 main.py