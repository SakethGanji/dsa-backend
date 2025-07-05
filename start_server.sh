#!/bin/bash
# Start the DSA Platform API server

echo "Starting DSA Platform API server..."

# Set environment variables
export PYTHONPATH=/home/saketh/Projects/dsa/src
export DATABASE_URL="postgresql://user:password@localhost/dsa"

# Kill any existing server on port 8000
lsof -ti:8000 | xargs kill -9 2>/dev/null || true

# Start the server in the background
cd /home/saketh/Projects/dsa/src
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
SERVER_PID=$!

echo "Server starting with PID: $SERVER_PID"
echo "Waiting for server to be ready..."

# Wait for server to start
for i in {1..30}; do
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "✅ Server is ready!"
        echo "Server PID: $SERVER_PID"
        echo "To stop the server, run: kill $SERVER_PID"
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo "❌ Server failed to start within 30 seconds"
exit 1