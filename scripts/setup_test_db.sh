#!/bin/bash
# Setup script for test database

echo "Setting up DSA test database..."

# Check if postgres is running
if ! docker ps | grep -q "postgres"; then
    echo "Error: PostgreSQL container is not running"
    echo "Please start the postgres container first"
    exit 1
fi

# Initialize the database schema
echo "Initializing database schema..."
python3 scripts/init_database.py --reset --test-user

if [ $? -eq 0 ]; then
    echo "Database setup complete!"
    echo ""
    echo "Test credentials:"
    echo "  SOEID: TEST999"
    echo "  Password: testpass123"
    echo "  Role: admin"
    echo ""
    echo "You can now run tests with: python3 -m pytest tests/"
else
    echo "Database setup failed!"
    exit 1
fi