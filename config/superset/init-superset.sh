#!/bin/sh
set -e

echo "Initializing Apache Superset..."

# Install required packages
echo "Installing Python packages..."
pip install psycopg2-binary

# Upgrade Superset database
echo "Upgrading Superset database..."
superset db upgrade

# Create admin user
echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.com \
    --password admin

# Initialize Superset
echo "Initializing Superset..."
superset init

echo "Superset initialization completed successfully!"

# Start Superset webserver
echo "Starting Superset webserver..."
superset run -h 0.0.0.0 -p 8088