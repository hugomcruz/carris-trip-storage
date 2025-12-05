#!/bin/bash

# Setup script for trip-storage system

echo "🚀 Setting up Trip Storage System..."
echo ""

# Check Python version
echo "Checking Python version..."
python3 --version

# Create virtual environment
echo ""
echo "Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt

# Create output directory
echo ""
echo "Creating output directory..."
mkdir -p output/parquet

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Configure your .env file with Redis and MySQL credentials"
echo "2. Create MySQL database: mysql -u root -p < schema.sql"
echo "3. Run: source venv/bin/activate"
echo "4. Process trips: python main.py --all"
echo ""
echo "For help: python main.py --help"
