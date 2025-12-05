#!/usr/bin/env python3
"""
Script to add start_date column to trips table.
"""

import os
from dotenv import load_dotenv
from postgres_client import PostgreSQLClient

load_dotenv()

def add_start_date_column():
    """Add start_date column to trips table."""
    client = PostgreSQLClient(
        host=os.getenv('POSTGRES_HOST'),
        port=int(os.getenv('POSTGRES_PORT')),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        database=os.getenv('POSTGRES_DATABASE')
    )
    
    try:
        # Connect to database
        print("Connecting to PostgreSQL...")
        client.connect()
        print("✓ Connected successfully")
        
        # Add start_date column
        print("\nAdding start_date column to trips table...")
        cursor = client.connection.cursor()
        
        # Check if column already exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='trips' AND column_name='start_date'
        """)
        
        if cursor.fetchone():
            print("✓ Column 'start_date' already exists")
        else:
            # Add the column
            cursor.execute("""
                ALTER TABLE trips 
                ADD COLUMN start_date VARCHAR(8)
            """)
            client.connection.commit()
            print("✓ Column 'start_date' added successfully")
        
        # Create index if it doesn't exist
        print("\nCreating index on start_date...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_trips_start_date ON trips(start_date)
        """)
        client.connection.commit()
        print("✓ Index created successfully")
        
        cursor.close()
        print("\n✓ Migration completed successfully")
        
    except Exception as e:
        print(f"✗ Error during migration: {str(e)}")
        raise
    
    finally:
        client.disconnect()
        print("✓ Disconnected from PostgreSQL")

if __name__ == "__main__":
    add_start_date_column()
