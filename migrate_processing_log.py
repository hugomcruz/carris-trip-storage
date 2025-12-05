#!/usr/bin/env python3
"""
Script to add start_date column to trip_processing_log table and create unique constraint.
"""

import os
from dotenv import load_dotenv
from postgres_client import PostgreSQLClient

load_dotenv()

def migrate_trip_processing_log():
    """Add start_date column and unique constraint to trip_processing_log table."""
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
        
        cursor = client.connection.cursor()
        
        # Check if column already exists
        print("\nChecking for start_date column in trip_processing_log...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='trip_processing_log' AND column_name='start_date'
        """)
        
        if cursor.fetchone():
            print("✓ Column 'start_date' already exists")
        else:
            # Add the column
            print("Adding start_date column to trip_processing_log...")
            cursor.execute("""
                ALTER TABLE trip_processing_log 
                ADD COLUMN start_date VARCHAR(8) NOT NULL DEFAULT '00000000'
            """)
            client.connection.commit()
            print("✓ Column 'start_date' added successfully")
        
        # Remove default value
        print("\nRemoving default value from start_date...")
        cursor.execute("""
            ALTER TABLE trip_processing_log 
            ALTER COLUMN start_date DROP DEFAULT
        """)
        client.connection.commit()
        print("✓ Default value removed")
        
        # Check if unique constraint exists
        print("\nChecking for unique constraint on (trip_id, start_date)...")
        cursor.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name='trip_processing_log' 
            AND constraint_type='UNIQUE'
        """)
        
        constraints = cursor.fetchall()
        if any(c for c in constraints):
            print("✓ Unique constraint already exists, recreating...")
            # Drop existing constraints
            for constraint in constraints:
                cursor.execute(f"ALTER TABLE trip_processing_log DROP CONSTRAINT IF EXISTS {constraint[0]}")
        
        # Create unique constraint
        print("Creating unique constraint on (trip_id, start_date)...")
        cursor.execute("""
            ALTER TABLE trip_processing_log 
            ADD CONSTRAINT trip_processing_log_trip_id_start_date_key 
            UNIQUE (trip_id, start_date)
        """)
        client.connection.commit()
        print("✓ Unique constraint created successfully")
        
        # Create index if it doesn't exist
        print("\nCreating index on start_date...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_processing_log_start_date 
            ON trip_processing_log(start_date)
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
    migrate_trip_processing_log()
