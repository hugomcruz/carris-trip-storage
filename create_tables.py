#!/usr/bin/env python3
"""
Script to create PostgreSQL tables for trip storage.
"""

import os
from dotenv import load_dotenv
from postgres_client import PostgreSQLClient

load_dotenv()

def create_tables():
    """Create PostgreSQL tables using the schema file."""
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
        
        # Read and execute schema file
        print("\nCreating tables from schema...")
        schema_file = os.path.join(os.path.dirname(__file__), 'schema_postgres.sql')
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Execute the entire schema as one script
        # This handles triggers and functions correctly
        cursor = client.connection.cursor()
        cursor.execute(schema_sql)
        client.connection.commit()
        cursor.close()
        print("✓ All statements executed successfully")
        
        print("\n✓ Tables created successfully")
        
    except Exception as e:
        print(f"✗ Error creating tables: {str(e)}")
        raise
    
    finally:
        client.disconnect()
        print("✓ Disconnected from PostgreSQL")

if __name__ == "__main__":
    create_tables()
