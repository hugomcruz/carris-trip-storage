# Trip Storage System

This system extracts trip data from Redis and stores it in appropriate storage systems:
- Trip completion metadata → MySQL database
- Trip track details → Parquet files

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables in `.env` file:
   - Redis connection details
   - MySQL connection details
   - Output directory for Parquet files

3. Initialize MySQL database:
```bash
mysql -u root -p < schema.sql
```

## Usage

Run the main script to process trips:
```bash
python main.py
```

Or process a specific trip:
```bash
python main.py --trip-id <trip_id>
```

## Data Flow

1. **Redis `trip:{trip_id}:{start_date}:completion`**: Trip metadata is extracted and stored in PostgreSQL/MySQL
   - Keys now include start_date (format: YYYYMMDD) to distinguish trips across consecutive days
   - Example: `trip:21520:20250901:completion`
2. **Redis STREAM**: Trip track details are extracted and stored in Parquet files
   - Stream keys follow pattern: `trip:{trip_id}:{start_date}:track`

## Project Structure

- `main.py` - Main orchestration script
- `redis_client.py` - Redis connection and data retrieval
- `mysql_client.py` - MySQL connection and data storage
- `parquet_writer.py` - Parquet file writing utilities
- `schema.sql` - MySQL database schema
- `.env` - Environment configuration (not in git)
