# Redis Key Structure Migration Guide

## Overview
The Redis key structure has been updated to include the trip `start_date` to distinguish trips across consecutive days.

## Key Changes

### Old Format
```
trip:{trip_id}:completion
trip:{trip_id}:track
```

### New Format
```
trip:{trip_id}:{start_date}:completion
trip:{trip_id}:{start_date}:track
```

Where `start_date` is in the format `YYYYMMDD` (e.g., `20250901`).

### Example
- **Old**: `trip:21520:completion`
- **New**: `trip:21520:20250901:completion`

## Modified Files

### 1. `redis_client.py`
- **`get_trip_completion_keys()`**: Updated default pattern to `"trip:*:*:completion"`
- **`get_trip_completion_data(trip_id, start_date)`**: Now **requires** `start_date` parameter
- **`find_trip_stream(trip_id, start_date)`**: Now **requires** `start_date` parameter
- **`delete_trip_data(trip_id, start_date)`**: Now **requires** `start_date` parameter
- **`extract_trip_id_from_key(key)`**: Now returns tuple `(trip_id, start_date)` instead of just `trip_id`
  - **Only supports new format** - returns `None` for old format keys

### 2. `main.py`
- **`process_trip(trip_id, start_date)`**: `start_date` parameter is now **required**
- **`process_all_trips()`**: Updated to extract and use `(trip_id, start_date)` tuples
- **`list_trips_in_redis()`**: Returns list of tuples `[(trip_id, start_date), ...]`
- **`--list` command**: Now displays both trip_id and start_date

### 3. `parquet_writer.py`
- **`write_trip_track_data(trip_id, track_data, start_date)`**: `start_date` parameter is now **required**
  - Filename format: `trip_{trip_id}_{start_date}_{timestamp}.parquet`

### 4. Example and Test Files
- **`test_one_trip.py`**: Updated to use explicit `start_date` variable
- **`explore_redis.py`**: Updated to handle tuple returns and display start_date
- **`examples.py`**: Updated all examples to extract and use `(trip_id, start_date)` tuples

### 5. `README.md`
- Updated Data Flow section to document new key structure

## Backward Compatibility

**⚠️ NO BACKWARD COMPATIBILITY**: This implementation does **NOT** support the old key format.

- Old format keys (`trip:{trip_id}:completion`) will **NOT** be recognized
- `extract_trip_id_from_key()` returns `None` for old format keys
- All Redis keys **MUST** follow the new format: `trip:{trip_id}:{start_date}:completion`

## Migration Notes

### For Existing Data
**IMPORTANT**: If you have existing Redis keys in the old format (`trip:{trip_id}:completion`), you **MUST** migrate them before using this system.

Migration options:
1. **Rename keys** to include start_date extracted from completion data
2. **Recreate keys** with the new format
3. **Process old data separately** before migrating to the new format

Example migration script concept:
```python
# Pseudocode - adapt as needed
old_keys = redis.keys("trip:*:completion")
for old_key in old_keys:
    # Extract trip data
    data = redis.get(old_key)
    trip_id = old_key.split(':')[1]
    
    # Extract start_date from data (assuming start_time field exists)
    start_date = extract_date_from_timestamp(data['start_time'])  # YYYYMMDD
    
    # Create new key
    new_key = f"trip:{trip_id}:{start_date}:completion"
    redis.set(new_key, data)
    
    # Delete old key
    redis.delete(old_key)
```

### For New Implementations
Always create keys with the new format including `start_date`:
```python
# Python example
trip_id = "21520"
start_date = "20250901"
key = f"trip:{trip_id}:{start_date}:completion"
```

### Redis Key Creation Example
```bash
# Redis CLI example
SET "trip:21520:20250901:completion" '{"trip_id": "21520", "start_time": "2025-09-01T10:30:00"}'
XADD "trip:21520:20250901:track" * lat 38.7223 lon -9.1393
```

## Testing

Before deploying to production:
1. Test with existing data using `python explore_redis.py`
2. Verify key parsing with `python main.py --list`
3. Process a single trip: `python main.py --trip-id <trip_id>`
4. Check Parquet filenames include start_date

## Support

If you encounter issues:
1. Check Redis keys format: `KEYS trip:*:*:completion`
2. Verify completion data includes `start_time` field
3. Review `trip_processing.log` for detailed error messages
