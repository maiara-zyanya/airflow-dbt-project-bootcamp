from datetime import datetime, timezone

# Convert the Unix timestamp using the updated method with timezone awareness
unix_timestamp = 1737090000000
readable_date = datetime.fromtimestamp(unix_timestamp / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

print(readable_date)