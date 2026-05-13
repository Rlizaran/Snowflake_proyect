import pandas as pd
import io
import requests

# 1. Load your station IDs (the file I generated for you)
# Or use your original 'Task Control_2026-05-13-1120.csv'
df_user = pd.read_csv('us_station_ids.csv')
target_ids = set(df_user['STATION_ID'].unique())

# 2. Download NOAA's Global Station Directory (GHCND)
print("Downloading official NOAA metadata...")
url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
r = requests.get(url)

# 3. Parse NOAA Fixed-Width Format
# Format: ID (11), Lat (9), Lon (10), Elev (7), State (3), Name (31)
col_specs = [(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71)]
names = ['station_id', 'lat', 'lng', 'elevation_m', 'state', 'station_name']

print("Processing metadata...")
df_noaa = pd.read_fwf(io.StringIO(r.text), colspecs=col_specs, names=names)

# 4. Filter for your US IDs
df_final = df_noaa[df_noaa['station_id'].isin(target_ids)].copy()

# 5. Clean and format
df_final['station_name'] = df_final['station_name'].str.strip()
df_final['state'] = df_final['state'].str.strip()

# Create a 'city' column by taking the first word of the station name 
# (Commonly the city/location in NOAA data)
df_final['city'] = df_final['station_name'].apply(lambda x: x.split(' ')[0].title())

# 6. Save as CSV for dbt Seed
df_final.to_csv('../seeds/slv_weather_station.csv', index=False)
print(f"Success! Created 'slv_weather_station.csv' with {len(df_final)} stations.")