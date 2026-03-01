import requests
import pandas as pd
import time
from datetime import datetime

# =========================
# PERAK AREA COORDINATES
# =========================
lat_min = 3.5
lat_max = 5.8
lon_min = 100.3
lon_max = 101.5

url_data = (
    "https://opensky-network.org/api/states/all?"
    f"lamin={lat_min}&lomin={lon_min}&lamax={lat_max}&lomax={lon_max}"
)

FLASK_URL = "http://api:5000/api/ingest"

print("Local Flight Data Collection Started...\n")

while True:
    try:
        r = requests.get(url_data, timeout=30)
        print("OpenSky Status:", r.status_code)

        response = r.json()

        if response.get("states") is None:
            print("⚠ No data from OpenSky")
            time.sleep(10)
            continue

        print("Flights received:", len(response["states"]))

        col_name = [
            'icao24','flight_number','origin_country','time_position',
            'last_contact','longitude','latitude','baro_altitude',
            'on_ground','velocity','true_track','vertical_rate',
            'sensors','geo_altitude','squawk','spi','position_source'
        ]

        flight_df = pd.DataFrame(response["states"], columns=col_name)

        flight_df = flight_df[[
            'icao24','flight_number','origin_country',
            'latitude','longitude','baro_altitude','velocity'
        ]]

        flight_df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        flight_df = flight_df.dropna(subset=["latitude", "longitude"])

        if flight_df.empty:
            print("⚠ No valid coordinates found")
            time.sleep(10)
            continue

        print(f" Sending {len(flight_df)} records...")

        for _, row in flight_df.iterrows():

            payload = {
                "timestamp": row["timestamp"],
                "icao24": row["icao24"],
                "flight_number": row["flight_number"],
                "origin_country": row["origin_country"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "baro_altitude": float(row["baro_altitude"]) if pd.notna(row["baro_altitude"]) else None,
                "velocity": float(row["velocity"]) if pd.notna(row["velocity"]) else None
            }

            try:
                res = requests.post(FLASK_URL, json=payload, timeout=5)
                print("POST:", res.status_code)
            except Exception as e:
                print("POST Error:", e)

    except Exception as e:
        print("OpenSky Error:", e)

    print(" Waiting...\n")
    time.sleep(600)

    
