from flask import Flask, jsonify, request
import sqlite3
import pandas as pd

import requests
import time

from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

import os
print("Flask DB path:", os.path.abspath("perak_flights.db"))

DB = "/app/perak_flights.db"

def fetch_real_departures_job():

    print("Running scheduled real departure fetch...")

    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flight_routes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            icao24 TEXT,
            flight_number TEXT,
            departure_airport TEXT,
            arrival_airport TEXT,
            departure_icao TEXT,
            arrival_icao TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()

    cursor.execute("""
        SELECT DISTINCT flight_number, icao24
        FROM flights
        WHERE flight_number IS NOT NULL
          AND TRIM(flight_number) != ''
        LIMIT 5
    """)

    flights = cursor.fetchall()

    for flight_number, icao24 in flights:

        flight_number = flight_number.strip()

        url = "https://api.aviationstack.com/v1/flights"
        params = {
            "access_key": AVIATIONSTACK_KEY,
            "flight_icao": flight_number
        }

        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            if "data" not in data or len(data["data"]) == 0:
                continue

            flight_data = data["data"][0]

            cursor.execute("""
                INSERT INTO flight_routes
                (icao24, flight_number, departure_airport,
                 arrival_airport, departure_icao,
                 arrival_icao, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                icao24,
                flight_number,
                flight_data["departure"]["airport"],
                flight_data["arrival"]["airport"],
                flight_data["departure"]["icao"],
                flight_data["arrival"]["icao"],
                time.strftime("%Y-%m-%d %H:%M:%S")
            ))

            time.sleep(1)

        except:
            continue

    conn.commit()
    conn.close()

def query_db(query):
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

from flask import render_template

@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/api/flights_per_hour")
def flights_per_hour():
    
    query = """
    SELECT strftime('%H:00', timestamp) as hour_only,
           COUNT(*) as count
    FROM flights
    GROUP BY hour_only
    ORDER BY hour_only
    """

    df = query_db(query)

    return jsonify({
        "hours": df["hour_only"].tolist(),
        "counts": df["count"].tolist()
    })

@app.route("/api/summary")
def summary():
    df = query_db("SELECT * FROM flights")

    total = len(df)
    avg_alt = df["baro_altitude"].mean()
    avg_vel = df["velocity"].mean()

    return jsonify({
        "total_flights": int(total),
        "average_altitude": round(df["baro_altitude"].mean(), 2) if not df.empty else 0,
        "average_velocity": round(df["velocity"].mean(), 2) if not df.empty else 0
    })

@app.route("/api/flights_per_day")
def flights_per_day():
    query = """
    SELECT date(timestamp) as day,
           COUNT(*) as count
    FROM flights
    GROUP BY day
    ORDER BY day
    """
    df = query_db(query)

    return jsonify({
        "days": df["day"].tolist(),
        "counts": df["count"].tolist()
    })

@app.route("/api/top_countries")
def top_countries():
    query = """
    SELECT origin_country, COUNT(*) as count
    FROM flights
    GROUP BY origin_country
    ORDER BY count DESC
    LIMIT 10
    """
    df = query_db(query)

    return jsonify({
        "countries": df["origin_country"].tolist(),
        "counts": df["count"].tolist()
    })

@app.route("/api/altitude_distribution")
def altitude_distribution():
    query = "SELECT baro_altitude FROM flights WHERE baro_altitude IS NOT NULL"
    df = query_db(query)

    bins = [0,1000,3000,6000,10000,15000,20000,30000]
    hist = pd.cut(df["baro_altitude"], bins).value_counts().sort_index()

    return jsonify({
        "ranges": [str(x) for x in hist.index],
        "counts": hist.tolist()
    })

@app.route("/api/velocity_distribution")
def velocity_distribution():
    query = "SELECT velocity FROM flights WHERE velocity IS NOT NULL"
    df = query_db(query)

    bins = [0,100,200,300,400,500,600]
    hist = pd.cut(df["velocity"], bins).value_counts().sort_index()

    return jsonify({
        "ranges": [str(x) for x in hist.index],
        "counts": hist.tolist()
    })

@app.route("/api/map_data")
def map_data():
    query = """
    SELECT latitude, longitude
    FROM flights
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
    ORDER BY timestamp DESC
    LIMIT 500
    """
    df = query_db(query)

    return jsonify(df.to_dict(orient="records"))

# your other routes above...







AVIATIONSTACK_KEY = os.getenv("AVIATIONSTACK_KEY")

@app.route("/api/fetch_real_departures")
def fetch_real_departures():

    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    # ✅ Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flight_routes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            icao24 TEXT,
            flight_number TEXT,
            departure_airport TEXT,
            arrival_airport TEXT,
            departure_icao TEXT,
            arrival_icao TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()

    # ✅ IMPORTANT: Fetch flights from your flights table
    cursor.execute("""
        SELECT DISTINCT flight_number, icao24
        FROM flights
        WHERE flight_number IS NOT NULL
          AND TRIM(flight_number) != ''
        LIMIT 10
    """)

    

    flights = cursor.fetchall()
    print("Flights fetched from DB:", flights)

    results = []

    for flight_number, icao24 in flights:

        flight_number = flight_number.strip()
        print("Calling AviationStack for:", flight_number)

        url = "https://api.aviationstack.com/v1/flights"
        params = {
            "access_key": AVIATIONSTACK_KEY,
            "flight_icao": flight_number   # using ICAO now
        }

        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            if "data" not in data or len(data["data"]) == 0:
                print("No data found for:", flight_number)
                continue

            flight_data = data["data"][0]

            departure_airport = flight_data["departure"]["airport"]
            arrival_airport = flight_data["arrival"]["airport"]
            departure_icao = flight_data["departure"]["icao"]
            arrival_icao = flight_data["arrival"]["icao"]

            # Save to DB
            cursor.execute("""
                INSERT INTO flight_routes
                (icao24, flight_number, departure_airport, arrival_airport,
                 departure_icao, arrival_icao, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                icao24,
                flight_number,
                departure_airport,
                arrival_airport,
                departure_icao,
                arrival_icao,
                time.strftime("%Y-%m-%d %H:%M:%S")
            ))

            results.append({
                "flight_number": flight_number,
                "departure": departure_airport,
                "arrival": arrival_airport
            })

            time.sleep(1)  # avoid rate limit

        except Exception as e:
            print("API Error:", e)
            continue

    conn.commit()
    conn.close()

    return jsonify(results)

@app.route("/api/real_departure_summary")
def real_departure_summary():

    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT origin_country, COUNT(*)
        FROM flights
        GROUP BY origin_country
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)

    data = cursor.fetchall()
    conn.close()

    return jsonify([
        {"airport": row[0], "count": row[1]}
        for row in data
    ])


@app.route("/api/ingest", methods=["POST"])
def ingest():
    data = request.json

    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO flights (
            timestamp, icao24, flight_number,
            origin_country, latitude, longitude,
            baro_altitude, velocity
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("timestamp"),
        data.get("icao24"),
        data.get("flight_number"),
        data.get("origin_country"),
        data.get("latitude"),
        data.get("longitude"),
        data.get("baro_altitude"),
        data.get("velocity")
    ))

    conn.commit()
    conn.close()

    return {"status": "success"}


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_real_departures_job, 'interval', minutes=5)
    scheduler.start()

    app.run(host="0.0.0.0", port=5000, debug=True)

