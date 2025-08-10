import argparse
import requests
import pandas as pd
from utils.io import upsert_df

BASE = "https://ergast.com/api/f1"

# Helpers

def to_ms(time_str):
    if time_str is None:
        return None
    # handle mm:ss.xxx
    parts = time_str.split(":")
    if len(parts) == 2:
        m = int(parts[0])
        s, ms = parts[1].split(".")
        return (m*60 + int(s))*1000 + int(ms.ljust(3,'0')[:3])
    # handle ss.xxx
    s, ms = time_str.split(".")
    return int(s)*1000 + int(ms.ljust(3,'0')[:3]


def pull_static():
    # drivers
    drivers = []
    url = f"{BASE}/drivers.json?limit=2000"
    for d in requests.get(url).json()["MRData"]["DriverTable"]["Drivers"]:
        drivers.append({
            "driver_id": d["driverId"],
            "code": d.get("code"),
            "forename": d["givenName"],
            "surname": d["familyName"],
            "dob": d["dateOfBirth"],
            "nationality": d["nationality"],
        })
    upsert_df(pd.DataFrame(drivers), "drivers", pk_cols=["driver_id"])

    # constructors
    cons = []
    url = f"{BASE}/constructors.json?limit=2000"
    for c in requests.get(url).json()["MRData"]["ConstructorTable"]["Constructors"]:
        cons.append({
            "constructor_id": c["constructorId"],
            "name": c["name"],
            "nationality": c["nationality"],
        })
    upsert_df(pd.DataFrame(cons), "constructors", pk_cols=["constructor_id"])


def pull_season(season: int):
    # races
    races = []
    url = f"{BASE}/{season}.json?limit=200"
    data = requests.get(url).json()["MRData"]["RaceTable"]["Races"]
    for r in data:
        races.append({
            "race_id": f"{season}_{r['round']}",
            "season": season,
            "round": int(r["round"]),
            "circuit_id": r["Circuit"]["circuitId"],
            "name": r["raceName"],
            "date": r["date"],
            "time": r.get("time"),
        })
    upsert_df(pd.DataFrame(races), "races", pk_cols=["race_id"])

    # results per race
    results = []
    pitstops = []
    qualis = []
    for r in races:
        rid = r["race_id"]
        season = r["season"]
        round_ = r["round"]
        # results
        res = requests.get(f"{BASE}/{season}/{round_}/results.json?limit=100").json()
        for row in res["MRData"]["RaceTable"]["Races"][0]["Results"]:
            results.append({
                "result_id": f"{rid}_{row['Driver']['driverId']}",
                "race_id": rid,
                "driver_id": row["Driver"]["driverId"],
                "constructor_id": row["Constructor"]["constructorId"],
                "grid": int(row.get("grid", 0)),
                "position": int(row.get("position", 0)) if row.get("position") else None,
                "points": float(row.get("points", 0)),
                "status": row.get("status"),
                "fastest_lap": int(row.get("FastestLap", {}).get("lap", 0)) if row.get("FastestLap") else None,
                "time_ms": to_ms(row.get("Time", {}).get("time")) if row.get("Time") else None,
            })
        # pitstops
        pit = requests.get(f"{BASE}/{season}/{round_}/pitstops.json?limit=1000").json()
        prs = pit["MRData"]["RaceTable"]["Races"]
        if prs:
            for p in prs[0]["PitStops"]:
                pitstops.append({
                    "race_id": rid,
                    "driver_id": p["driverId"],
                    "stop": int(p["stop"]),
                    "lap": int(p["lap"]),
                    "duration_ms": to_ms(p.get("duration")),
                })
        # qualifying
        q = requests.get(f"{BASE}/{season}/{round_}/qualifying.json?limit=100").json()
        qr = q["MRData"]["RaceTable"]["Races"]
        if qr:
            for row in qr[0]["QualifyingResults"]:
                def ms(k):
                    return to_ms(row.get(k)) if row.get(k) else None
                qualis.append({
                    "race_id": rid,
                    "driver_id": row["Driver"]["driverId"],
                    "constructor_id": row["Constructor"]["constructorId"],
                    "q1_ms": ms("Q1"),
                    "q2_ms": ms("Q2"),
                    "q3_ms": ms("Q3"),
                    "position": int(row["position"]),
                })

    import pandas as pd
    if results:
        upsert_df(pd.DataFrame(results), "results", pk_cols=["result_id"])
    if pitstops:
        upsert_df(pd.DataFrame(pitstops), "pitstops")
    if qualis:
        upsert_df(pd.DataFrame(qualis), "qualifying")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-season", type=int, required=True)
    parser.add_argument("--to-season", type=int, required=True)
    args = parser.parse_args()

    pull_static()
    for season in range(args.from_season, args.to_season + 1):
        pull_season(season)
    print("Ingest complete.")