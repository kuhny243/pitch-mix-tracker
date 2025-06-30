# update_pitchers.py
from pybaseball import statcast_pitcher
import pandas as pd
from datetime import date

START = "2025-03-01"                 # season start
END   = date.today().isoformat()     # auto-today

PITCHERS = {
    "Zack_Wheeler": 554430,
    "Aaron_Nola":   596966,
    "Spencer_Schreiter": 1234567,    # add as many as you like
}

def bucket(row):
    # keep 0-0 1st pitches only
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None
    # tag buckets
    if row.inning == 1:
        # bat_order 2 or 3 ( skipper uses 100+ for subs; keep only 2/3 )
        if row.bat_order in (2, 3):
            return f"Batter_{row.bat_order}"
    elif row.inning in (2, 3) and row.ab_number == 1:
        return f"Inning_{row.inning}_leadoff"
    return None

for name, pid in PITCHERS.items():
    df = statcast_pitcher(START, END, pid)
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    df[["game_pk","game_date","bucket","pitch_name"]].to_csv(
        f"{name}_first_pitch.csv", index=False
    )
