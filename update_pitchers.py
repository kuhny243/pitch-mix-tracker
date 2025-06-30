# update_pitchers.py  – four buckets, accurate PA order
from pybaseball import statcast_pitcher
import pandas as pd
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

PITCHERS = {
    "Zack_Wheeler": 554430,
    # "Aaron_Nola": 596966,
}

def add_pa_order(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add pa_order = 1,2,3… (plate-appearance order in each half-inning)
    using at_bat_number BEFORE we drop any rows.
    """
    df["pa_order"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense")
          .astype(int)
    )
    return df

def bucket(row):
    # keep only first pitch AND 0-0 count
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None

    # Batter 2 or Batter 3 (1st inning)
    if row.inning == 1 and row.pa_order in (2, 3):
        return f"Batter_{row.pa_order}"

    # Leadoff pitch of 2nd or 3rd inning
    if row.pa_order == 1 and row.inning in (2, 3):
        return f"Inning_{int(row.inning)}_leadoff"

    return None


for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    df = statcast_pitcher(START, END, pid)
    print("downloaded rows:", len(df))

    if df.empty:
        continue

    # 1️⃣  Add plate-appearance order
    df = add_pa_order(df)

    # 2️⃣  Apply bucket logic
    df["bucket"] = df.apply(bucket, axi
