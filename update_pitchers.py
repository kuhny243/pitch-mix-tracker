# update_pitchers.py
#
# Pulls Statcast pitch-by-pitch data for any pitchers you list,
# keeps only 0-0 count, pitch #1 events,
# tags buckets:
#   • Batter 2 and Batter 3 in the 1st inning  (skips Batter 1)
#   • Leadoff pitch of the 2nd and 3rd innings
# Writes one CSV per pitcher:  <Name>_first_pitch.csv

from pybaseball import statcast_pitcher
import pandas as pd
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

PITCHERS = {
    "Zack_Wheeler": 554430,
    # add more:  "Aaron_Nola": 596966,
}

def bucket(row) -> str | None:
    """Return bucket label or None if row should be dropped."""
    # keep only first pitch, 0-0 count
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None

    # --- 1st-inning buckets (skip first batter) ----------------------------
    bat_order = getattr(row, "batting_order", getattr(row, "bat_order", None))
    if row.inning == 1 and bat_order in (2, 3):
        return f"Batter_{bat_order}"

    # --- 2nd / 3rd inning leadoff buckets ----------------------------------
    if row.inning in (2, 3) and row.at_bat_number == 1:
        return f"Inning_{int(row.inning)}_leadoff"

    return None


for name, pid in PITCHERS.items():
    print(f"Gathering Statcast for {name} ({pid}) …")
    df = statcast_pitcher(START, END, pid)        # pulls entire season once

    # Tag buckets & drop everything else
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])

    # Pick whichever pitch-name column exists (pybaseball API changed in 2024)
    pitch_col = "pitch_name" if "pitch_name" in df.columns else "pitch_type"

    # Keep only useful columns & save
    df_out = df[["game_pk", "game_date", "bucket", pitch_col]] \
               .rename(columns={pitch_col: "pitch_name"}) \
               .sort_values(["game_date", "game_pk"])

    out_file = f"{name}_first_pitch.csv"
    df_out.to_csv(out_file, index=False)
    print(f"  → wrote {out_file}  ({len(df_out)} rows)")
