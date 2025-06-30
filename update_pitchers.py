# update_pitchers.py  – four target buckets, 0-0 count only
from pybaseball import statcast_pitcher
import pandas as pd
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

PITCHERS = {
    "Zack_Wheeler": 554430,
    # "Aaron_Nola": 596966,
}

def bucket(row):
    """Return bucket label (4 possible) or None."""
    # keep only first pitch AND 0-0 count
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None

    # Batter 2 or Batter 3 in the 1st inning
    if row.inning == 1 and row.pa_of_inning == 2:
        return "Batter_2"
    if row.inning == 1 and row.pa_of_inning == 3:
        return "Batter_3"

    # Leadoff pitches of 2nd and 3rd innings
    if row.pa_of_inning == 1 and row.inning == 2:
        return "Inning_2_leadoff"
    if row.pa_of_inning == 1 and row.inning == 3:
        return "Inning_3_leadoff"

    return None


for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    df = statcast_pitcher(START, END, pid)
    print("downloaded rows:", len(df))

    if df.empty:
        continue

    # plate-appearance number within each half-inning
    df["pa_of_inning"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])
          .cumcount() + 1
    )

    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    print("kept after filter:", len(df))

    if df.empty:
        continue

    pitch_col = "pitch_name" if "pitch_name" in df.columns else "pitch_type"

    df_out = (
        df[["game_pk", "game_date", "bucket", pitch_col]]
          .rename(columns={pitch_col: "pitch_name"})
          .sort_values(["game_date", "game_pk"])
    )

    out_file = f"{name}_first_pitch.csv"
    df_out.to_csv(out_file, index=False)
    print(f"→ wrote {out_file}   ({len(df_out)} rows)")
