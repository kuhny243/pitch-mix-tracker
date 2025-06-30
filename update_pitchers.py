# update_pitchers.py
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
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None
    if row.inning == 1 and row.pa_of_inning in (2, 3):
        return f"Batter_{row.pa_of_inning}"
    if row.inning in (2, 3) and row.pa_of_inning == 1:
        return f"Inning_{int(row.inning)}_leadoff"
    return None

for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    df = statcast_pitcher(START, END, pid)       # ← removed verbose=False
    print("downloaded rows:", len(df))

    if df.empty:
        continue

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
