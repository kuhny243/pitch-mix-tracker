# update_pitchers.py  – 0-0 first pitches, four precise buckets
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
    Adds pa_order = 1,2,3… (plate-appearance order within each half-inning)
    computed from at_bat_number BEFORE any rows are dropped.
    """
    df["pa_order"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense")
          .astype(int)
    )
    return df

def bucket(row) -> str | None:
    """Return one of the four target bucket labels or None."""
    # Keep only first pitch AND 0-0 count
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None

    # Batter 2 or 3 of the 1st inning
    if row.inning == 1 and row.pa_order in (2, 3):
        return f"Batter_{row.pa_order}"

    # Leadoff pitches of 2nd and 3rd innings
    if row.pa_order == 1 and row.inning in (2, 3):
        return f"Inning_{int(row.inning)}_leadoff"

    return None


for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    df = statcast_pitcher(START, END, pid)
    print("downloaded rows:", len(df))

    if df.empty:
        continue

    # 1️⃣  Add plate-appearance order per half-inning
    df = add_pa_order(df)

    # 2️⃣  Apply bucket logic
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    print("kept after filter:", len(df))

    if df.empty:
        continue

    # 3️⃣  Choose correct pitch-name column (new vs old schema)
    pitch_col = "pitch_name" if "pitch_name" in df.columns else "pitch_type"

    df_out = (
        df[["game_pk", "game_date", "bucket", pitch_col]]
          .rename(columns={pitch_col: "pitch_name"})
          .sort_values(["game_date", "game_pk"])
    )

    out_file = f"{name}_first_pitch.csv"
    df_out.to_csv(out_file, index=False)
    print(f"→ wrote {out_file}   ({len(df_out)} rows)")
