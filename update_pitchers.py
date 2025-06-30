# update_pitchers.py
#
# Pulls Statcast data for the pitchers you list (0-0 count, pitch #1 only),
# keeps:
#   • Batter 2 and Batter 3 of the 1st inning   (skips Batter 1)
#   • Leadoff pitch of the 2nd and 3rd innings
# Writes one CSV per pitcher:  <Name>_first_pitch.csv
#
# Run nightly via GitHub Actions (see update.yml).

from pybaseball import statcast_pitcher
import pandas as pd
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

PITCHERS = {
    "Zack_Wheeler": 554430,
    # "Aaron_Nola":   596966,      # ← add more pitchers here if you like
}

def bucket(row) -> str | None:
    """Return bucket label or None if row should be dropped."""
    # keep only first-pitch, 0-0 count
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None

    # Batter 2 or 3 in the 1st inning
    if row.inning == 1 and row.pa_of_inning in (2, 3):
        return f"Batter_{row.pa_of_inning}"

    # Leadoff pitch of 2nd or 3rd inning
    if row.inning in (2, 3) and row.pa_of_inning == 1:
        return f"Inning_{int(row.inning)}_leadoff"

    return None


for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    # 1️⃣  Download entire season for that pitcher
    df = statcast_pitcher(START, END, pid, verbose=False)
    print("downloaded rows:", len(df))

    if df.empty:
        print("No data downloaded — check pitcher ID or date range.")
        continue

    # 2️⃣  Derive plate-appearance number within each half-inning
    df["pa_of_inning"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])
          .cumcount() + 1
    )

    # 3️⃣  Apply bucket filter
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    print("kept after filter:", len(df))

    if df.empty:
        print("⚠️  No rows matched filter — check bucket logic.")
        continue

    # 4️⃣  Pick pitch-name column (schema changed in 2024)
    pitch_col = "pitch_name" if "pitch_name" in df.columns else "pitch_type"

    # 5️⃣  Write output CSV
    out_cols = ["game_pk", "game_date", "bucket", pitch_col]
    df_out = (
        df[out_cols]
        .rename(columns={pitch_col: "pitch_name"})
        .sort_values(["game_date", "game_pk"])
    )

    out_file = f"{name}_first_pitch.csv"
    df_out.to_csv(out_file, index=False)
    print(f"→ wrote {out_file}   ({len(df_out)} rows)")
