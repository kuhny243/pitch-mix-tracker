# update_pitchers.py
#
# Four buckets:
#   • Batter 2 – 1st inning
#   • Batter 3 – 1st inning
#   • Inning 2 leadoff
#   • Inning 3 leadoff
#
# Files created nightly per pitcher:
#   <Name>_first_pitch.csv           (raw rows)
#   <Name>_first_pitch_summary.csv   (count + pct by pitch type)

from pybaseball import statcast_pitcher
import pandas as pd
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

PITCHERS = {
    "Zack_Wheeler": 554430,
    # "Aaron_Nola": 596966,     # add more pitchers if desired
}


def add_pa_order(df: pd.DataFrame) -> pd.DataFrame:
    """Add pa_order = 1,2,3… within each half-inning (before filtering)."""
    df["pa_order"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense")
          .astype(int)
    )
    return df


def bucket(row) -> str | None:
    """Return bucket label or None (drop row)."""
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None

    # Batter 2 or 3 in 1st inning
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

    # 1️⃣  add plate-appearance order
    df = add_pa_order(df)

    # 2️⃣  apply bucket logic
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    print("kept after filter:", len(df))
    if df.empty:
        continue

    # 3️⃣  choose correct pitch-name column
    pitch_col = "pitch_name" if "pitch_name" in df.columns else "pitch_type"

    # 4️⃣  detailed CSV
    detail_cols = ["game_pk", "game_date", "bucket", pitch_col]
    df_out = (df[detail_cols]
              .rename(columns={pitch_col: "pitch_name"})
              .sort_values(["game_date", "game_pk"]))
    detail_file = f"{name}_first_pitch.csv"
    df_out.to_csv(detail_file, index=False)
    print(f"→ wrote {detail_file}  ({len(df_out)} rows)")

    # 5️⃣  summary CSV  (count + % inside each bucket)
    summary = (
        df_out.groupby(["bucket", "pitch_name"])
              .size()
              .to_frame("count")
              .groupby(level=0)
              .apply(lambda s: s.assign(pct=(s["count"]/s["count"].sum()*100)
                                        .round(1)))
              .reset_index()
    )
    summary_file = f"{name}_first_pitch_summary.csv"
    summary.to_csv(summary_file, index=False)
    print(f"→ wrote {summary_file}  ({len(summary)} rows)")
