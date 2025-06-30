# update_pitchers.py  – four buckets + summary (fixed)
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
    """Add pa_order (1,2,3…) within each half-inning from at_bat_number."""
    df["pa_order"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense")
          .astype(int)
    )
    return df

def bucket(row) -> str | None:
    """Return one of the four target buckets or None."""
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None
    if row.inning == 1 and row.pa_order in (2, 3):
        return f"Batter_{row.pa_order}"
    if row.pa_order == 1 and row.inning in (2, 3):
        return f"Inning_{int(row.inning)}_leadoff"
    return None


for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    df = statcast_pitcher(START, END, pid)
   # keep only regular-season games (game_type == "R")
    df = df[df.game_type == "R"]
    print("downloaded rows:", len(df))
    if df.empty:
        continue

    df = add_pa_order(df)
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

    # detailed CSV
    detail_file = f"{name}_first_pitch.csv"
    df_out.to_csv(detail_file, index=False)
    print(f"→ wrote {detail_file}  ({len(df_out)} rows)")

    # summary CSV (count + pct within each bucket)
    summary = (
        df_out.groupby(["bucket", "pitch_name"])
              .size()
              .reset_index(name="count")
    )
    summary["pct"] = (
        summary.groupby("bucket")["count"]
               .transform(lambda x: (x / x.sum() * 100).round(1))
    )

    summary_file = f"{name}_first_pitch_summary.csv"
    summary.to_csv(summary_file, index=False)
    print(f"→ wrote {summary_file}  ({len(summary)} rows)")
