# update_pitchers.py  – 19-pitcher starter list
from pybaseball import statcast_pitcher, playerid_lookup
import pandas as pd
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

# Paste names (one per line).  Zack Wheeler is skipped automatically.
NAME_LIST = """
Colin Rea
Paul Skenes
Seth Lugo
Simeon Woods Richardson
Ben Lively
Zack Wheeler
David Peterson
Tyler Anderson
Bryan Bello
Andrew Abbott
Sonny Gray
Jose Berrios
Mitch Keller
Ben Brown
Kyle Freeland
Framber Valdez
Jose Soriano
Bryan Woo
Brandon Pfaadt
Kevin Gausman
""".strip().splitlines()

# ── Clean list ──────────────────────────────────────────────────────────
clean_names = {n.strip() for n in NAME_LIST if n.strip() and n.strip() != "Zack Wheeler"}

# ── Resolve each name → MLBAM ID ────────────────────────────────────────
def resolve_ids(names):
    mapping = {}
    for full_name in sorted(names):
        try:
            last, first = full_name.split()[-1], full_name.split()[0]
            lookup = playerid_lookup(last, first)
            if not lookup.empty:
                pid = int(lookup.key_mlbam.values[0])
                mapping[full_name.replace(" ", "_")] = pid
                print(f"Resolved {full_name} → {pid}")
            else:
                print(f"⚠️  No ID found for {full_name}")
        except Exception as e:
            print(f"⚠️  Lookup failed for {full_name}: {e}")
    return mapping

PITCHERS = resolve_ids(clean_names)
print(f"Tracking {len(PITCHERS)} pitchers")

# ── Helper functions (unchanged) ────────────────────────────────────────
def add_pa_order(df: pd.DataFrame) -> pd.DataFrame:
    df["pa_order"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense")
          .astype(int)
    )
    return df

def bucket(row) -> str | None:
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None
    if row.inning == 1 and row.pa_order in (2, 3):
        return f"Batter_{row.pa_order}"
    if row.pa_order == 1 and row.inning in (2, 3):
        return f"Inning_{int(row.inning)}_leadoff"
    return None

# ── Main loop ───────────────────────────────────────────────────────────
for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    df = statcast_pitcher(START, END, pid)

    # regular-season only
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

    # detailed file
    df_out.to_csv(f"{name}_first_pitch.csv", index=False)

    # summary file (count + %)
    summary = (
        df_out.groupby(["bucket", "pitch_name"])
              .size()
              .reset_index(name="count")
    )
    summary["pct"] = (
        summary.groupby("bucket")["count"]
               .transform(lambda x: (x / x.sum() * 100).round(1))
    )
    summary.to_csv(f"{name}_first_pitch_summary.csv", index=False)
