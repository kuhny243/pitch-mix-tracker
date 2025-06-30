# update_pitchers.py  – starter = faced first PA of his half-inning
from pybaseball import statcast_pitcher, playerid_lookup
import pandas as pd
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

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

clean_names = {n.strip() for n in NAME_LIST if n.strip() != "Zack Wheeler"}

def resolve_ids(names):
    mapping = {}
    for full in sorted(names):
        last, first = full.split()[-1], full.split()[0]
        lk = playerid_lookup(last, first)
        if not lk.empty:
            mapping[full.replace(" ", "_")] = int(lk.key_mlbam.iloc[0])
    return mapping

PITCHERS = resolve_ids(clean_names)
print(f"Tracking {len(PITCHERS)} pitchers")

# ── helpers ─────────────────────────────────────────────────────────────
def add_pa_order(df):
    df["pa_order"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense").astype(int)
    )
    return df

def bucket(row):
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None
    if row.inning == 1 and row.pa_order in (2, 3):
        return f"Batter_{row.pa_order}"
    if row.pa_order == 1 and row.inning in (2, 3):
        return f"Inning_{int(row.inning)}_leadoff"
    return None

# ── main loop ───────────────────────────────────────────────────────────
for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    df = statcast_pitcher(START, END, pid)

    # regular season only
    df = df[df.game_type == "R"]

    # add plate-appearance order BEFORE start filter
    df = add_pa_order(df)

    # starter = pitched inning 1 AND pa_order 1 (first PA of his half)
    starter_games = df.loc[(df.inning == 1) & (df.pa_order == 1), "game_pk"].unique()
    df = df[df.game_pk.isin(starter_games)]

    print("rows after starter filter:", len(df))
    if df.empty:
        continue

    # apply bucket logic
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    print("kept after bucket filter:", len(df))
    if df.empty:
        continue

    pitch_col = "pitch_name" if "pitch_name" in df.columns else "pitch_type"
    df_out = (
        df[["game_pk", "game_date", "bucket", pitch_col]]
          .rename(columns={pitch_col: "pitch_name"})
          .sort_values(["game_date", "game_pk"])
    )

    # detailed CSV
    df_out.to_csv(f"{name}_first_pitch.csv", index=False)

    # summary CSV (count + pct)
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
