# update_pitchers.py
# – 118 pitchers • starter-only • FIVE buckets (new Leadoff_2nd_PA) • summary • auto-retry –

from pybaseball import statcast_pitcher, playerid_lookup
import pandas as pd, time
from pandas.errors import ParserError, EmptyDataError
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

# 1 ── Master name list (duplicates OK) ──────────────────────────────────
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
Tanner Bibee
Brady Singer
Carlos Rodon
Bowden Francis
German Marquez
Luis Severino
Trevor Williams
Matthew Boyd
Hunter Greene
Griffin Canning
Shane Smith
Landon Roupp
Freddy Peralta
Drew Rasmussen
Pablo Lopez
Zac Gallen
Cade Povich
Miles Mikolas
AJ Smith-Shawver
Tyler Mahle
Cristopher Sanchez
Chris Sale
Shane Baz
Emerson Hancock
MacKenzie Gore
Zach Eflin
Sean Burke
Taijuan Walker
Chris Bassitt
Jeffrey Springs
Luis Ortiz
Cal Quantrill
Grant Holmes
Jack Leiter
Matthew Liberatore
Casey Mize
Ryan Pepiot
Nick Pivetta
Merrill Kelly
Jake Irvin
Tony Gonsolin
Max Fried
Nick Lodolo
Dean Kremer
Jesus Luzardo
Patrick Corbin
Kyle Hendricks
Spencer Schwellenbach
Walker Buehler
Kodai Senga
Antonio Senzatela
Edward Cabrera
Robbie Ray
Michael Wacha
Tarik Skubal
Zack Littell
Bryce Miller
Landon Knack
Will Warren
Dylan Cease
Bailey Falter
Bailey Ober
Jacob DeGrom
Bryce Elder
Garrett Crochet
JP Sears
Gavin Williams
Jack Kochanowicz
Clay Holmes
Kris Bubic
Hunter Brown
Jameson Taillon
Yoshinobu Yamamoto
Max Meyer
Logan Webb
Joe Ryan
Dustin May
Mitchell Parker
Charlie Morton
Nick Martinez
Taj Bradley
Chris Paddack
Luis Castillo
Tomoyuki Sugano
Chase Dollander
Tylor Megill
Michael Lorenzen
Jose Quintana
Andre Pallante
Hunter Dobbins
Sandy Alcantara
Lucas Giolito
Ranger Suarez
Logan Allen
Emmet Sheehan
Stephen Kolek
Eduardo Rodriguez
Eric Lauer
Spencer Strider
Andrew Heaney
Justin Verlander
Shota Imanaga
""".strip().splitlines()

# 2 ── Deduplicate & strip blanks ───────────────────────────────────────
clean_names = {n.strip() for n in NAME_LIST if n.strip()}

# 3 ── Resolve names → MLB IDs ──────────────────────────────────────────
def resolve_ids(names):
    mapping = {}
    for full in sorted(names):
        try:
            last, first = full.split()[-1], full.split()[0]
            lk = playerid_lookup(last, first)
            if not lk.empty and lk.key_mlbam.iloc[0]:
                pid = int(lk.key_mlbam.iloc[0])
                mapping[full.replace(" ", "_")] = pid
                print(f"Resolved {full:<25} → {pid}")
            else:
                print(f"⚠️  No ID for {full}")
        except Exception as e:
            print(f"⚠️  Lookup failed for {full}: {e}")
    return mapping

PITCHERS = resolve_ids(clean_names)
print(f"\nTracking {len(PITCHERS)} unique pitchers")

# 4 ── Helper functions ────────────────────────────────────────────────
def add_pa_order(df: pd.DataFrame) -> pd.DataFrame:
    """Plate-appearance order within each half-inning."""
    df["pa_order"] = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense").astype(int)
    )
    return df

def add_leadoff_seq(df: pd.DataFrame) -> pd.DataFrame:
    """
    leadoff_seq:
       −1  → not lineup spot-1 PA
        0  → first time spot-1 bats
        1  → second time spot-1 bats  (target bucket)
        2… → third, fourth, etc.
    """
    order_col = "batting_order" if "batting_order" in df.columns else "bat_order"
    df["leadoff_seq"] = -1
    mask = df[order_col] == 1
    df.loc[mask, "leadoff_seq"] = df.loc[mask].groupby("game_pk").cumcount()
    return df

def bucket(row) -> str | None:
    """Return one of FIVE bucket labels or None."""
    if not (row.pitch_number == 1 and row.balls == 0 and row.strikes == 0):
        return None
    # original four buckets
    if row.inning == 1 and row.pa_order in (2, 3):
        return f"Batter_{row.pa_order}"
    if row.pa_order == 1 and row.inning in (2, 3):
        return f"Inning_{int(row.inning)}_leadoff"
    # new bucket: second PA for lineup spot-1
    if row.leadoff_seq == 1:
        return "Leadoff_2nd_PA"
    return None

# 5 ── Main loop with retry ─────────────────────────────────────────────
for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")

    # 5-A  download with one retry
    for attempt in (1, 2):
        try:
            df = statcast_pitcher(START, END, pid)
            break
        except (ParserError, EmptyDataError) as e:
            print(f"⚠️  Statcast parse error (attempt {attempt}) – {e}")
            if attempt == 2:
                print("   Skipping pitcher.\n")
                df = pd.DataFrame()
            else:
                time.sleep(3)
    if df.empty:
        continue

    # 5-B  filters & derived columns
    df = df[df.game_type == "R"]      # regular season only
    df = add_pa_order(df)
    df = add_leadoff_seq(df)

    # starter filter: faced first PA of own half-inning
    starter_games = df.loc[(df.inning == 1) & (df.pa_order == 1), "game_pk"].unique()
    df = df[df.game_pk.isin(starter_games)]
    print("rows after starter filter:", len(df))
    if df.empty:
        continue

    # 5-C  bucket logic
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    print("kept after bucket filter:", len(df))
    if df.empty:
        continue

    # 5-D  write detailed + summary CSVs
    pitch_col = "pitch_name" if "pitch_name" in df.columns else "pitch_type"
    df_out = (
        df[["game_pk", "game_date", "bucket", pitch_col]]
          .rename(columns={pitch_col: "pitch_name"})
          .sort_values(["game_date", "game_pk"])
    )
    df_out.to_csv(f"{name}_first_pitch.csv", index=False)

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
