# update_pitchers.py
# 118 pitchers • starter-only • FIVE buckets (0-0) • auto-retry

from pybaseball import statcast_pitcher
import pandas as pd, time
from pandas.errors import ParserError, EmptyDataError
from datetime import date

START = "2025-03-01"
END   = date.today().isoformat()

# ── 1. 118 Pitchers → MLBAM IDs ────────────────────────────────────────
PITCHERS = {
    "Colin_Rea": 607067,  "Paul_Skenes": 694973,  "Seth_Lugo": 607625,
    "Simeon_Woods_Richardson": 677943,  "Ben_Lively": 594902,
    "Zack_Wheeler": 554430,  "David_Peterson": 656849,
    "Tyler_Anderson": 542881,  "Brayan_Bello": 686880,
    "Andrew_Abbott": 671096,  "Sonny_Gray": 543243,  "Jose_Berrios": 621244,
    "Mitch_Keller": 656605,  "Ben_Brown": 676962,  "Kyle_Freeland": 607536,
    "Framber_Valdez": 664285,  "Jose_Soriano": 660670,  "Bryan_Woo": 693433,
    "Brandon_Pfaadt": 694297,  "Kevin_Gausman": 592332,  "Tanner_Bibee": 676440,
    "Brady_Singer": 663903,  "Carlos_Rodon": 592682,  "Bowden_Francis": 670102,
    "German_Marquez": 606466,  "Luis_Severino": 622663,
    "Trevor_Williams": 592866,  "Matthew_Boyd": 571440,  "Hunter_Greene": 668881,
    "Griffin_Canning": 656288,  "Shane_Smith": 681343,  "Landon_Roupp": 677974,
    "Freddy_Peralta": 642547,  "Drew_Rasmussen": 656876,
    "Pablo_Lopez": 642456,  "Zac_Gallen": 668678,  "Cade_Povich": 700249,
    "Miles_Mikolas": 571945,  "AJ_Smith_Shawver": 700363,  "Tyler_Mahle": 641816,
    "Cristopher_Sanchez": 665742,  "Chris_Sale": 519242,  "Shane_Baz": 669358,
    "Emerson_Hancock": 676106,  "MacKenzie_Gore": 669022,  "Zach_Eflin": 621107,
    "Sean_Burke": 680732,  "Taijuan_Walker": 592836,  "Chris_Bassitt": 605135,
    "Jeffrey_Springs": 605488,  "Luis_Ortiz": 656814,  "Cal_Quantrill": 615698,
    "Grant_Holmes": 656550,  "Jack_Leiter": 683004,
    "Matthew_Liberatore": 669461,  "Casey_Mize": 663554,
    "Ryan_Pepiot": 686752,  "Nick_Pivetta": 601713,  "Merrill_Kelly": 518876,
    "Jake_Irvin": 663623,  "Tony_Gonsolin": 664062,  "Max_Fried": 608331,
    "Nick_Lodolo": 666157,  "Dean_Kremer": 665152,  "Jesus_Luzardo": 666200,
    "Patrick_Corbin": 571578,  "Kyle_Hendricks": 543294,
    "Spencer_Schwellenbach": 680885,  "Walker_Buehler": 621111,
    "Kodai_Senga": 673540,  "Antonio_Senzatela": 622608,
    "Edward_Cabrera": 665795,  "Robbie_Ray": 592662,  "Michael_Wacha": 608379,
    "Tarik_Skubal": 669373,  "Zack_Littell": 641793,  "Bryce_Miller": 682243,
    "Landon_Knack": 689017,  "Will_Warren": 701542,  "Dylan_Cease": 656302,
    "Bailey_Falter": 663559,  "Bailey_Ober": 641927,  "Jacob_DeGrom": 594798,
    "Bryce_Elder": 693821,  "Garrett_Crochet": 676979,  "JP_Sears": 676664,
    "Gavin_Williams": 668909,  "Jack_Kochanowicz": 686799,  "Clay_Holmes": 605280,
    "Kris_Bubic": 663460,  "Hunter_Brown": 686613,  "Jameson_Taillon": 592791,
    "Yoshinobu_Yamamoto": 808967,  "Max_Meyer": 676974,  "Logan_Webb": 657277,
    "Joe_Ryan": 657746,  "Dustin_May": 669160,  "Mitchell_Parker": 680730,
    "Charlie_Morton": 119424,  "Nick_Martinez": 607212,  "Taj_Bradley": 671737,
    "Chris_Paddack": 663978,  "Luis_Castillo": 664057,  "Tomoyuki_Sugano": 608372,
    "Chase_Dollander": 801403,  "Tylor_Megill": 656731,
    "Michael_Lorenzen": 547179,  "Jose_Quintana": 500779,
    "Andre_Pallante": 669467,  "Hunter_Dobbins": 690928,
    "Sandy_Alcantara": 645261,  "Lucas_Giolito": 608337,
    "Ranger_Suarez": 664561,  "Logan_Allen": 671106,  "Emmet_Sheehan": 686218,
    "Stephen_Kolek": 663568,  "Eduardo_Rodriguez": 571561,  "Eric_Lauer": 641778,
    "Spencer_Strider": 675911,  "Andrew_Heaney": 571760,
    "Justin_Verlander": 434378,  "Shota_Imanaga": 684007,
}

# ── 2. Helpers ─────────────────────────────────────────────────────────
def add_pa_order(df: pd.DataFrame) -> pd.DataFrame:
    df["pa_order"] = (
        df.sort_values("at_bat_number")
          .groupby(["game_pk", "inning", "inning_topbot"])["at_bat_number"]
          .rank(method="dense").astype(int)
    )
    return df

def mark_first_pitch(df: pd.DataFrame) -> pd.DataFrame:
    pc = "pitch_name" if "pitch_name" in df.columns else "pitch_type"
    df["first_pitch"] = False
    idx = (df[df[pc].notna()]
           .sort_values(["game_pk", "at_bat_number", "pitch_number"])
           .groupby(["game_pk", "at_bat_number"]).head(1).index)
    df.loc[idx, "first_pitch"] = True
    return df

def tag_leadoff_seq(df: pd.DataFrame) -> pd.DataFrame:
    """Label 0-0 first-pitch PAs for *the same batter* who led off the game."""
    df["leadoff_seq"] = -1
    # determine leadoff batter id for each game
    lead_dict = (
        df[(df.inning == 1) & (df.pa_order == 1)]
          .groupby("game_pk")["batter"].first()
          .to_dict()
    )
    for gpk, b_id in lead_dict.items():
        mask = (
            (df.game_pk == gpk) &
            (df.batter == b_id) &
            df.first_pitch &
            (df.balls == 0) & (df.strikes == 0)
        )
        df.loc[mask, "leadoff_seq"] = (
            df.loc[mask]
              .sort_values("at_bat_number")
              .groupby("game_pk").cumcount()
        )
    return df

def bucket(row) -> str | None:
    if not (row.first_pitch and row.balls == 0 and row.strikes == 0):
        return None
    if row.inning == 1 and row.pa_order == 2:
        return "Batter_2"
    if row.inning == 1 and row.pa_order == 3:
        return "Batter_3"
    if row.inning == 2 and row.pa_order == 1:
        return "Inning_2_leadoff"
    if row.inning == 3 and row.pa_order == 1:
        return "Inning_3_leadoff"
    if row.leadoff_seq == 1:
        return "Leadoff_2nd_PA"
    return None

# ── 3. Main loop ───────────────────────────────────────────────────────
for name, pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    for attempt in (1, 2):
        try:
            df = statcast_pitcher(START, END, pid); break
        except (ParserError, EmptyDataError):
            time.sleep(2); df = pd.DataFrame()
    if df.empty:
        continue

    df = df[df.game_type == "R"]
    df = add_pa_order(df)
    df = mark_first_pitch(df)

    # keep only games they started
    started = df.loc[(df.inning == 1) & (df.pa_order == 1), "game_pk"].unique()
    df = df[df.game_pk.isin(started)]
    if df.empty:
        continue

    df = tag_leadoff_seq(df)
    df["bucket"] = df.apply(bucket, axis=1)
    df = df.dropna(subset=["bucket"])
    if df.empty:
        continue

    pc = "pitch_name" if "pitch_name" in df.columns else "pitch_type"
    detail = (df[["game_pk", "game_date", "bucket", pc]]
              .rename(columns={pc: "pitch_name"})
              .sort_values(["game_date", "game_pk"]))
    detail.to_csv(f"{name}_first_pitch.csv", index=False)

    summary = (detail.groupby(["bucket", "pitch_name"])
               .size().reset_index(name="count"))
    summary["pct"] = summary.groupby("bucket")["count"].transform(
        lambda x: (x / x.sum() * 100).round(1)
    )
    summary.to_csv(f"{name}_first_pitch_summary.csv", index=False)

print("\n✅ All pitchers processed")
