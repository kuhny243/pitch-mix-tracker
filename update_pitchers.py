# update_pitchers.py
# 118 pitchers • FIVE buckets • full pitch names

from pybaseball import statcast_pitcher
import pandas as pd, time
from pandas.errors import ParserError, EmptyDataError
from datetime import date

START, END = "2026-03-01","2026-12-31"

# ── full-name lookup for pitch_type codes ──────────────────────────────
CODE_TO_NAME = {
    "FF": "4-Seam Fastball",
    "FT": "2-Seam Fastball",
    "SI": "Sinker",
    "FC": "Cutter",
    "FS": "Splitter",
    "SL": "Slider",
    "SW": "Sweeper",
    "KC": "Knuckle Curve",
    "CU": "Curveball",
    "CH": "Changeup",
    "EP": "Eephus",
    "KN": "Knuckleball",
    "SC": "Screwball",
    # add rare codes if they appear
}

# ── 1. Pitcher → MLBAM IDs (118 total) ─────────────────────────────────
PITCHERS = {
    "Andrew_Abbott": 671096,
"Mick_Abel": 690953,
"Sandy_Alcantara": 645261,
"Sam_Aldegheri": 691951,
"Jason_Alexander": 669920,
"Andrew_Alvarez": 674841,
"Spencer_Arrighetti": 681293,
"Braxton_Ashcraft": 677952,
"Javier_Assad": 665871,
"Luinder_Avila": 679883,
"Chris_Bassitt": 605135,
"Shane_Baz": 669358,
"Brayan_Bello": 678394,
"Jake_Bennett": 687562,
"Tanner_Bibee": 676440,
"Matthew_Boyd": 571510,
"Joe_Boyle": 671212,
"Kyle_Bradish": 680694,
"Taj_Bradley": 671737,
"Ben_Brown": 676962,
"Hunter_Brown": 686613,
"Kris_Bubic": 663460,
"Walker_Buehler": 621111,
"Sean_Burke": 680732,
"Chase_Burns": 695505,
"Mike_Burrows": 681347,
"Edward_Cabrera": 665795,
"Jose_Cabrera": 703615,
"Noah_Cameron": 702070,
"Griffin_Canning": 656288,
"Joey_Cantillo": 676282,
"Luis_Castillo": 622491,
"Cade_Cavalli": 676917,
"Dylan_Cease": 656302,
"Slade_Cecconi": 677944,
"Bubba_Chandler": 696149,
"Aaron_Civale": 650644,
"Gerrit_Cole": 543037,
"Patrick_Corbin": 571578,
"Garrett_Crochet": 676979,
"Coleman_Crow": 689441,
"Jacob_deGrom": 594798,
"Reid_Detmers": 672282,
"Chase_Dollander": 801403,
"Shane_Drohan": 675660,
"Connelly_Early": 813349,
"Bryce_Elder": 693821,
"Nathan_Eovaldi": 543135,
"Erick_Fedde": 607200,
"Ryan_Feltner": 663372,
"Jack_Flaherty": 656427,
"Kyle_Freeland": 607536,
"Max_Fried": 608331,
"Zac_Gallen": 668678,
"Robert_Gasser": 688107,
"Kevin_Gausman": 592332,
"Trey_Gibson": 694346,
"Luis_Gil": 661563,
"Logan_Gilbert": 669302,
"JT_Ginn": 669372,
"Lucas_Giolito": 608337,
"Tyler_Glasnow": 607192,
"Tanner_Gordon": 685299,
"MacKenzie_Gore": 669022,
"Sonny_Gray": 543243,
"Foster_Griffin": 656492,
"Ryan_Gusto": 687473,
"Emerson_Hancock": 676106,
"Kyle_Harrison": 690986,
"Logan_Henderson": 701656,
"Clay_Holmes": 605280,
"Grant_Holmes": 656550,
"Adrian_Houser": 605288,
"Tatsuya_Imai": 837227,
"Shota_Imanaga": 684007,
"Jake_Irvin": 663623,
"Griffin_Jax": 643377,
"Ryan_Johnson": 696270,
"Jared_Jones": 683003,
"Gage_Jump": 695611,
"Janson_Junk": 676083,
"Anthony_Kay": 641743,
"Mitch_Keller": 656605,
"Merrill_Kelly": 518876,
"Yusei_Kikuchi": 579328,
"Michael_King": 650633,
"George_Kirby": 669923,
"Jack_Kochanowicz": 686799,
"Stephen_Kolek": 663568,
"Dean_Kremer": 665152,
"Peter_Lambert": 663567,
"Eric_Lauer": 641778,
"Kyle_Leahy": 681517,
"Jack_Leiter": 683004,
"Matthew_Liberatore": 669461,
"Zack_Littell": 641793,
"Nick_Lodolo": 666157,
"Reynaldo_López": 625643,
"Jacob_Lopez": 682052,
"Michael_Lorenzen": 547179,
"Rhett_Lowder": 695076,
"Seth_Lugo": 607625,
"Jesús_Luzardo": 666200,
"Tyler_Mahle": 641816,
"Sean_Manaea": 640455,
"Germán_Márquez": 608566,
"Davis_Martin": 663436,
"Nick_Martinez": 607259,
"Zebby_Matthews": 805673,
"Steven_Matz": 571927,
"Dustin_May": 669160,
"Shane_McClanahan": 663556,
"Lance_McCullers_Jr": 621121,
"Trevor_McDonald": 686790,
"Michael_McGreevy": 700241,
"Nolan_McLean": 690997,
"Troy_Melton": 675512,
"Parker_Messick": 800048,
"Max_Meyer": 676974,
"Miles_Mikolas": 571945,
"Bryce_Miller": 682243,
"Jacob_Misiorowski": 694819,
"Casey_Mize": 663554,
"Carmen_Mlodzinski": 669387,
"Keider_Montero": 672456,
"Ryne_Nelson": 669194,
"Aaron_Nola": 605400,
"Bailey_Ober": 641927,
"Shohei_Ohtani": 660271,
"Chris_Paddack": 663978,
"Andrew_Painter": 691725,
"Andre_Pallante": 669467,
"Mike_Paredes": 702474,
"Chad_Patrick": 694477,
"Freddy_Peralta": 642547,
"Eury_Pérez": 691587,
"Martín_Pérez": 527048,
"Jack_Perkins": 678022,
"David_Peterson": 656849,
"Chase_Petty": 695534,
"Brandon_Pfaadt": 694297,
"Tyler_Phillips": 663969,
"Nick_Pivetta": 601713,
"PJ_Poulin": 676571,
"Cade_Povich": 700249,
"Connor_Prielipp": 687570,
"Jose_Quintana": 500779,
"Cole_Ragans": 666142,
"Drew_Rasmussen": 656876,
"Robbie_Ray": 592662,
"Colin_Rea": 607067,
"JR_Ritchie": 702275,
"Kumar_Rocker": 677958,
"Carlos_Rodón": 607074,
"Eduardo_Rodriguez": 593958,
"Grayson_Rodriguez": 680570,
"Elmer_Rodríguez": 695684,
"Trevor_Rogers": 669432,
"Landen_Roupp": 694738,
"Joe_Ryan": 657746,
"Chris_Sale": 519242,
"Cristopher_Sánchez": 650911,
"David_Sandlin": 689818,
"Roki_Sasaki": 808963,
"Max_Scherzer": 453286,
"Cam_Schlittler": 693645,
"Jesse_Scholtens": 669947,
"Noah_Schultz": 702273,
"Christian_Scott": 681035,
"JP_Sears": 676664,
"Kodai_Senga": 673540,
"Luis_Severino": 622663,
"Ian_Seymour": 693855,
"Emmet_Sheehan": 686218,
"Brady_Singer": 663903,
"Paul_Skenes": 694973,
"Tarik_Skubal": 669373,
"José_Soriano": 667755,
"Michael_Soroka": 647336,
"Jeffrey_Springs": 605488,
"Brandon_Sproat": 687075,
"Spencer_Strider": 675911,
"Ranger_Suarez": 624133,
"Tomoyuki_Sugano": 608372,
"Sean_Sullivan": 807743,
"Jameson_Taillon": 592791,
"Kai-Wei_Teng": 678906,
"Zach_Thornton": 804267,
"Payton_Tolle": 801139,
"Walbert_Ureña": 700712,
"Framber_Valdez": 664285,
"Randy_Vásquez": 681190,
"Michael_Wacha": 608379,
"Matt_Waldron": 663362,
"Taijuan_Walker": 592836,
"Will_Warren": 701542,
"Ryan_Weathers": 677960,
"Logan_Webb": 657277,
"Zack_Wheeler": 554430,
"Gavin_Williams": 668909,
"Brandon_Williamson": 682227,
"Bryan_Woo": 693433,
"Brandon_Woodruff": 605540,
"Simeon_Woods_Richardson": 680573,
"Justin_Wrobleski": 680736,
"Yoshinobu_Yamamoto": 808967,
"Trey_Yesavage": 702056,
"Brandon_Young": 687064,
}

# ── 2. Helper functions ────────────────────────────────────────────────
def add_pa_order(df):
    df["pa_order"] = (
        df.sort_values("at_bat_number")
          .groupby(["game_pk","inning","inning_topbot"])["at_bat_number"]
          .rank(method="dense").astype(int)
    ); return df

def mark_first_pitch(df):
    pc = "pitch_name" if "pitch_name" in df.columns else "pitch_type"
    df["first_pitch"] = False
    idx = (df[df[pc].notna()]
           .sort_values(["game_pk","at_bat_number","pitch_number"])
           .groupby(["game_pk","at_bat_number"]).head(1).index)
    df.loc[idx,"first_pitch"] = True; return df

def tag_slot_seq(df):
    df["slot_seq"] = -1
    mask = (df.first_pitch & (df.pa_order==1) &
            (df.balls==0) & (df.strikes==0))
    df.loc[mask,"slot_seq"] = (
        df.loc[mask].sort_values("at_bat_number")
          .groupby("game_pk").cumcount()
    ); return df

def bucket(row):
    if row.first_pitch and row.balls==0 and row.strikes==0:
        if row.inning==1 and row.pa_order==1: return "Batter_1"
        if row.inning==1 and row.pa_order==2: return "Batter_2"
        if row.inning==1 and row.pa_order==3: return "Batter_3"
        if row.inning==2 and row.pa_order==1: return "Inning_2_leadoff"
        if row.inning==3 and row.pa_order==1: return "Inning_3_leadoff"
    if row.slot_seq==1:
        return "Leadoff_2nd_PA"
    return None

def full_pitch_name(row):
    """Return existing pitch_name if present, else map pitch_type code."""
    if pd.notna(row.pitch_name) and row.pitch_name.strip():
        return row.pitch_name.strip()
    return CODE_TO_NAME.get(row.pitch_type, row.pitch_type)

# ── 3. Main loop ───────────────────────────────────────────────────────
for name,pid in PITCHERS.items():
    print(f"\n=== {name} ({pid}) ===")
    for attempt in (1,2):
        try:
            df = statcast_pitcher(START,END,pid); break
        except (ParserError,EmptyDataError):
            time.sleep(2); df = pd.DataFrame()
    if df.empty: continue

    df = df[df.game_type=="R"]
    df = add_pa_order(df); df = mark_first_pitch(df)

    starts = df.loc[(df.inning==1)&(df.pa_order==1),"game_pk"].unique()
    df = df[df.game_pk.isin(starts)]
    if df.empty: continue

    df = tag_slot_seq(df)
    df["bucket"] = df.apply(bucket,axis=1)
    df = df.dropna(subset=["bucket"])
    if df.empty: continue

    df["pitch_name_full"] = df.apply(full_pitch_name, axis=1)

    detail = (
        df[["game_pk","game_date","bucket","pitch_name_full"]]
          .rename(columns={"pitch_name_full":"pitch_name"})
          .sort_values(["game_date","game_pk"])
    )
    detail.to_csv(f"{name}_first_pitch.csv",index=False)

    summary = (
        detail.groupby(["bucket","pitch_name"])
              .size().reset_index(name="count")
    )
    summary["pct"] = summary.groupby("bucket")["count"].transform(
        lambda x:(x/x.sum()*100).round(1)
    )
    summary.to_csv(f"{name}_first_pitch_summary.csv",index=False)

print("\n✅ All pitchers processed — full names restored")
