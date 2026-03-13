#!/usr/bin/env python3
"""
Plausi-Notebook Generator für OGD-Datensätze der Stadt Zürich.

Erstellt neue Jupyter Notebooks für Plausibilitätsprüfungen basierend auf
den Vorlagen in mypy_plausis_orig. Generiert die Zellen regelbasiert
aus dem Datensatz-Schema (keine externe API erforderlich).

Usage:
  Neues Notebook:
    python generate_plausi_notebook.py \\
        --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \\
        --dataset_name BEV336OD3361

  Mit Output-Verzeichnis:
    python generate_plausi_notebook.py \\
        --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \\
        --dataset_name BEV336OD3361 \\
        --output_dir ./mypy_plausis_vandamme/plausi_bev_jahresupdates

  Update eines bestehenden Notebooks:
    python generate_plausi_notebook.py \\
        --update \\
        --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \\
        --dataset_name BEV336OD3361

Voraussetzungen:
  pip install pandas requests
"""

import argparse
import io
import json
import re
import requests
import sys
import urllib3
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

urllib3.disable_warnings()

# ─── Pfade ────────────────────────────────────────────────────────────────────
# Dieses Script liegt in plausis_made_easy/ → BASE_DIR ist das Script-Verzeichnis
BASE_DIR = Path(__file__).parent
VANDAMME_DIR = BASE_DIR / "mypy_plausis_vandamme"
DEFAULT_OUTPUT_DIR = VANDAMME_DIR / "plausi_bev_jahresupdates"

# ─── CKAN API ─────────────────────────────────────────────────────────────────
CKAN_PROD = "https://data.stadt-zuerich.ch"
CKAN_INTEG = "https://data.integ.stadt-zuerich.ch"

# ─── Spalten-Präfix → menschenlesbares Label ─────────────────────────────────
# Bekannte OGD-Zürich-Konventionen. Fallback: Präfix mit Grossbuchstabe.
LABEL_MAP = {
    "Quar":         "Stadtquartier",
    "Kreis":        "Stadtkreis",
    "NationHist":   "Nationalität",
    "Nation":       "Nationalität",
    "Kontinent":    "Kontinent",
    "Region":       "Region",
    "Geschlecht":   "Geschlecht",
    "Alter":        "Altersklasse",
    "AlterV":       "Altersklasse",
    "Herkunft":     "Herkunft",
    "Zivilstand":   "Zivilstand",
    "Konfession":   "Konfession",
    "Aufenthalt":   "Aufenthaltsart",
    "Wohnviertel":  "Wohnviertel",
    "Gebiet":       "Gebiet",
    "Stat":         "Statistisches Gebiet",
    "Zuzugsort":    "Zuzugsort",
    "Wegzugsort":   "Wegzugsort",
}


def get_dim_label(lang_col: str) -> str:
    """
    Gibt den menschenlesbaren Label für eine *Lang-Spalte zurück.
    Sucht zuerst im LABEL_MAP (längster passender Präfix gewinnt),
    fällt zurück auf den Präfix mit Grossbuchstabe.
    """
    prefix = lang_col.replace("Lang", "")
    # Längsten Match im LABEL_MAP bevorzugen (z.B. "NationHist" vor "Nation")
    best = ""
    for key, label in LABEL_MAP.items():
        if prefix.startswith(key) and len(key) > len(best):
            best = key
    if best:
        return LABEL_MAP[best]
    # Fallback: CamelCase aufteilen, z.B. "AltersklasseV2" → "Altersklasse V 2"
    return re.sub(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", prefix).strip()

# ─── Notebook-Boilerplate ─────────────────────────────────────────────────────
IMPORTS_CODE = """\
import altair as alt
import datetime
import folium
import geopandas as gpd
import io
from IPython.display import Markdown as md
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
#import pivottablejs
#from pivottablejs import pivot_ui
import plotly.express as px
import requests
import seaborn as sns"""

VERSION_CODE = """\
#base env 2025: Python 3.11.7
import ipykernel
print(ipykernel.__version__)

import sys
import platform
print("Python-Version:", sys.version)
print("Python-Implementierung:", platform.python_implementation())
print("Python-Build:", platform.python_build())
print("Python-Compiler:", platform.python_compiler())

print("Altair-Version:", alt.__version__)
print("Seaborn-Version:", sns.__version__)"""

CUSTOM_IMPORTS_CODE = """\
import sys
sys.path.append('../0_scripts')

import my_py_dataviz_functions as mypy_dv
import my_py_dataloading_functions as mypy_dl"""

SSL_CODE_1 = """\
SSL_VERIFY = False
# evtl. SSL_VERIFY auf False setzen wenn die Verbindung zu https://www.gemeinderat-zuerich.ch nicht klappt (z.B. wegen Proxy)
# Um die SSL Verifikation auszustellen, bitte die nächste Zeile einkommentieren ("#" entfernen)
# SSL_VERIFY = False"""

SSL_CODE_2 = """\
if not SSL_VERIFY:
    import urllib3
    urllib3.disable_warnings()"""

SETTINGS_CODE = """\
#pd.options.display.float_format = lambda x : '{:,.1f}'.format(x) if (np.isnan(x) | np.isinf(x)) else '{:,.0f}'.format(x) if int(x) == x else '{:,.1f}'.format(x)
pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.width', 100)
pd.set_option('display.max_columns', 15)"""

PALETTES_CODE = """\
# Quantitative Paletten
zuericolors_qual12 = ["#3431DE", "#0A8DF6", "#23C3F1", "#7B4FB7", "#DB247D", "#FB737E", "#007C78", "#1F9E31", "#99C32E", "#9A5B01", "#FF720C", "#FBB900"]
zuericolors_qual12br = ["#5D4BFE", "#4AA9FF", "#55FFFF", "#986AD5", "#FC4C99", "#FF919A", "#349894", "#44B14A", "#B7E14E", "#B97624", "#FF7231", "#FFD736"]
zuericolors_qual12da= ["#0017BF", "#0072D7", "#00A5D2", "#5E359A", "#BA0062", "#DA5563", "#00615D", "#00770F", "#7BA600", "#7B4100", "#DC5500", "#DA9C00"]
# Divergente Paletten
zuericolors_div9val  =  ["#A30059", "#DB247D", "#FF579E", "#FFA8D0", "#E4E0DF", "#A8DBB1", "#55BC5D", "#1F9E31", "#10652A"]
zuericolors_div9ntr  =  ["#782600", "#CC4309", "#FF720C", "#FFBC88", "#E4E0DF", "#AECBFF", "#6B8EFF", "#3B51FF", "#2F2ABB"]
# Geschlechter Paletten
zuericolors_gender3  =  ["#349894", "#FFD736", "#986AD5"]
zuericolors_gender6origin  =  ["#00615D", "#349894", "#DA9C00", "#FFD736", "#5E359A", "#986AD5"]
zuericolors_gender5wedding  =  ["#349894", "#FFD736", "#3431DE", "#B8B8B8", "#D6D6D6"]
# Sequenzielle Paletten
zuericolors_seq9blu  =  ["#CADEFF", "#AEC2FF", "#93A6FF", "#778AFF", "#5B6EFF", "#4D59E2", "#3E44C5", "#302FA7", "#211A8A"]
zuericolors_seq9red  =  ["#FED2EE", "#FEAED6", "#F589BE", "#F165A5", "#ED408D", "#D1307B", "#B52069", "#991056", "#7D0044"]
zuericolors_seq9grn  =  ["#CFEED8", "#A8E0B3", "#81D18F", "#5BC36A", "#34B446", "#2A9A3C", "#208032", "#166529", "#0C4B1F"]
zuericolors_seq9brn  =  ["#FCDDBB", "#F7BD8C", "#F39D5E", "#EE7D2F", "#EA5D00", "#C84E00", "#A53E00", "#832F00", "#611F00"]"""

TIME_VARS_CODE = """\
#Zeitvariabeln als Strings:
now = datetime.date.today()
year_today = now.strftime("%Y")
date_today = "_"+now.strftime("%Y-%m-%d")

#Zeitvariabeln als Integers:
int_times = now.timetuple()
aktuellesJahr = int_times[0]
aktuellerMonat = int_times[1]
selectedMonat = int_times[1]-2
#print(aktuellesJahr, aktuellerMonat,'datenstand: ', selectedMonat, int_times)"""


# ─── Hilfsfunktionen für Notebook-Zellen ─────────────────────────────────────

def md_cell(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": source, "id": ""}


def code_cell(source: str) -> dict:
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": source, "id": ""}


def add_cell_ids(cells: list) -> list:
    for i, cell in enumerate(cells):
        cell["id"] = f"cell-{i:04d}"
    return cells


# ─── CKAN API Funktionen ──────────────────────────────────────────────────────

def fetch_ckan_metadata(package_name: str) -> dict:
    """Holt Titel und Beschreibung von der CKAN PROD API."""
    url = f"{CKAN_PROD}/api/3/action/package_show?id={package_name.lower()}"
    print(f"  Hole CKAN-Metadaten: {url}")
    try:
        r = requests.get(url, verify=False, timeout=15)
        r.raise_for_status()
        result = r.json().get("result", {})
        return {
            "title": result.get("title", package_name),
            "notes": result.get("notes", ""),
        }
    except Exception as e:
        print(f"  Warnung: CKAN-Metadaten nicht verfügbar ({e}) – nutze Standardwerte")
        return {"title": package_name, "notes": ""}


def fetch_dataset_sample(
    package_name: str,
    dataset_name: str,
    status: str = "int",
    source: str = "web",
    n_rows: int = 300,
) -> Optional[pd.DataFrame]:
    """Lädt einen Ausschnitt des Datensatzes herunter (für Spalten-Analyse)."""
    if source == "dropzone":
        print()
        print("  ┌─────────────────────────────────────────────────────────────────┐")
        print("  │  HINWEIS: Dropzone-Zugriff                                      │")
        print("  │                                                                  │")
        print("  │  Die Dropzone (\\\\szh\\ssz\\applikationen\\OGD_Dropzone) ist ein    │")
        print("  │  internes Netzlaufwerk und kann nicht automatisch analysiert     │")
        print("  │  werden.                                                         │")
        print("  │                                                                  │")
        print("  │  → Das Notebook wird mit Standard-Spalten generiert:            │")
        print("  │    StichtagDatJahr / AnzBestWir                                 │")
        print("  │                                                                  │")
        print("  │  → Bitte nach der Generierung im Notebook manuell anpassen:     │")
        print("  │    - dataset_name in der load_data()-Zelle prüfen               │")
        print("  │    - Spaltenamen in groupby/Viz-Zellen korrigieren              │")
        print("  └─────────────────────────────────────────────────────────────────┘")
        print()
        return None

    if source == "ld":
        ld_base = "https://ld.integ.stzh.ch" if status == "int" else "https://ld.stadt-zuerich.ch"
        url = f"{ld_base}/statistics/view/{package_name.upper()}/observation?format=csv"
    else:
        # web
        if status == "int":
            url = f"{CKAN_INTEG}/dataset/int_dwh_{package_name.lower()}/download/{dataset_name}.csv"
        else:
            url = f"{CKAN_PROD}/dataset/{package_name.lower()}/download/{dataset_name}.csv"

    print(f"  Lade Datensatz-Sample: {url}")
    try:
        r = requests.get(url, verify=False, timeout=30)
        r.raise_for_status()
        df = pd.read_csv(
            io.StringIO(r.text),
            sep=",",
            na_values=["", ".", "...", "NA", "NULL"],
            low_memory=False,
            nrows=n_rows,
        )
        print(f"  {len(df)} Zeilen geladen, {len(df.columns)} Spalten: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"  Warnung: Datensatz nicht verfügbar ({e}) – Spalten unbekannt")
        return None


# ─── Spalten-Analyse ──────────────────────────────────────────────────────────

@dataclass
class ColumnInfo:
    date_cols: list = field(default_factory=list)
    value_cols: list = field(default_factory=list)
    lang_cols: list = field(default_factory=list)   # *Lang – Beschreibungsspalten
    sort_cols: list = field(default_factory=list)   # *Sort – Sortierspalten
    code_cols: list = field(default_factory=list)   # *Cd   – Codespalten
    other_cols: list = field(default_factory=list)
    all_cols: list = field(default_factory=list)
    primary_value_col: str = ""
    primary_date_col: str = "StichtagDatJahr"
    dtypes_str: str = ""


def classify_columns(df: pd.DataFrame) -> ColumnInfo:
    """
    Klassifiziert Spalten nach OGD-Zürich-Namenskonventionen:
      *Lang  → Dimensions-Spalte (Label)
      *Sort  → Sortier-Spalte (numerisch)
      *Cd    → Code-Spalte
      Anz*/Sum*/Rate* → Wert-Spalte
      Stichtag*/DatJahr* → Datum-Spalte
    """
    ci = ColumnInfo()
    ci.all_cols = list(df.columns)
    ci.dtypes_str = df.dtypes.to_string()

    for col in df.columns:
        col_lower = col.lower()
        dtype = str(df[col].dtype)

        if "stichtag" in col_lower or "datjahr" in col_lower or dtype.startswith("datetime"):
            ci.date_cols.append(col)
        elif col.startswith(("Anz", "Sum", "Rate", "Anteil", "Wert", "Anzahl")):
            ci.value_cols.append(col)
        elif col.endswith("Lang"):
            ci.lang_cols.append(col)
        elif col.endswith("Sort"):
            ci.sort_cols.append(col)
        elif re.search(r"Cd\d*$", col):
            ci.code_cols.append(col)
        else:
            ci.other_cols.append(col)

    ci.primary_date_col = ci.date_cols[0] if ci.date_cols else "StichtagDatJahr"
    ci.primary_value_col = ci.value_cols[0] if ci.value_cols else ""
    return ci


def get_col_group(lang_col: str, ci: ColumnInfo) -> list:
    """
    Gibt für eine Lang-Spalte die zugehörigen Sort/Cd-Spalten zurück.
    Beispiel: 'QuarLang' → ['QuarLang', 'QuarSort'] falls QuarSort existiert.
    """
    prefix = lang_col.replace("Lang", "")
    group = [lang_col]
    for col in ci.sort_cols + ci.code_cols:
        if col.startswith(prefix) and col not in group:
            group.append(col)
    return group


# ─── Regelbasierte Zellen-Generierung ────────────────────────────────────────

def generate_dynamic_cells(
    package_name: str,
    title: str,
    ci: ColumnInfo,
) -> list:
    """
    Generiert die dataset-spezifischen Notebook-Zellen regelbasiert.
    Kein externer API-Aufruf nötig – basiert auf der Spalten-Klassifikation.
    """
    cells = []

    date_col = ci.primary_date_col
    value_col = ci.primary_value_col or "AnzBestWir"
    agg_name = f"sum_{value_col}"

    # Spalten-Gruppen: jede Lang-Spalte mit ihren Sort/Cd-Begleitern
    dim_groups = [(lang, get_col_group(lang, ci)) for lang in ci.lang_cols]
    all_dim_cols = [col for _, grp in dim_groups for col in grp]

    prim_lang = ci.lang_cols[0] if ci.lang_cols else None
    prim_group = dim_groups[0][1] if dim_groups else []
    prim_label = prim_lang.replace("Lang", "") if prim_lang else ""
    prim_sort = next((c for c in ci.sort_cols if prim_lang and c.startswith(prim_lang.replace("Lang", ""))), None)

    def cols_str(cols):
        """['A', 'B', 'C'] → \"'A', 'B', 'C'\" """
        return ", ".join(f"'{c}'" for c in cols)

    # ─── Gruppierungen ────────────────────────────────────────────────────────
    cells.append(md_cell("### Gruppierungen"))

    # agg_jahr: nur nach Zeit
    cells.append(code_cell(
        f"agg_jahr = data2betested.loc[data_min_date:data_max_date]\\\n"
        f"    .groupby(['Jahr', 'Jahr_nbr', 'Jahr_end']) \\\n"
        f"    .agg({agg_name}=('{value_col}', 'sum')) \\\n"
        f"    .sort_values('Jahr', ascending=False) \n"
        f"agg_jahr.reset_index().head(3)"
    ))

    # agg_dim: nach primärer Dimension (ohne Zeit)
    if prim_group:
        cells.append(code_cell(
            f"agg_dim = data2betested.loc[data_min_date:data_max_date]\\\n"
            f"    .groupby([{cols_str(prim_group)}]) \\\n"
            f"    .agg({agg_name}=('{value_col}', 'sum')) \\\n"
            f"    .sort_values('{prim_lang}', ascending=True) \n"
            f"agg_dim.reset_index().head(5)"
        ))

    # agg_raum_zeit: Zeit + alle Dimensionen
    if all_dim_cols:
        cells.append(code_cell(
            f"agg_raum_zeit = data2betested.loc[data_min_date:data_max_date]\\\n"
            f"    .groupby(['Jahr_nbr', 'Jahr_end', {cols_str(all_dim_cols)}]) \\\n"
            f"    .agg({agg_name}=('{value_col}', 'sum')) \\\n"
            f"    .sort_values('Jahr_nbr', ascending=True) \n"
            f"agg_raum_zeit.reset_index().head(3)"
        ))

    cells.append(code_cell("data2betested.reset_index().columns"))

    # ─── Pivot ────────────────────────────────────────────────────────────────
    cells.append(md_cell("### Pivotiere"))

    pivot_cols = prim_group[:2] if prim_group else [value_col]
    cells.append(code_cell(
        f"pivoted_df = data2betested.pivot_table(\n"
        f"    index='Jahr_nbr',\n"
        f"    columns= ({cols_str(pivot_cols)}),\n"
        f"    values='{value_col}',\n"
        f"    aggfunc='sum'\n"
        f")\n\n"
        f"pivoted_df = pivoted_df.sort_index(ascending=False)\n\n"
        f"print(pivoted_df.head(8).T)"
    ))

    # ─── Visualisierungen ─────────────────────────────────────────────────────
    cells.append(md_cell("### Visualisierungen nach Zeitausschnitten"))

    if not prim_lang:
        # Fallback: keine Dimensions-Spalten, nur Zeitreihe
        cells.append(md_cell("#### Zeitreihe (keine Dimensions-Spalten erkannt)"))
        cells.append(code_cell(
            f"grafik1 = mypy_dv.plot_altair_multiline_highlight(\n"
            f"    data = agg_jahr.reset_index()\n"
            f"    ,x = 'Jahr:T'\n"
            f"    ,y = '{agg_name}:Q'\n"
            f"    ,x_beschriftung = 'Jahr'\n"
            f"    ,y_beschriftung = 'Anz. Bestand'\n"
            f"    ,warning_status = \"ignore\"\n"
            f'    ,myTitle="{title}, seit "+data_min_date\n'
            f")\n"
            f"grafik1"
        ))
        return cells

    # Alle Dimensions-Spalten inkl. Datum für Aggregation
    viz_groupby = [date_col] + all_dim_cols
    viz_groupby_time = [date_col] + all_dim_cols + ["Jahr", "Jahr_end", "Jahr_nbr"]

    # ── Altair Liniendiagramm ─────────────────────────────────────────────────
    cells.append(md_cell(f"#### Entwicklung {title} nach {prim_label}"))

    cells.append(code_cell(
        f"myAgg1 = data2betested.loc[data_min_date:data_max_date]\\\n"
        f"    .groupby([{cols_str(viz_groupby)}]) \\\n"
        f"    .agg({agg_name}=('{value_col}', 'sum')) \\\n"
        f"    .sort_values('{date_col}', ascending=True) \n\n"
        f"myAgg1.reset_index().head(3)"
    ))

    cells.append(code_cell(
        f"grafik1 = mypy_dv.plot_altair_multiline_highlight(\n"
        f"    data = myAgg1.reset_index().sort_values('{prim_lang}', ascending=True)\n"
        f"    ,x = '{date_col}:T'\n"
        f"    ,y = '{agg_name}:Q'\n"
        f"    ,x_beschriftung = 'Jahr'\n"
        f"    ,y_beschriftung = 'Anz. Bestand'\n"
        f"    ,category = \"{prim_lang}:N\"\n"
        f"    ,category_beschriftung= 'Legende:'\n"
        f"    ,x_sort = None\n"
        f"    ,palette_scheme = None\n"
        f"    ,custom_palette = zuericolors_qual12br\n"
        f"    ,line_width = 1.1\n"
        f"    ,warning_status = \"ignore\"\n"
        f'    ,myTitle="{title} nach {prim_label}, seit "+data_min_date\n'
        f")\n"
        f"grafik1"
    ))

    cells.append(md_cell("##### Kombinierte Grafik in Altair"))
    cells.append(code_cell(
        "top_row = alt.hconcat(grafik1, grafik1)\n"
        "combined_chart = alt.vconcat(grafik1, grafik1, spacing=20)\n\n"
        "final_chart = combined_chart.properties(\n"
        '    title="Alle Teilgrafiken in einer zusammengesetzt:    ",\n'
        '    background="#FDFDFD",\n'
        '    padding={"left": 20, "top": 20, "right": 20, "bottom": 20},\n'
        '    autosize={"type": "fit", "contains": "padding"}\n'
        ")\n"
        "final_chart = final_chart.resolve_scale(color='independent', shape='independent', size='independent')\n"
        "final_chart"
    ))

    # ── Seaborn Barcharts ─────────────────────────────────────────────────────
    cells.append(md_cell("#### Barcharts mit Seaborn"))

    cells.append(code_cell(
        f"myAggBar = data2betested.loc[data_min_date:data_max_date]\\\n"
        f"    .groupby([{cols_str(viz_groupby_time)}]) \\\n"
        f"    .agg({agg_name}=('{value_col}', 'sum')) \\\n"
        f"    .sort_values('{date_col}', ascending=True)\n\n"
        f"myAggBar.reset_index().head(3)"
    ))

    cells.append(code_cell('sns.set_theme(style="whitegrid")'))

    cells.append(code_cell(
        f'myHist = sns.catplot(x="{prim_lang}"\n'
        f'            , y="{agg_name}"\n'
        f'            , kind="bar"\n'
        f'            , palette="pastel"\n'
        f'            , height=5\n'
        f'            , aspect=3\n'
        f'            , order=None, legend_out=True\n'
        f'            ,data=myAggBar.reset_index().query(f"Jahr_nbr == {{data_max_jahr}}")\n'
        f'           )\n'
        f'myHist.set_xticklabels(rotation=45)\n'
        f"myHist.set_xlabels('{prim_label}', fontsize=11)\n"
        f"myHist.set_ylabels('Anz. Bestand', fontsize=11)"
    ))

    cells.append(md_cell("##### Stacked Bar Chart"))
    cells.append(code_cell(
        f"data = myAggBar.query(\"{agg_name} > 0\").reset_index()\n"
        f"data_pivoted = data.pivot(index='Jahr_nbr', columns='{prim_lang}', values='{agg_name}').fillna(0)\n\n"
        f"colors = sns.color_palette(\"cubehelix\", n_colors=len(data_pivoted.columns))\n"
        f"fig, ax = plt.subplots(figsize=(12, 6))\n"
        f"data_pivoted.plot(kind='bar', stacked=True, ax=ax, color=colors, width=0.9)\n"
        f"plt.title('{title} nach {prim_label} seit '+data_min_date, fontsize=14)\n"
        f"ax.set_xlabel('Jahr', fontsize=11)\n"
        f"ax.set_ylabel('Anz. Bestand', fontsize=11)\n"
        f"plt.legend(title='{prim_label}', bbox_to_anchor=(1.05, 1), loc='upper left')\n"
        f"plt.tight_layout()\n"
        f"plt.show()"
    ))

    # ── FacetGrid ─────────────────────────────────────────────────────────────
    cells.append(md_cell("#### Faced Grids"))
    cells.append(code_cell("data2betested.columns"))

    filter_expr = f"{prim_sort} != 0" if prim_sort else "index == index"
    cells.append(code_cell(
        f"myFG = data2betested.reset_index().query('{filter_expr}')\n"
        f"myFG.head(3)"
    ))

    sort_col_for_fg = prim_sort or prim_lang
    cells.append(code_cell(
        f"faced_grid1 = mypy_dv.plot_sns_facetgrid(\n"
        f"    data = myFG.reset_index().sort_values('{sort_col_for_fg}', ascending=True)\n"
        f"    ,col = \"{prim_lang}\"\n"
        f"    ,hue = \"{prim_lang}\"\n"
        f"    ,col_wrap = 5\n"
        f"    ,grafiktyp = sns.lineplot\n"
        f"    ,x = \"{date_col}\"\n"
        f"    ,y = \"{value_col}\"\n"
        f"    ,ylabel= \"Anz. Bestand\"\n"
        f"    ,warning_status =\"ignore\"\n"
        f"    ,height = 3\n"
        f'    ,myTitle="{title} nach {prim_label}, seit "+str(int(data2betested.index.year.min()))\n'
        f")\n"
        f"faced_grid1"
    ))

    # ── Treemaps ──────────────────────────────────────────────────────────────
    cells.append(md_cell("#### Treemaps"))
    cells.append(md_cell("**Funktion zum einfärben**\n\nMuss ich noch als Funktion umsetzen "))

    cells.append(code_cell(
        f"attr2becolored = data2betested['{prim_lang}'].unique().tolist()\n"
        f"verfügbare_farben_zuericolors = zuericolors_qual12da+zuericolors_qual12br+zuericolors_qual12+zuericolors_div9ntr\n\n"
        f"farben_dict_zc = {{'(?)':'lightgrey'}}\n"
        f"for index, x in enumerate(attr2becolored):\n"
        f"    farben_dict_zc[x] = verfügbare_farben_zuericolors[index % len(verfügbare_farben_zuericolors)]\n\n"
        f"print(farben_dict_zc)"
    ))

    cells.append(code_cell("data2betested.columns"))

    cells.append(md_cell("Jahre definieren, die dargestellt werden sollen"))
    cells.append(code_cell(
        "int_data_max_year = data2betested.index.max().year\n\n"
        "years = [\n"
        "    int_data_max_year - 20,\n"
        "    int_data_max_year\n"
        "]\n"
        "print(years)"
    ))

    cells.append(md_cell(f"##### {title} nach {prim_label} und Jahr"))
    cells.append(code_cell(
        f"myTM = data2betested.loc[data_min_date:data_max_date].reset_index()\n\n"
        f"myTM.reset_index().head(2)"
    ))

    cells.append(code_cell(
        f"treeMap1 = mypy_dv.plot_px_treemap(\n"
        f"    data=myTM.reset_index().query(\"{value_col}>0\")\n"
        f"    ,levels=['{prim_lang}', 'Jahr_nbr']\n"
        f"    ,values=\"{value_col}\"\n"
        f"    ,color=\"{value_col}\"\n"
        f"    ,color_discrete_map={{'(?)':'lightgrey'}}\n"
        f"    ,height=600\n"
        f"    ,width=1100\n"
        f'    ,myHeaderTitle="{title} nach {prim_label} und Jahr, seit "+data_min_date\n'
        f")\n"
        f"treeMap1"
    ))

    cells.append(md_cell(f"##### {title} nach {prim_label} (aktuellstes Jahr)"))
    cells.append(code_cell(
        f"myTM2 = data2betested.loc[data_max_date]\n\n"
        f"myTM2.reset_index().head(2)"
    ))

    cells.append(code_cell(
        f"treeMap2 = mypy_dv.plot_px_treemap(\n"
        f"    data=myTM2.reset_index().query(\"{value_col}>0\")\n"
        f"    ,levels=['{prim_lang}']\n"
        f"    ,values=\"{value_col}\"\n"
        f"    ,color=\"{prim_lang}\"\n"
        f"    ,color_discrete_map=farben_dict_zc\n"
        f"    ,height=600\n"
        f"    ,width=1100\n"
        f'    ,myHeaderTitle="{title} nach {prim_label} am "+data_max_date\n'
        f")\n"
        f"treeMap2"
    ))

    return cells


# ─── Fixe Boilerplate-Zellen ──────────────────────────────────────────────────

def build_fixed_cells(
    package_name: str,
    dataset_name: str,
    title: str,
    description: str,
    ci: ColumnInfo,
    status: str = "int",
    source: str = "web",
) -> list:
    """Erstellt die identischen Boilerplate-Zellen (Imports, Settings, etc.)."""
    today_str = date.today().strftime("%d.%m.%Y")
    prod_url = f"{CKAN_PROD}/dataset/{package_name.lower()}"
    integ_url = f"{CKAN_INTEG}/dataset/int_dwh_{package_name.lower()}"
    date_col = ci.primary_date_col
    value_col = ci.primary_value_col or "AnzBestWir"

    header = (
        f"# {title}\n"
        f"{package_name}\n\n"
        f"### Kurzbeschreibung\n"
        f"{description if description else 'Beschreibung'}\n\n"
        f"Datum: {today_str}\n\n\n"
        f"Dataset auf PROD-Datakatalog: Link {prod_url}\n\n"
        f"Dataset auf INTEG-Datakatalog: Link {integ_url}"
    )

    return [
        # Header
        md_cell(header),

        # Imports
        md_cell("### Importiere die notwendigen Packages"),
        code_cell("#%pip install geopandas altair fiona requests folium mplleaflet contextily seaborn datetime plotly leafmap"),
        code_cell(IMPORTS_CODE),

        # Version Check
        md_cell("Welche Python, Altair und Seaborn Version wird verwendet?"),
        code_cell(VERSION_CODE),

        # Custom Functions
        md_cell("Importiere die eigenen Funktionen, die unter ../0_scripts abegelegt sind:"),
        code_cell(CUSTOM_IMPORTS_CODE),

        # SSL
        code_cell(SSL_CODE_1),
        code_cell(SSL_CODE_2),

        # Settings
        md_cell("### Settings\nDefiniere Settings. \nHier das Zahlenformat von Float-Werten (z.B. *'{:,.2f}'.format* mit Komma als Tausenderzeichen)"),
        code_cell(SETTINGS_CODE),

        # Paletten
        md_cell("#### Paletten aus Zuericolors\nDie Farbwerte habe ich aus R ausgelesen. Siehe dazu: `G:\\sszsim\\myR\\zuericolors4python`"),
        code_cell(PALETTES_CODE),

        # Zeit-Variablen
        md_cell("#### Zeitvariabeln\n"),
        code_cell(TIME_VARS_CODE),

        # Package Name
        code_cell(f'package_name = "{package_name}"'),

        # Data Loading
        code_cell(
            f"data2betested = mypy_dl.load_data(\n"
            f"    status = '{status}'\n"
            f"    , data_source = '{source}'\n"
            f"    , package_name = package_name\n"
            f"    , dataset_name = \"{dataset_name}\"\n"
            f"    , datums_attr = ['{date_col}']\n"
            f"    )"
        ),

        # Head
        code_cell("data2betested.head(2).T"),

        # Feature Engineering
        md_cell("Berechne weitere Attribute falls notwendig"),
        code_cell(
            f"data2betested = (\n"
            f"    data2betested\n"
            f"    .copy()\n"
            f"    .assign(\n"
            f"        {date_col}_str = lambda x: x.{date_col}.astype(str),\n"
            f"        Jahr = lambda x: x.{date_col},\n"
            f"        Jahr_end = lambda x: x.{date_col}+pd.offsets.YearEnd(0),\n"
            f"        Jahr_nbr = lambda x: x.Jahr.dt.year,\n"
            f"    )\n"
            f"    .sort_values('{date_col}', ascending=False)\n"
            f"    )\n"
            f"data2betested.dtypes"
        ),

        # Min/Max Datum
        md_cell("Minimales und maximales Jahr im Datensatz"),
        code_cell(
            f"data_max_jahr = str(max(data2betested.Jahr).year)\n"
            f"data_min_jahr = str(min(data2betested.Jahr).year)\n\n"
            f'print(f"Die Daten haben ein Minimumjahr von {{data_min_jahr}} und ein Maximumjahr von {{data_max_jahr}}")'
        ),
        code_cell(
            f"data_max_date = str(max(data2betested.Jahr).year)\n"
            f"data_min_date = str(min(data2betested.Jahr).year)\n\n"
            f'print(f"Die Daten haben ein Minimumjahr von {{data_min_date}} und ein Maximumjahr von {{data_max_date}}")'
        ),

        # Einfache Datentests
        md_cell("### Einfache Datentests"),
        code_cell("data2betested.info(memory_usage='deep', verbose=True)"),
        code_cell(
            "print(f'The dataset has {data2betested.shape[0]:,.0f} rows (observations) and {data2betested.shape[1]:,.0f} columns (variables).')\n"
            "print(f'There seem to be {data2betested.duplicated().sum()} exact duplicates in the data.')"
        ),

        # Describe
        md_cell("Beschreibe einzelne Attribute"),
        code_cell("data2betested.describe()"),

        # Null-Check für primäre Wert-Spalte
        md_cell(f"Welches sind die Zeilen ohne Werte bei {value_col}?"),
        code_cell(f"data2betested[np.isnan(data2betested.{value_col})]" if value_col else "# Keine Wert-Spalte erkannt"),

        # Index setzen
        md_cell(
            f"### Verwende das Datum als Index\n\n"
            f"While we did already parse the `datetime` column into the respective datetime type, it currently is just a regular column. \n"
            f"**To enable quick and convenient queries and aggregations, we need to turn it into the index of the DataFrame**"
        ),
        code_cell(
            f'data2betested = data2betested.set_index("{date_col}")\n'
            f"data2betested = data2betested.sort_index()"
        ),
        code_cell("data2betested.index.year.unique()"),

        # Attribute beschreiben
        md_cell("### Beschreibe einzelne Attribute"),
        md_cell("Beschreibe nicht numerische Attribute"),
        code_cell(
            "# describe non-numerical features\n"
            "try:\n"
            "    with pd.option_context('display.float_format', '{:,.2f}'.format):\n"
            "        display(data2betested.describe(exclude='number'))\n"
            "except:\n"
            '    print("No categorical data in dataset.")'
        ),
        md_cell("Beschreibe numerische Attribute"),
        code_cell(
            "# describe numerical features\n"
            "try:\n"
            "    with pd.option_context('display.float_format', '{:,.0f}'.format):\n"
            "        display(data2betested.describe(include='number'))\n"
            "except:\n"
            '    print("No numercial data in dataset.")'
        ),
        code_cell(
            "plt.style.use('ggplot')\n"
            "params = {'text.color': (0.25, 0.25, 0.25), 'figure.figsize': [18, 6]}\n"
            "plt.rcParams.update(params)\n\n"
            "try:\n"
            "    data2betested.hist(bins=25, rwidth=0.9)\n"
            "    plt.tight_layout()\n"
            "    plt.show()\n"
            "except:\n"
            '    print("No numercial data to plot.")'
        ),

        # Duplikate
        md_cell("### Gibt es Duplikate?"),
        code_cell(
            "# find duplicate rows\n"
            "duplicate_rows = data2betested[data2betested.duplicated()]\n"
            "duplicate_rows"
        ),

        # Null-Werte
        md_cell("### Nullwerte und Missings?"),
        code_cell("data2betested.isnull().sum()"),
        code_cell(
            "# check missing values with missingno\n"
            "#import missingno as msno\n"
            "#msno.matrix(data2betested, labels=True, sort='descending');\n"
            "#msno.heatmap(data2betested)"
        ),
    ]


def build_end_cells() -> list:
    return [
        md_cell("## ---------------------- hier Plausi beendet\n\n**Sharepoint als gecheckt markieren!**"),
        md_cell("Record auf Sharepoint: **[Link]()**\n\n---------------------------------------------------------------------------"),
        code_cell(""),
        code_cell(""),
        code_cell(""),
        code_cell(""),
        code_cell(""),
    ]


def build_notebook(cells: list) -> dict:
    cells = add_cell_ids(cells)
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "pygments_lexer": "ipython3",
                "version": "3.11.0",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


# ─── Update-Modus ─────────────────────────────────────────────────────────────

def extract_col_names_from_notebook(nb: dict) -> list:
    """Extrahiert Spaltennamen aus einem bestehenden Notebook (Heuristik via groupby)."""
    for cell in nb.get("cells", []):
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        if "groupby" in src:
            return re.findall(r"'([A-Z][a-zA-Z0-9]+)'", src)
    return []


def update_existing_notebook(
    existing_nb_path: Path,
    package_name: str,
    dataset_name: str,
    metadata: dict,
    ci_new: ColumnInfo,
) -> dict:
    """
    Aktualisiert ein bestehendes Notebook:
    - Datum im Header auf heute setzen
    - Neue Dimensions-Spalten erkennen und Visualisierungen neu generieren
    """
    with open(existing_nb_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    cells = nb.get("cells", [])
    today_str = date.today().strftime("%d.%m.%Y")

    # Datum im Header aktualisieren
    if cells and cells[0].get("cell_type") == "markdown":
        src = cells[0].get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        src = re.sub(r"Datum: \d{2}\.\d{2}\.\d{4}", f"Datum: {today_str}", src)
        cells[0]["source"] = src
        print(f"  Header-Datum aktualisiert auf {today_str}")

    # Alte Spalten aus Notebook extrahieren
    old_cols = set(extract_col_names_from_notebook(nb))
    new_lang_cols = [c for c in ci_new.lang_cols if c not in old_cols]
    removed_cols = old_cols - set(ci_new.all_cols)

    if new_lang_cols:
        print(f"  Neue Dimensions-Spalten: {new_lang_cols}")
    if removed_cols:
        print(f"  Entfernte Spalten: {removed_cols}")

    # Bei neuen Spalten: Visualisierungsteil neu generieren
    if new_lang_cols:
        title = metadata.get("title", package_name)
        new_viz_cells = generate_dynamic_cells(package_name, title, ci_new)

        # End-Marker finden und neue Zellen davor einfügen
        end_idx = next(
            (i for i, c in enumerate(cells)
             if "hier Plausi beendet" in ("".join(c.get("source", "")) if isinstance(c.get("source"), list) else c.get("source", ""))),
            None
        )

        if end_idx is not None:
            cells = cells[:end_idx] + new_viz_cells + cells[end_idx:]
            print(f"  {len(new_viz_cells)} neue Zellen vor End-Marker eingefügt")
        else:
            cells.extend(new_viz_cells)
            print(f"  {len(new_viz_cells)} neue Zellen ans Ende angefügt")

    nb["cells"] = add_cell_ids(cells)
    return nb


# ─── Hauptfunktionen ──────────────────────────────────────────────────────────

def generate_new_notebook(
    package_name: str,
    dataset_name: str,
    output_dir: Path,
    status: str = "int",
    source: str = "web",
) -> Path:
    print(f"\n=== Generiere neues Plausi-Notebook ===")
    print(f"  Package:    {package_name}")
    print(f"  Dataset:    {dataset_name}")
    print(f"  Status:     {status}")
    print(f"  Source:     {source}")
    print(f"  Output-Dir: {output_dir}")

    print("\n[1/3] Hole Metadaten und Datensatz-Schema...")
    metadata = fetch_ckan_metadata(package_name)
    title = metadata["title"] or package_name
    description = metadata["notes"] or ""
    print(f"  Titel: {title}")

    df = fetch_dataset_sample(package_name, dataset_name, status=status, source=source)
    if df is not None:
        ci = classify_columns(df)
        print(f"  Datum:       {ci.date_cols}")
        print(f"  Werte:       {ci.value_cols}")
        print(f"  Dimensionen: {ci.lang_cols}")
        print(f"  Sort:        {ci.sort_cols}")
        print(f"  Codes:       {ci.code_cols}")
    else:
        print("  Fallback: Standard-Spaltenstruktur (StichtagDatJahr / AnzBestWir)")
        ci = ColumnInfo(
            date_cols=["StichtagDatJahr"],
            value_cols=["AnzBestWir"],
            all_cols=["StichtagDatJahr", "AnzBestWir"],
            primary_date_col="StichtagDatJahr",
            primary_value_col="AnzBestWir",
        )

    print("\n[2/3] Generiere Notebook-Zellen...")
    fixed = build_fixed_cells(package_name, dataset_name, title, description, ci, status, source)
    dynamic = generate_dynamic_cells(package_name, title, ci)
    end = build_end_cells()
    nb = build_notebook(fixed + dynamic + end)
    print(f"  {len(fixed + dynamic + end)} Zellen generiert")

    print("\n[3/3] Speichere Notebook...")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{package_name}.ipynb"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)

    print(f"\n✓ Notebook gespeichert: {output_path}")
    return output_path


def update_notebook(package_name: str, dataset_name: str, existing_nb: Path, output_dir: Path) -> Path:
    print(f"\n=== Aktualisiere Plausi-Notebook ===")
    print(f"  Bestehendes Notebook: {existing_nb}")

    if not existing_nb.exists():
        print(f"Fehler: Notebook nicht gefunden: {existing_nb}")
        sys.exit(1)

    print("\n[1/3] Hole Metadaten und neues Datensatz-Schema...")
    metadata = fetch_ckan_metadata(package_name)
    df = fetch_dataset_sample(package_name, dataset_name)
    ci_new = classify_columns(df) if df is not None else ColumnInfo(
        date_cols=["StichtagDatJahr"], value_cols=["AnzBestWir"],
        all_cols=["StichtagDatJahr", "AnzBestWir"],
        primary_date_col="StichtagDatJahr", primary_value_col="AnzBestWir",
    )

    print("\n[2/3] Aktualisiere Notebook...")
    nb_updated = update_existing_notebook(existing_nb, package_name, dataset_name, metadata, ci_new)

    print("\n[3/3] Speichere aktualisiertes Notebook...")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / existing_nb.name if output_dir != existing_nb.parent else existing_nb
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(nb_updated, f, ensure_ascii=False, indent=1)

    print(f"\n✓ Aktualisiertes Notebook gespeichert: {output_path}")
    return output_path


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generiert Plausi-Notebooks für OGD-Datensätze der Stadt Zürich",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--package_name", required=True,
        help="CKAN Package-Name, z.B. bev_bestand_jahr_quartier_nationalitaet_od3361",
    )
    parser.add_argument(
        "--dataset_name", required=True,
        help="Datensatz-Name ohne Endung, z.B. BEV336OD3361",
    )
    parser.add_argument(
        "--output_dir", type=Path, default=DEFAULT_OUTPUT_DIR,
        help=f"Ausgabe-Verzeichnis (Default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--status", choices=["int", "prod"], default="int",
        help="Datenumgebung: 'int' (Integration, Default) oder 'prod' (Produktion)",
    )
    parser.add_argument(
        "--source", choices=["web", "ld", "dropzone"], default="web",
        help="Datenquelle: 'web' (CKAN, Default), 'ld' (Linked Data), 'dropzone'",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Bestehendes Notebook aktualisieren statt neues zu erstellen",
    )
    parser.add_argument(
        "--existing_nb", type=Path,
        help="Pfad zum bestehenden Notebook (nur bei --update, sonst automatisch gesucht)",
    )

    args = parser.parse_args()

    if args.update:
        nb_path = args.existing_nb or (args.output_dir / f"{args.package_name}.ipynb")
        update_notebook(args.package_name, args.dataset_name, nb_path, args.output_dir)
    else:
        generate_new_notebook(args.package_name, args.dataset_name, args.output_dir, args.status, args.source)


if __name__ == "__main__":
    main()
