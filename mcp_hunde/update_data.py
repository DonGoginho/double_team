"""
Aktualisiert die Daten in index.html mit den neuesten Werten
aus dem OGD-Katalog der Stadt Zürich (CKAN Datastore API).

Voraussetzungen: Python 3.8+, requests (`pip install requests`)

Verwendung:
    python update_data.py
    python update_data.py --dry-run   # Zeigt Änderungen, schreibt aber nicht
"""

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote

import requests

API_BASE = "https://data.stadt-zuerich.ch/api/3/action/datastore_search_sql"
RESOURCE_ID = "f32e7c2a-2a37-4557-9cb4-38183ddf3f0d"
HTML_PATH = Path(__file__).parent / "index.html"

TOP_RASSEN_LIMIT = 40
PYRAMID_RASSEN_LIMIT = 25
MAX_ALTER = 18

KREIS_RASSEN = [
    'Labrador Retriever', 'Chihuahua', 'Malteser', 'Französische Bulldogge',
    'Jack Russell Terrier', 'Yorkshire Terrier', 'Golden Retriever',
    'Kleinpudel', 'Standarddachshund', 'Zwergpudel', 'Zwergdachshund',
    'Deutscher Zwergspitz/Pomeranian', 'Englischer Cocker Spaniel',
]


def sql_query(sql: str) -> list[dict]:
    resp = requests.get(API_BASE, params={"sql": sql}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"API-Fehler: {data.get('error', data)}")
    return data["result"]["records"]


def in_list(values: list[str]) -> str:
    escaped = [v.replace("'", "''") for v in values]
    return ",".join(f"'{v}'" for v in escaped)


def fetch_jahre_bestand() -> tuple[list[int], list[int]]:
    rows = sql_query(f'''
        SELECT "StichtagDatJahr", COUNT(*) as anzahl
        FROM "{RESOURCE_ID}"
        GROUP BY "StichtagDatJahr"
        ORDER BY "StichtagDatJahr"
    ''')
    jahre = [int(r["StichtagDatJahr"]) for r in rows]
    bestand = [int(r["anzahl"]) for r in rows]
    return jahre, bestand


def fetch_geschlecht(jahre: list[int]) -> tuple[list[int], list[int]]:
    rows = sql_query(f'''
        SELECT "StichtagDatJahr", "SexHundLang", COUNT(*) as anzahl
        FROM "{RESOURCE_ID}"
        GROUP BY "StichtagDatJahr", "SexHundLang"
        ORDER BY "StichtagDatJahr", "SexHundLang"
    ''')
    m_map = {int(r["StichtagDatJahr"]): int(r["anzahl"]) for r in rows if r["SexHundLang"] == "männlich"}
    w_map = {int(r["StichtagDatJahr"]): int(r["anzahl"]) for r in rows if r["SexHundLang"] == "weiblich"}
    maennlich = [m_map.get(j, 0) for j in jahre]
    weiblich = [w_map.get(j, 0) for j in jahre]
    return maennlich, weiblich


def fetch_anzahl_rassen(max_jahr: int) -> int:
    rows = sql_query(f'''
        SELECT COUNT(DISTINCT "Rasse1Text") as anzahl
        FROM "{RESOURCE_ID}"
        WHERE "StichtagDatJahr" = '{max_jahr}'
          AND "Rasse1Text" NOT LIKE '%Unbestimmt%'
    ''')
    return int(rows[0]["anzahl"])


def fetch_top_rassen(max_jahr: int, vorjahr: int, limit: int) -> list[dict]:
    rows_cur = sql_query(f'''
        SELECT "Rasse1Text", COUNT(*) as anzahl
        FROM "{RESOURCE_ID}"
        WHERE "StichtagDatJahr" = '{max_jahr}'
          AND "Rasse1Text" NOT LIKE '%Unbestimmt%'
        GROUP BY "Rasse1Text"
        ORDER BY anzahl DESC
        LIMIT {limit}
    ''')
    rassen_namen = [r["Rasse1Text"] for r in rows_cur]
    cur_map = {r["Rasse1Text"]: int(r["anzahl"]) for r in rows_cur}

    rows_prev = sql_query(f'''
        SELECT "Rasse1Text", COUNT(*) as anzahl
        FROM "{RESOURCE_ID}"
        WHERE "StichtagDatJahr" = '{vorjahr}'
          AND "Rasse1Text" IN ({in_list(rassen_namen)})
        GROUP BY "Rasse1Text"
    ''')
    prev_map = {r["Rasse1Text"]: int(r["anzahl"]) for r in rows_prev}

    return [
        {"rasse": name, "cur": cur_map[name], "prev": prev_map.get(name, 0)}
        for name in rassen_namen
    ]


def fetch_kreis_data(jahre: list[int]) -> dict:
    rassen_in = in_list(KREIS_RASSEN)
    rows = sql_query(f'''
        SELECT "StichtagDatJahr", "KreisLang", "Rasse1Text", COUNT(*) as anzahl
        FROM "{RESOURCE_ID}"
        WHERE "KreisLang" != 'Unbekannt (Stadt Zürich)'
          AND "Rasse1Text" IN ({rassen_in})
        GROUP BY "StichtagDatJahr", "KreisLang", "Rasse1Text"
        ORDER BY "KreisLang", "Rasse1Text", "StichtagDatJahr"
    ''')

    result = {}
    for r in rows:
        kreis = r["KreisLang"]
        rasse = r["Rasse1Text"]
        jahr = int(r["StichtagDatJahr"])
        anz = int(r["anzahl"])
        if kreis not in result:
            result[kreis] = {}
        if rasse not in result[kreis]:
            result[kreis][rasse] = {j: 0 for j in jahre}
        result[kreis][rasse][jahr] = anz

    kreise_sorted = sorted(result.keys(), key=lambda k: int(re.search(r'\d+', k).group()))
    output = {}
    for kreis in kreise_sorted:
        output[kreis] = {}
        for rasse in sorted(result[kreis].keys()):
            output[kreis][rasse] = [result[kreis][rasse].get(j, 0) for j in jahre]
    return output


def fetch_pyramid_data(rassen_namen: list[str], max_jahr: int) -> dict:
    top25 = rassen_namen[:PYRAMID_RASSEN_LIMIT]
    rassen_in = in_list(top25)
    rows = sql_query(f'''
        SELECT "Rasse1Text", "AlterVHundNum", "SexHundLang", COUNT(*) as anzahl
        FROM "{RESOURCE_ID}"
        WHERE "StichtagDatJahr" = '{max_jahr}'
          AND "Rasse1Text" IN ({rassen_in})
        GROUP BY "Rasse1Text", "AlterVHundNum", "SexHundLang"
        ORDER BY "Rasse1Text", "AlterVHundNum", "SexHundLang"
    ''')

    result = {}
    for r in rows:
        rasse = r["Rasse1Text"]
        alter_raw = int(r["AlterVHundNum"])
        alter_bin = min(alter_raw, MAX_ALTER)
        sex = "m" if r["SexHundLang"] == "männlich" else "w"
        anz = int(r["anzahl"])
        if rasse not in result:
            result[rasse] = {"m": [0] * (MAX_ALTER + 1), "w": [0] * (MAX_ALTER + 1)}
        result[rasse][sex][alter_bin] += anz

    name_map = {
        'Deutscher Zwergspitz/Pomeranian': 'Dt. Zwergspitz/Pomeranian',
        'Englischer Cocker Spaniel': 'Engl. Cocker Spaniel',
        'Wasserhund der Romagna': 'Wasserhund d. Romagna',
        'Australischer Schäferhund': 'Austral. Schäferhund',
        'Cavalier King Charles Spaniel': 'Cavalier King Charles',
    }

    output = {}
    for name in top25:
        if name in result:
            display = name_map.get(name, name)
            output[display] = result[name]
    return output


# --- HTML-Ersetzung ---

def js_array(values: list) -> str:
    return "[" + ",".join(str(v) for v in values) + "]"


def replace_between(html: str, marker_start: str, marker_end: str, new_content: str) -> str:
    pattern = re.escape(marker_start) + r'.*?' + re.escape(marker_end)
    return re.sub(pattern, marker_start + new_content + marker_end, html, count=1, flags=re.DOTALL)


def update_js_array(html: str, var_name: str, values: list) -> str:
    pattern = rf'(const {var_name} = )\[.*?\];'
    replacement = rf'\g<1>{js_array(values)};'
    return re.sub(pattern, replacement, html, count=1)


def build_rassen_js(top_rassen: list[dict], max_jahr: int, vorjahr: int) -> str:
    lines = []
    for r in top_rassen:
        escaped = r["rasse"].replace("'", "\\'")
        lines.append(f"  {{ rasse: '{escaped}', a25: {r['cur']}, a24: {r['prev']} }},")
    return "const rassen = [\n" + "\n".join(lines) + "\n];"


def build_kreis_js(kreis_data: dict) -> str:
    lines = ["const kreisData = {"]
    for kreis, rassen in kreis_data.items():
        lines.append(f"  '{kreis}': {{")
        for rasse, werte in rassen.items():
            escaped = rasse.replace("'", "\\'")
            lines.append(f"    '{escaped}': {js_array(werte)},")
        lines.append("  },")
    lines.append("};")
    return "\n".join(lines)


def build_pyramid_js(pyramid_data: dict) -> str:
    lines = ["const pyramidData = {"]
    for rasse, d in pyramid_data.items():
        escaped = rasse.replace("'", "\\'")
        lines.append(f"  '{escaped}': {{m:{js_array(d['m'])},w:{js_array(d['w'])}}},")
    lines.append("};")
    return "\n".join(lines)


def build_kpi_html(bestand_aktuell: int, bestand_start: int, anzahl_rassen: int, max_jahr: int, start_jahr: int) -> dict:
    zuwachs_abs = bestand_aktuell - bestand_start
    zuwachs_pct = (zuwachs_abs / bestand_start) * 100
    return {
        "bestand": f"{bestand_aktuell:,}".replace(",", " "),
        "bestand_jahr": str(max_jahr),
        "pct": f"+{zuwachs_pct:,.1f}".replace(",", " ").replace(".", ","),
        "abs": f"+{zuwachs_abs:,}".replace(",", " "),
        "start_jahr": str(start_jahr),
        "rassen": str(anzahl_rassen),
    }


def update_html(html: str, jahre, bestand, maennlich, weiblich, top_rassen,
                kreis_data, pyramid_data, kpi, max_jahr, vorjahr) -> str:
    html = update_js_array(html, "jahre", jahre)
    html = update_js_array(html, "bestand", bestand)
    html = update_js_array(html, "maennlich", maennlich)
    html = update_js_array(html, "weiblich", weiblich)

    # Rassen-Array
    old_rassen = re.search(r'const rassen = \[.*?\];', html, re.DOTALL).group()
    html = html.replace(old_rassen, build_rassen_js(top_rassen, max_jahr, vorjahr))

    # kreisData
    old_kreis = re.search(r'const kreisData = \{.*?\n\};', html, re.DOTALL).group()
    html = html.replace(old_kreis, build_kreis_js(kreis_data))

    # pyramidData
    old_pyramid = re.search(r'const pyramidData = \{.*?\n\};', html, re.DOTALL).group()
    html = html.replace(old_pyramid, build_pyramid_js(pyramid_data))

    # KPI-Kacheln
    html = re.sub(
        r'(<div class="value">)[\d\s]+(</div>\s*<div class="label">Hunde im Jahr )\d+',
        rf'\g<1>{kpi["bestand"]}\g<2>{kpi["bestand_jahr"]}',
        html, count=1
    )
    html = re.sub(
        r'(<div class="value">)\+[\d,]+\s*%(</div>\s*<div class="label">Zuwachs seit )\d+',
        rf'\g<1>{kpi["pct"]} %\g<2>{kpi["start_jahr"]}',
        html, count=1
    )
    html = re.sub(
        r'(<div class="value">)\+[\d\s]+(</div>\s*<div class="label">Mehr Hunde als )\d+',
        rf'\g<1>{kpi["abs"]}\g<2>{kpi["start_jahr"]}',
        html, count=1
    )
    html = re.sub(
        r'(<div class="value">)\d+(</div>\s*<div class="label">Verschiedene Rassen)',
        rf'\g<1>{kpi["rassen"]}\g<2>',
        html, count=1
    )

    # Untertitel-Jahre aktualisieren
    min_jahr = jahre[0]
    html = re.sub(
        r'(Anzahl registrierter Hunde per 31\. Dezember, )\d+ – \d+',
        rf'\g<1>{min_jahr} – {max_jahr}',
        html
    )
    html = re.sub(
        r'(nach Geschlecht des Hundes per 31\. Dezember, )\d+ – \d+',
        rf'\g<1>{min_jahr} – {max_jahr}',
        html
    )
    html = re.sub(
        r'(Bestand per 31\. Dezember )\d+( mit Veränderung)',
        rf'\g<1>{max_jahr}\g<2>',
        html
    )
    html = re.sub(
        r'(Anzahl 20)\d\d(</th>)',
        lambda m: f'{m.group(1)}{str(max_jahr)[2:]}{m.group(2)}',
        html, count=1
    )
    html = re.sub(
        r'(Anzahl 20)\d\d(</th>)',
        lambda m: f'{m.group(1)}{str(vorjahr)[2:]}{m.group(2)}',
        html, count=1
    )

    return html


def main():
    parser = argparse.ArgumentParser(description="Aktualisiert die Hunde-Daten in index.html")
    parser.add_argument("--dry-run", action="store_true", help="Änderungen nur anzeigen, nicht schreiben")
    args = parser.parse_args()

    print("Lade Daten von data.stadt-zuerich.ch ...")

    print("  - Jahresbestand ...")
    jahre, bestand = fetch_jahre_bestand()
    max_jahr = jahre[-1]
    vorjahr = jahre[-2]
    min_jahr = jahre[0]

    print(f"  - Geschlecht ({min_jahr}-{max_jahr}) ...")
    maennlich, weiblich = fetch_geschlecht(jahre)

    print(f"  - Anzahl Rassen ({max_jahr}) ...")
    anzahl_rassen = fetch_anzahl_rassen(max_jahr)

    print(f"  - Top {TOP_RASSEN_LIMIT} Rassen ({max_jahr} vs. {vorjahr}) ...")
    top_rassen = fetch_top_rassen(max_jahr, vorjahr, TOP_RASSEN_LIMIT)

    print(f"  - Kreis-Daten (13 Rassen x 12 Kreise x {len(jahre)} Jahre) ...")
    kreis_data = fetch_kreis_data(jahre)

    print(f"  - Alterspyramiden (Top {PYRAMID_RASSEN_LIMIT} Rassen) ...")
    rassen_namen = [r["rasse"] for r in top_rassen]
    pyr_data = fetch_pyramid_data(rassen_namen, max_jahr)

    kpi = build_kpi_html(bestand[-1], bestand[0], anzahl_rassen, max_jahr, min_jahr)

    print(f"\nErgebnisse:")
    print(f"  Zeitraum: {min_jahr}-{max_jahr}")
    print(f"  Bestand {max_jahr}: {bestand[-1]:,}")
    print(f"  Rassen: {anzahl_rassen}")
    print(f"  Top-Rasse: {top_rassen[0]['rasse']} ({top_rassen[0]['cur']})")

    print(f"\nAktualisiere {HTML_PATH.name} ...")
    html = HTML_PATH.read_text(encoding="utf-8")
    html_new = update_html(
        html, jahre, bestand, maennlich, weiblich,
        top_rassen, kreis_data, pyr_data, kpi, max_jahr, vorjahr
    )

    if html == html_new:
        print("Keine Änderungen nötig – Daten sind aktuell.")
        return

    if args.dry_run:
        print("[Dry-Run] Änderungen erkannt, aber nicht geschrieben.")
    else:
        HTML_PATH.write_text(html_new, encoding="utf-8")
        print("Fertig! index.html wurde aktualisiert.")


if __name__ == "__main__":
    main()
