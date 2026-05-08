# Geburten-Verlauf Dashboard

Interaktives HTML-Dashboard: Geburten pro Tag in der Stadt Zürich seit dem Muttertag 2025 (11. Mai 2025).

## Kontext

Erstellt für einen Social-Media-Post zum **Muttertag 2026** der Statistik Stadt Zürich. Das Dashboard zeigt verschiedene Auswertungen der täglichen Geburtenzahlen.

## Dateien

- `geburten_verlauf.html` — Standalone-HTML-Dashboard, keine weiteren Abhängigkeiten (CDN-Libraries)

## Datensatz

- **Name:** Geburten nach Tag, Stadtquartier, Geschlecht und Herkunft
- **OGD-ID:** `bev_tag_geburten_quartier_geschl_ag_herkunft_od4031`
- **Resource-UUID:** `38907592-5c65-4ff2-bbaf-762b4569edde`
- **URL:** https://data.stadt-zuerich.ch/dataset/bev_tag_geburten_quartier_geschl_ag_herkunft_od4031
- **Lizenz:** CC0
- **Aktualisierung:** monatlich
- **Hinweis:** Daten des aktuellsten Jahres sind provisorisch

### Relevante Spalten

| Spalte | Beschreibung |
|---|---|
| `GueltigAbDatJahr` / `GueltigAbDatMM` / `GueltigAbDatDD` | Datum (Jahr, Monat, Tag) |
| `SexLang` | Geschlecht (`männlich`, `weiblich`) |
| `HerkunftLang` | Herkunft (`Schweizer*in`, `Ausländer*in`) |
| `KreisLang` | Stadtkreis (`Kreis 1` bis `Kreis 12`) |
| `QuarLang` | Stadtquartier |
| `AnzGebuWir` | Anzahl Geburten (wirtschaftliche Wohnbevölkerung) |

### Datenbezug

Die Daten werden live geladen:
1. **Primär:** CKAN SQL-API (`datastore_search_sql`) — aggregiert serverseitig
2. **Fallback:** CSV-Download mit clientseitigem Parsing via PapaParse

## Dashboard-Inhalte

1. **KPI-Leiste** — Total Geburten, Anzahl Tage, Tagesdurchschnitt, Maximum (Datum)
2. **Liniengrafik Total** — Geburten pro Tag, Top-10-Tage hervorgehoben (Magenta), gestrichelte Durchschnittslinie
3. **Nach Geschlecht** — Gestapeltes Liniendiagramm (männlich/weiblich)
4. **Nach Herkunft** — Gestapeltes Liniendiagramm (Schweizer\*in/Ausländer\*in)
5. **Nach Stadtkreis** — Balkendiagramm, alle 12 Kreise
6. **Tabelle Top 10** — Geburtenstärkste Tage mit Datum und Wochentag, sortierbar
7. **Tabelle Stadtkreis** — Aufschlüsselung nach Geschlecht und Herkunft pro Kreis, sortierbar

## Design: zueriplots / zuericolors / zueritheme

Gestaltet nach dem SSZ-Visualisierungssystem (Web-Modus).
Referenz: https://github.com/StatistikStadtZuerich/zueriplots

### Farbpaletten

```
qual6:   #3431DE, #DB247D, #1F9E31, #FBB900, #23C3F1, #FF720C
qual12:  #3431DE, #0A8DF6, #23C3F1, #7B4FB7, #DB247D, #FB737E, #007C78, #1F9E31, #99C32E, #9A5B01, #FF720C, #FBB900
Gender:  weiblich #349894 (Teal), männlich #FFD736 (Gold)
Herkunft: Schweizer*in #3431DE (Blau), Ausländer*in #FF720C (Orange)
Grau:    #FAFAFA, #EAEAEA, #D6D6D6, #B8B8B8, #7C7C7C, #545454
```

### Anwendung im Dashboard

- **Hauptlinie:** `#3431DE` (qual6 Blau)
- **Top-10-Punkte:** `#DB247D` (Magenta), nur Rand, keine Füllung
- **Durchschnittslinie:** `#7C7C7C` (Grau), gestrichelt
- **Stadtkreis-Balken:** qual12-Palette (12 Farben)
- **Geschlecht-Flächen:** Gender-Palette
- **Herkunft-Flächen:** Blau/Orange

### Typografie und Layout

- Font: `Helvetica Neue`, Fallback `Arial`, `sans-serif`
- Textfarbe: `#737373` (Web-Modus), Titel `#020304`
- Grid: nur Y-Achse, Farbe `#D6D6D6`, Linienstärke 0.5px
- X-Achse: keine Gridlines, dezente Achslinie
- Legende: unten, mit Point-Style
- Tooltips: weisser Hintergrund, #D6D6D6 Rahmen
- Chart-Höhen: 350px (Hauptchart), 300px (Nebencharts)
- KPI-Karten: blaue Akzentlinie links (#3431DE)
- Tabellen: Uppercase-Header, #EAEAEA-Trennlinien

## Libraries (via CDN)

- Chart.js 4.x — Diagramme
- chartjs-adapter-date-fns 3.x — Zeitachse
- PapaParse 5.x — CSV-Fallback

## Erweiterungsideen

- Zeitraumfilter (Datepicker)
- Vergleich mit Vorjahr
- Quartier-Ebene (Daten vorhanden, aber noch nicht visualisiert)
- Export als PNG/SVG
