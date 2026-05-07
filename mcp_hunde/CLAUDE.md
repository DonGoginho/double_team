# Projekt: Hundebestand Stadt Zürich – Interaktive Webseite

## Ziel

Einzelne HTML-Seite (`index.html`) mit interaktiven Grafiken zum Hundebestand der Stadt Zürich. Alle Daten stammen ausschliesslich vom OGD-Katalog der Stadt Zürich (data.stadt-zuerich.ch). Keine erfundenen Daten.

## Techstack

- Reines HTML/CSS/JS (kein Build-Tool, kein Framework)
- **Chart.js 4** via CDN (`https://cdn.jsdelivr.net/npm/chart.js@4`)
- **Wikipedia-API** (en.wikipedia.org) und **Wikimedia Commons API** für Rassenfotos
- Responsive Design, mobile-tauglich

## Datenquellen (OGD Stadt Zürich)

Alle Datensätze: CC0-Lizenz, Hundekontrolle der Stadtpolizei Zürich.

### 1. Jährlicher Hundebestand (Hauptdatensatz)

- **Dataset-ID**: `sid_stapo_hundebestand_od1001`
- **Resource-ID (Datastore)**: `f32e7c2a-2a37-4557-9cb4-38183ddf3f0d`
- **CSV**: `KUL100OD1001.csv`
- **URL**: https://data.stadt-zuerich.ch/dataset/sid_stapo_hundebestand_od1001
- **Aktualisierung**: jährlich (Stichtag 31. Dezember)
- **Zeitraum**: 2014 – 2025
- **Umfang**: ~103'600 Zeilen (eine Zeile pro Hund pro Jahr)
- **Wichtige Spalten**:
  | Spalte | Beschreibung |
  |---|---|
  | `StichtagDatJahr` | Jahr (2014–2025) |
  | `AnzHunde` | Immer 1 (jede Zeile = 1 Hund) |
  | `KreisLang` | Stadtkreis (Kreis 1–12) |
  | `QuarLang` | Statistisches Quartier |
  | `Rasse1Text` | Hunderasse (primär) |
  | `Rasse2Text` | Hunderasse (sekundär, bei Mischlingen) |
  | `Rassetyp1Lang` | Rassentypenliste (I, II, etc.) |
  | `SexHundLang` | Geschlecht des Hundes |
  | `AlterVHundLang` | Alter des Hundes |
  | `GebHundDatJahr` | Geburtsjahr des Hundes |
  | `HundefarbeText` | Farbe des Hundes |
  | `SexLang` | Geschlecht der haltenden Person |
  | `AlterV10Lang` | Altersgruppe der haltenden Person |
  | `HID` | Hunde-ID (anonymisiert, über Jahre stabil) |

### 2. Monatlicher Hundebestand

- **Dataset-ID**: `sid_stapo_aktueller_hundebestand_monat_od1003`
- **CSV**: `KUL100OD1003.csv`
- **URL**: https://data.stadt-zuerich.ch/dataset/sid_stapo_aktueller_hundebestand_monat_od1003
- **Aktualisierung**: monatlich
- **Gleiche Spaltenstruktur** wie der jährliche Datensatz

### 3. Hundenamen

- **Dataset-ID**: `sid_stapo_hundenamen_od1002`
- **CSV**: `KUL100OD1002.csv`
- **URL**: https://data.stadt-zuerich.ch/dataset/sid_stapo_hundenamen_od1002
- **Aktualisierung**: jährlich

## Datenzugriff

Daten werden über die CKAN Datastore-API abgefragt. Der MCP-Server `opendata-stzh-mcp (hayal)` stellt die Tools `zurich_datastore_sql` und `zurich_datastore_query` bereit.

**Hinweis**: Direkte SQL mit dem CSV-Dateinamen als Tabelle (`FROM "KUL100OD1001"`) schlägt fehl (HTTP 409). Immer die **Resource-UUID** verwenden.

### Beispiel-Abfragen

```sql
-- Hundebestand pro Jahr
SELECT "StichtagDatJahr", COUNT(*) as anzahl
FROM "f32e7c2a-2a37-4557-9cb4-38183ddf3f0d"
GROUP BY "StichtagDatJahr"
ORDER BY "StichtagDatJahr"

-- Hundebestand nach Geschlecht pro Jahr
SELECT "StichtagDatJahr", "SexHundLang", COUNT(*) as anzahl
FROM "f32e7c2a-2a37-4557-9cb4-38183ddf3f0d"
GROUP BY "StichtagDatJahr", "SexHundLang"
ORDER BY "StichtagDatJahr", "SexHundLang"

-- Top-Rassen für ein bestimmtes Jahr (ohne Unbestimmt)
SELECT "Rasse1Text", COUNT(*) as anzahl
FROM "f32e7c2a-2a37-4557-9cb4-38183ddf3f0d"
WHERE "StichtagDatJahr" = '2025'
  AND "Rasse1Text" NOT LIKE '%Unbestimmt%'
GROUP BY "Rasse1Text"
ORDER BY anzahl DESC
LIMIT 40

-- Top-Rassen pro Kreis und Jahr (für Kreis-Grafik)
SELECT "StichtagDatJahr", "KreisLang", "Rasse1Text", COUNT(*) as anzahl
FROM "f32e7c2a-2a37-4557-9cb4-38183ddf3f0d"
WHERE "KreisLang" != 'Unbekannt (Stadt Zürich)'
  AND "Rasse1Text" IN ('Labrador Retriever','Chihuahua', ...)
GROUP BY "StichtagDatJahr", "KreisLang", "Rasse1Text"
ORDER BY "KreisLang", "Rasse1Text", "StichtagDatJahr"

-- Alterspyramide pro Rasse (für Faceted Grid)
SELECT "Rasse1Text", "AlterVHundNum", "SexHundLang", COUNT(*) as anzahl
FROM "f32e7c2a-2a37-4557-9cb4-38183ddf3f0d"
WHERE "StichtagDatJahr" = '2025'
  AND "Rasse1Text" IN ('Labrador Retriever','Chihuahua', ...)
GROUP BY "Rasse1Text", "AlterVHundNum", "SexHundLang"
ORDER BY "Rasse1Text", "AlterVHundNum", "SexHundLang"

-- Anzahl verschiedene Rassen
SELECT COUNT(DISTINCT "Rasse1Text") as anzahl_rassen
FROM "f32e7c2a-2a37-4557-9cb4-38183ddf3f0d"
WHERE "StichtagDatJahr" = '2025'
  AND "Rasse1Text" NOT LIKE '%Unbestimmt%'
```

## Aufbau von index.html

Alles in einer einzigen Datei (CSS + JS inline). Reihenfolge der Elemente:

### 1. KPI-Kacheln (oben)
4 Kennzahlen: Bestand 2025 (10'241), Zuwachs seit 2014 (+40,5 %), absoluter Zuwachs (+2'951), Anzahl Rassen (308)

### 2. Grafik 1 – Zeitlicher Verlauf
Liniendiagramm, `chartBestand`, Daten in `bestand`-Array.

### 3. Grafik 2 – Geschlecht des Hundes
Zwei-Linien-Diagramm, `chartGeschlecht`, Daten in `maennlich`/`weiblich`-Arrays. Blau = männlich, Rot = weiblich.

### 4. Grafik 3 – Top-5-Rassen nach Stadtkreis
Multi-Line-Chart mit Dropdown (`kreisSelect`), `chartKreis`. Daten im Objekt `kreisData` (Kreis → Rasse → Array[12 Jahreswerte]). Die Funktion `updateKreisChart(kreis)` sortiert nach dem letzten Wert (2025), nimmt die Top 5 und baut das Chart. Standard-Kreis: Kreis 7. Enthält 5–10 Rassen pro Kreis (mehr als die Top 5, damit bei Rangwechseln über die Jahre die wichtigsten Rassen abgedeckt sind). 5 Farben: `kreisColors` = blau, rot, grün, gelb, lila.

### 5. Alterspyramiden (Faceted Grid 5x5)
25 Kleindiagramme, je eines pro Top-25-Rasse. Horizontale Balkendiagramme (Chart.js `indexAxis: 'y'`), männlich (blau) nach links, weiblich (rot) nach rechts. Alter 0–18+ (gekappt bei 18). Y-Achse umgekehrt (`reverse: true`): 0 unten, 18+ oben. Stacked bars (`stacked: true` auf beiden Achsen), damit männlich und weiblich exakt auf derselben Zeile liegen. Daten im Objekt `pyramidData` (Rasse → `{m: [19 Werte], w: [19 Werte]}`), männliche Werte werden als negative Zahlen gerendert. Funktion `buildPyramids()` erzeugt dynamisch die `<canvas>`-Elemente im `#pyramidGrid`. Jede Pyramide hat ihre eigene X-Skala (relativ zum Maximum der Rasse). Responsive: 5 Spalten > 900px, 3 Spalten > 550px, 2 Spalten darunter. Rassennamen im Grid leicht gekürzt (z.B. "Dt. Zwergspitz/Pomeranian", "Engl. Cocker Spaniel").

### 6. Rassentabelle (Top 40)
Sortierbar (Klick auf Spaltenüberschriften), filterbar (Suchfeld). Spalten: Rang, Rasse, Anzahl 2025, Anzahl 2024, Veränderung (farbige Badges).

**Rassen-Features:**
- **Wikipedia-Links**: Jeder Rassenname ist ein `<a>` zu de.wikipedia.org (`wikiLinks`-Objekt)
- **Hover-Fotos**: Globaler Tooltip (`<div id="rasseTooltip">`, `position: fixed`, folgt dem Mauszeiger)

### 7. Footer
Quellenangabe mit Link zum OGD-Datensatz.

## Rassenbilder-System

1. **`wikiMap`** – Zuordnung deutscher Rassennamen → englische Wikipedia-Artikeltitel (für Rassen mit Wikipedia-Seitenbild)
2. **`ladeRassenBilder()`** ruft beim Seitenaufruf die **Wikipedia pageimages API** auf (Batches à 10 Titel). Speichert Thumbnail-URLs in `rassenBilder`, indexiert nach **deutschem Rassennamen** (via Reverse-Mapping `wikiTitelZuRasse`)
3. Rassen ohne Wikipedia-Seitenbild (Chihuahua, Kleinpudel, Zwergpudel, Toypudel, Grosspudel) werden über die **Wikimedia Commons Search API** gesucht
4. **`getRasseBild(rasse)`** – einfacher Lookup `rassenBilder[rasse]`
5. Tooltip-Events (`mouseover`/`mousemove`/`mouseout`) auf `#rassenBody`

**Neue Rasse hinzufügen (3 Stellen):**
1. Eintrag in `rassen`-Array (mit a25/a24-Werten)
2. Eintrag in `wikiMap` (englischer Wikipedia-Artikelname) — ODER Eintrag in `commonsQueries` im `ladeRassenBilder()` falls kein Wikipedia-Seitenbild existiert
3. Eintrag in `wikiLinks` (deutscher Wikipedia-Link)

## Wikipedia-Links

Objekt `wikiLinks`: Alle 40 Rassen verlinken auf de.wikipedia.org. Pudelarten → "Pudel", Dackelarten → "Dackel", Bulldog → "Englische Bulldogge", Kurzhaariger Ungarischer Vorstehhund → "Magyar Vizsla".

## Design-Regeln

- Alle Y-Achsen beginnen bei 0
- Farbschema: Blau (`#0d47a1` / `#1565c0`) auf hellem Grau (`#f5f7fa`)
- Karten-Layout mit `border-radius: 12px` und leichtem Schatten
- Responsive Grid für KPI-Kacheln
- Rassenlinks: dezente gestrichelte Unterlinie, blau bei Hover
- Footer mit Quellenangabe und Link zum OGD-Datensatz

## Ideen für weitere Grafiken

- Gesamtbestand nach Stadtkreis (Balkendiagramm oder Karte)
- Beliebteste Hundenamen (aus Datensatz `sid_stapo_hundenamen_od1002`)
- Profil der Hundehaltenden (Alter, Geschlecht aus `AlterV10Lang`, `SexLang`)
- Hundefarben-Verteilung (aus `HundefarbeText`)
- Hunde pro Einwohner nach Quartier (erfordert Bevölkerungsdaten)
- Rassentypenliste-Verteilung (aus `Rassetyp1Lang`: Liste I, II, kein Eintrag)
