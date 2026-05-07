# Hundebestand Stadt Zuerich - Interaktive Webseite

Interaktive Webseite mit Grafiken und Tabellen zum Hundebestand der Stadt Zuerich. Alle Daten stammen ausschliesslich vom [OGD-Katalog der Stadt Zuerich](https://data.stadt-zuerich.ch) (CC0-Lizenz, Hundekontrolle der Stadtpolizei Zuerich).

## Inhalt der Webseite

- **Kennzahlen**: Aktueller Bestand, Zuwachs seit 2014, Anzahl Rassen
- **Zeitlicher Verlauf**: Hundebestand 2014-2025 als Liniendiagramm
- **Geschlechterverteilung**: Maennliche vs. weibliche Hunde im Zeitverlauf
- **Top-Rassen nach Stadtkreis**: Interaktive Grafik mit Kreisauswahl (Dropdown)
- **Alterspyramiden**: 25 Kleindiagramme (5x5 Grid) fuer die beliebtesten Rassen
- **Rassentabelle**: Top 40 Rassen, sortier- und filterbar, mit Wikipedia-Links und Vorschaubildern beim Hovern

## Dateien

| Datei | Beschreibung |
|---|---|
| `index.html` | Die komplette Webseite (HTML, CSS, JS alles inline) |
| `update_data.py` | Python-Script zur automatischen Datenaktualisierung |
| `CLAUDE.md` | Technische Dokumentation fuer die Weiterentwicklung mit Claude |
| `README.md` | Diese Datei |

## Datenquelle

**Datensatz**: [Hundebestand der Stadt Zuerich](https://data.stadt-zuerich.ch/dataset/sid_stapo_hundebestand_od1001)
- Aktualisierung: jaehrlich (Stichtag 31. Dezember)
- Zeitraum: 2014 bis heute
- Umfang: ca. 103'600 Zeilen (eine Zeile pro Hund pro Jahr)
- Resource-ID: `f32e7c2a-2a37-4557-9cb4-38183ddf3f0d`

## Daten aktualisieren

Wenn die Stadt Zuerich neue Daten veroeffentlicht (jeweils nach dem 31. Dezember), kann die Webseite mit dem Python-Script automatisch aktualisiert werden.

### Voraussetzungen

- Python 3.8 oder neuer
- Bibliothek `requests` (`pip install requests`)
- Internetzugang (fuer die CKAN Datastore API)

### Schritt fuer Schritt

1. **Pruefen, ob neue Daten verfuegbar sind** (optional):
   Auf [data.stadt-zuerich.ch](https://data.stadt-zuerich.ch/dataset/sid_stapo_hundebestand_od1001) nachsehen, ob ein neues Jahr vorhanden ist.

2. **Dry-Run ausfuehren** (zeigt Aenderungen, schreibt nichts):
   ```
   python update_data.py --dry-run
   ```
   Falls Python nicht direkt im PATH ist (z.B. bei Anaconda):
   ```
   "C:\Users\sszsim\AppData\Local\anaconda3\python.exe" update_data.py --dry-run
   ```

3. **Daten aktualisieren** (ueberschreibt `index.html`):
   ```
   python update_data.py
   ```

4. **Ergebnis pruefen**: `index.html` im Browser oeffnen und Grafiken/Tabelle kontrollieren.

### Was wird aktualisiert?

Das Script fragt 6 Datenbereiche von der CKAN API ab und ersetzt die entsprechenden Stellen in `index.html`:

| Bereich | Was passiert |
|---|---|
| Jahresbestand | Arrays `jahre` und `bestand` werden neu geschrieben |
| Geschlechterverteilung | Arrays `maennlich` und `weiblich` werden aktualisiert |
| Anzahl Rassen | KPI-Kachel wird aktualisiert |
| Top-40-Rassen | Array `rassen` mit aktuellen und Vorjahreswerten |
| Kreisdaten | Objekt `kreisData` (13 Rassen x 12 Kreise) |
| Alterspyramiden | Objekt `pyramidData` (Top 25 Rassen) |
| KPI-Kacheln | Bestand, Zuwachs (%), absoluter Zuwachs, Rassenanzahl |
| Untertitel | Jahreszahlen in allen Grafiktiteln |

### Was wird NICHT automatisch aktualisiert?

- **Wikipedia-Links** (`wikiLinks`): Muessen bei neuen Rassen in den Top 40 manuell ergaenzt werden
- **Rassenbilder** (`wikiMap`): Muessen bei neuen Rassen manuell ergaenzt werden
- **Kreis-Rassenliste** (`KREIS_RASSEN` in `update_data.py`): Die 13 Rassen fuer die Kreisgrafik sind fix hinterlegt. Falls sich die beliebtesten Rassen stark veraendern, sollte diese Liste angepasst werden.

## Techstack

- Reines HTML/CSS/JS (kein Build-Tool, kein Framework)
- [Chart.js 4](https://www.chartjs.org/) via CDN fuer alle Grafiken
- Wikipedia API und Wikimedia Commons API fuer Rassenfotos
- Responsive Design (Desktop, Tablet, Mobile)

## Weiterentwicklung

Details zur Codestruktur, allen Datenquellen und Ideen fuer weitere Grafiken finden sich in `CLAUDE.md`. Diese Datei ist speziell darauf ausgelegt, mit Claude Code weiterentwickeln zu koennen.
