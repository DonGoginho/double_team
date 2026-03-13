# plausis_made_easy

Automatischer Generator für Plausibilitätsprüfungs-Notebooks (Plausi-Notebooks) der OGD-Datensätze der Stadt Zürich.

## Übersicht

```
plausis_made_easy/
├── generate_plausi_notebook.py   ← Generator-Script (dieses Dokument)
├── mypy_plausis_orig/            ← Originale, manuell erstellte Notebooks (Vorlagen)
│   ├── 0_scripts/                ← Hilfsfunktionen (Dataloading, Visualisierung)
│   ├── plausi_bev_jahresupdates/ ← Notebooks für jährliche Datensätze
│   └── plausi_bev_monatsdaten/   ← Notebooks für monatliche Datensätze
└── mypy_plausis_vandamme/        ← KI-generierte Notebooks (Output)
    ├── 0_scripts/                ← Kopie der Hilfsfunktionen
    └── plausi_bev_jahresupdates/ ← Generierte Notebooks
```

## Voraussetzungen

```bash
pip install pandas requests
```

## Verwendung

### Neues Notebook erstellen

```bash
cd plausis_made_easy

python generate_plausi_notebook.py \
    --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \
    --dataset_name BEV336OD3361
```

### Bestehendes Notebook aktualisieren

Aktualisiert das Datum im Header und erkennt neue/geänderte Spalten:

```bash
python generate_plausi_notebook.py \
    --update \
    --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \
    --dataset_name BEV336OD3361
```

## Parameter

| Parameter | Pflicht | Default | Beschreibung |
|---|---|---|---|
| `--package_name` | ✓ | — | CKAN Package-Name (= Slug in der URL des Datakatalogs) |
| `--dataset_name` | ✓ | — | CSV-Dateiname ohne Endung |
| `--status` | | `int` | Datenumgebung: `int` (Integration) oder `prod` (Produktion) |
| `--source` | | `web` | Datenquelle: `web` (CKAN), `ld` (Linked Data), `dropzone` |
| `--output_dir` | | `mypy_plausis_vandamme/plausi_bev_jahresupdates` | Zielverzeichnis |
| `--update` | | — | Flag: bestehendes Notebook aktualisieren statt neu erstellen |
| `--existing_nb` | | auto | Pfad zum bestehenden Notebook (nur mit `--update`, sonst automatisch gesucht) |

### Woher kommen package_name und dataset_name?

Beide Werte stehen in der URL des Datakatalogs:

```
https://data.stadt-zuerich.ch/dataset/bev_bestand_jahr_quartier_nationalitaet_od3361
                                       ↑ package_name

https://data.stadt-zuerich.ch/dataset/.../download/BEV336OD3361.csv
                                                    ↑ dataset_name (ohne .csv)
```

### Wann welche --source?

| Quelle | Wann nutzen |
|---|---|
| `web` | Standardfall – Daten über CKAN-Webinterface |
| `ld` | Linked-Data-Datasets (dataset_name wird dann nicht für die URL verwendet) |
| `dropzone` | Interne Dropzone-Daten (Spaltenanalyse nicht möglich, Fallback auf Standardwerte) |

## Was der Generator macht

1. **Metadaten holen** – Titel und Beschreibung via CKAN API
2. **Spalten analysieren** – Ersten 300 Zeilen des Datensatzes herunterladen und Spalten klassifizieren:
   - `*Lang` → Dimensions-Spalten (Labels, für Gruppierung und Visualisierung)
   - `*Sort` → Sortier-Spalten
   - `*Cd` → Code-Spalten
   - `Anz*` / `Sum*` → Wert-Spalten (werden aggregiert)
   - `StichtagDat*` → Datum-Spalten
3. **Notebook generieren** – Zellen regelbasiert aufbauen:
   - **Fixe Zellen** (identisch für alle Notebooks): Imports, Custom Functions, SSL-Setup, Settings, Züri-Farbpaletten, Zeit-Variablen
   - **Variable Zellen** (aus Spalten-Schema): Data Loading, Feature Engineering, Gruppierungen, Pivot-Tabellen, Visualisierungen
4. **Speichern** als `.ipynb` in `mypy_plausis_vandamme/plausi_bev_jahresupdates/`

### Generierte Visualisierungen

Pro Notebook werden folgende Visualisierungen erstellt:
- **Altair Liniendiagramm** – zeitlicher Verlauf nach primärer Dimension
- **Kombinierte Altair-Grafik** – mehrere Teilgrafiken zusammengesetzt
- **Seaborn Barchart** – Vergleich im aktuellsten Jahr
- **Stacked Bar Chart** – gestapelter Balken über alle Jahre
- **Seaborn FacetGrid** – Verlauf pro Ausprägung der primären Dimension
- **Plotly Treemap (alle Jahre)** – hierarchische Darstellung über Zeit
- **Plotly Treemap (aktuellstes Jahr)** – Verteilung im letzten Jahr

## Beispiele

```bash
# Jährlicher Datensatz, INT-Umgebung (Standard)
python generate_plausi_notebook.py \
    --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \
    --dataset_name BEV336OD3361

# PROD-Umgebung
python generate_plausi_notebook.py \
    --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \
    --dataset_name BEV336OD3361 \
    --status prod

# Linked Data
python generate_plausi_notebook.py \
    --package_name BEV336OD3361 \
    --dataset_name "" \
    --source ld

# In ein anderes Verzeichnis
python generate_plausi_notebook.py \
    --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \
    --dataset_name BEV336OD3361 \
    --output_dir ./meine_notebooks

# Update (Datum + neue Spalten)
python generate_plausi_notebook.py \
    --update \
    --package_name bev_bestand_jahr_quartier_nationalitaet_od3361 \
    --dataset_name BEV336OD3361
```
