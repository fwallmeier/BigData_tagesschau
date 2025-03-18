
<!-- Add title image -->

# [![Tagesschau Favicon](https://www.tagesschau.de/favicon.ico)](https://www.tagesschau.de/)  Tagesschau Archive Article Dataset
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg?style=flat)](https://www.python.org/downloads/release/python-360/)
[![HuggingFace Datasets](https://img.shields.io/badge/huggingface-datasets-orange.svg?style=flat)](https://huggingface.co/datasets/bjoernp/tagesschau-2018-2023)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)



This repository contains the code for scraping every article
from the Tagesschau.de archive. 

(OUTDATED, Scraped with the "old" scraping Process) Find a deduplicated version of the dataset
from 01.01.2018 to 26.04.2023 [on HuggingFace](https://huggingface.co/datasets/bjoernp/tagesschau-2018-2023).

## Dataset Information
CSV structure:

| Field           | Description                                       |
|-----------------|---------------------------------------------------|
| `date_api`      | Called API Date                                   |
| `page_api`      | Called API Page                                   |
| `date`          | Date of the article                               |
| `headline`      | Title of the article                              |
| `short_headline` | A short headline / Context                        |
| `short_text`    | A brief summary of the article                    |
| `link`          | The href of the article on tagesschau.de          |
| `label`         | Lables of the Article (e.g. Corona, Ukraine, USA) |
| `articleBody`   | The full text of the article                      |
| `datePublished` | Exaxt Publishing timestamp                        |
| `author`        | The full text of the article                      |


Size (Outdated):

The final dataset (2018-today) contains 225202 articles from 1942 days. Of these articles only
21848 are unique (Tagesschau often keeps articles in circulation for ~1 month). The total download
size is ~65MB. --> There is no circulation. If you call days before around March 2023, you always receive the
Data for a whole Month

## Usage

Install with:

```bash
pip install -r requirements.txt
```

Run with:

```bash
python scrape_tagesschau.py --mode fetch --start_date=2018-01-01 --end_date=2023-01-01 --output=tagesschau_240121.csv
```
```bash
python scrape_tagesschau.py --mode fetch --start_date=2018-01-01 --end_date=2023-01-01 --output=tagesschau_240121.csv
```

# Dokumentation

Dieses Script dient dazu, das Archiv der Tagesschau zu scrapen. Es bietet drei Modi, die über das Argument `--mode` ausgewählt werden:

## Modi

### collect
Sammelt Links zu Artikeln innerhalb des angegebenen Datumsbereichs.  
**Beispiel:**
```bash
python scrape_tagesschau.py --mode collect --start_date=YYYY-MM-DD --end_date=YYYY-MM-DD --links_file=meine_links.json
```

### collect_after
Erfasst fehlgeschlagene oder fehlerhafte Links, die in einer vorherigen Sammlung nicht korrekt verarbeitet wurden.  
**Beispiel:**
```bash
python scrape_tagesschau.py --mode collect_after --links_file=meine_links.json
```

### fetch
Lädt die Artikelinhalte der zuvor gesammelten Links. Dabei werden zunächst die Links aus der Datei (Standard: `links.json`) geladen und anschließend der parallele Abruf der Artikel mittels `asyncio` durchgeführt.  
**Beispiel:**
```bash
python scrape_tagesschau.py --mode fetch --links_file=meine_links.json --output=tagesschau_articles.csv
```

## Allgemeine Parameter

- **`--start_date`**  
  Startdatum des Scraping-Zeitraums im Format `YYYY-MM-DD`.  
  *Standardwert: `2023-10-01`*

- **`--end_date`**  
  Enddatum des Scraping-Zeitraums im Format `YYYY-MM-DD`.  
  *Standardwert: Heutiges Datum*

- **`--links_file`**  
  Pfad zur Datei, in der die gesammelten Links gespeichert bzw. geladen werden.  
  *Standardwert: `links.json`*

- **`--output`**  
  Pfad zur CSV-Datei, in der die heruntergeladenen Artikel gespeichert werden.  
  *Standardwert: `tagesschau_articles.csv`*
