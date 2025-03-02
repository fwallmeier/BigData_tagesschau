#!/usr/bin/env python3
import asyncio
import aiohttp
import bs4
import urllib.request
import argparse
import pandas as pd
import threading
import concurrent.futures
import datetime
import json

from aiohttp import ClientResponseError
from tqdm import tqdm
import os



def load_content(date, page=1):
    """
    Lädt den HTML-Inhalt der Tagesschau-Archivseite für ein gegebenes Datum und pageIndex.
    """
    url = f"https://www.tagesschau.de/archiv?datum={date}&pageIndex={page}"
    response = urllib.request.urlopen(url).read()
    soup = bs4.BeautifulSoup(response, features="html.parser")
    return soup


def get_links_from_page(date, page):
    """
    Ruft die Artikel-Metadaten (u.a. Link, Headline, Kurztext) von der Archivseite ab.
    Dabei wird geprüft, ob es sich um eine Monatsübersicht handelt (z. B. "Dezember 2022")
    statt um einen Tag (z. B. "01. Dezember 2024").

    Rückgabe:
      - Eine Liste von Dictionaries mit den extrahierten Metadaten
      - Ein Flag (monthly_summary), das angibt, ob es sich um eine Monatsübersicht handelt.
    """
    soup = load_content(date, page)

    # Prüfen, ob das Element archive__headline vorhanden ist und ob es eine Monatsübersicht signalisiert.
    monthly_summary = False
    headline_elem = soup.find(class_="archive__headline")
    if headline_elem:
        headline_text = headline_elem.get_text(strip=True)
        if headline_text and not headline_text[0].isdigit():
            monthly_summary = True

    content_div = soup.find('div', id='content')
    if not content_div:
        return [], monthly_summary
    children = content_div.find_all("div", class_="copytext-element-wrapper__vertical-only")
    if not children:
        return [], monthly_summary

    links = []
    # Wie im Originalcode werden ab dem dritten Element die eigentlichen Artikelinfos erwartet.
    for child in children[2:]:
        a_elem = child.find("a", class_="teaser-right__link")
        link = a_elem['href'] if a_elem and a_elem.has_attr('href') else ""
        headline_elem = child.find("span", class_="teaser-right__headline")
        headline = headline_elem.get_text(strip=True) if headline_elem else ""
        short_headline_elem = child.find("span", class_="teaser-right__labeltopline")
        short_headline = short_headline_elem.get_text(strip=True) if short_headline_elem else ""
        short_text_elem = child.find("p", class_="teaser-right__shorttext")
        short_text = short_text_elem.get_text(strip=True) if short_text_elem else ""
        date_elem = child.find("div", class_="teaser-right__date")
        date_api = date_elem.get_text(strip=True) if date_elem else None
        links.append({
            "date_api": date,
            "page_api": page,
            "date": date_api if date_api else date,
            "headline": headline,
            "short_headline": short_headline,
            "short_text": short_text,
            "link": link
        })
    return links, monthly_summary


# Gemeinsame Struktur, um verarbeitete Monate zu speichern.
# Schlüssel: (Jahr, Monat), Wert: True, wenn ein Monats‑Summary bereits erkannt wurde.
processed_months = {}
processed_months_lock = threading.Lock()

# Globaler Lock zum synchronen Schreiben in die Datei
file_write_lock = threading.Lock()

# Globale Liste und Lock zum Tracken der aktiven Monate
active_months = []
active_months_lock = threading.Lock()


def process_month(year, month, days, links_filename, page = 1):
    """
    Verarbeitet alle Tage eines Monats sequentiell.
    Sobald innerhalb eines Monats ein Monats‑Summary erkannt wird, werden die restlichen Tage übersprungen.
    Zudem wird der aktuell verarbeitete Monat in der globalen Liste active_months geführt.
    """
    month_str = f"{month:02d}.{year}"
    with active_months_lock:
        active_months.append(month_str)

    monthly_summary_detected = False
    month_links = []

    for day in days:
        if monthly_summary_detected:
            # Falls bereits ein Monats‑Summary gefunden wurde, werden die restlichen Tage übersprungen.
            break

        date_str = day.strftime("%Y-%m-%d")
        daily_links = []

        # Seiten des Tages sequentiell abarbeiten.
        while True:
            try:
                links, monthly_summary = get_links_from_page(date_str, page)
            except Exception as e:
                print(f"Fehler bei {date_str} Seite {page}: {e}")
                break

            if not links:
                break  # Keine weiteren Seiten vorhanden

            daily_links.extend(links)

            if monthly_summary:
                monthly_summary_detected = True
            page += 1

        month_links.extend(daily_links)
    # Inkremetelles Speichern der gefundenen Links
    if month_links:
        with file_write_lock:
            with open(links_filename, "a", encoding="utf-8") as f:
                for entry in month_links:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # Monat als nicht mehr aktiv markieren
    with active_months_lock:
        active_months.remove(month_str)

    return month_links


def collect_links(start_date, end_date, links_filename):
    """
    Sammelt für alle Monate im angegebenen Zeitraum die verfügbaren Artikel-Links.
    - Die Tage werden zunächst nach (Jahr, Monat) gruppiert.
    - Für jeden Monat werden die Tage sequentiell abgearbeitet.
    - Falls innerhalb eines Monats ein Monats‑Summary erkannt wird, wird der Rest des Monats übersprungen.
    - Die Verarbeitung der Monate erfolgt parallel, und der Fortschritt (aktuelle aktive Monate)
      wird mittels tqdm angezeigt.
    """
    all_links = []

    # Gruppiere die Tage nach (Jahr, Monat)
    month_groups = {}
    current_date = start_date
    while current_date <= end_date:
        ym = (current_date.year, current_date.month)
        month_groups.setdefault(ym, []).append(current_date)
        current_date += datetime.timedelta(days=1)

    # Parallelverarbeitung der Monate (maximal 5 Threads, anpassbar)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for (year, month), days in month_groups.items():
            futures[executor.submit(process_month, year, month, days, links_filename)] = f"{month:02d}.{year}"

        with tqdm(total=len(futures), desc="Verarbeite Monate", unit="Monat") as pbar:
            for future in concurrent.futures.as_completed(futures):
                all_links.extend(future.result())
                # Fortschritt aktualisieren: Zeige die aktuell aktiven Monate an
                with active_months_lock:
                    pbar.set_postfix(active=active_months.copy())
                pbar.update(1)

    return all_links

def collect_links_with_error(links_file):
    with open("error_days.txt", "r", encoding="utf-8") as f:
        for l in f.readlines():
            month_groups = {}
            infos = l.split(";")
            start_date = datetime.datetime.strptime(infos[0], "%Y-%m-%d").date()
            current_date = start_date
            while True:
                ym = (current_date.year, current_date.month)
                month_groups.setdefault(ym, []).append(current_date)
                current_date += datetime.timedelta(days=1)
                if current_date.month > start_date.month:
                    break

            for (year, month), days in month_groups.items():
                process_month(year, month, days, links_file, page=int(infos[1]))



async def fetch_article(session, entry):
    """
    Lädt asynchron den Artikel zu einem übergebenen Link und extrahiert erweiterte Metadaten,
    einschließlich der Taglist (keywords).
    """
    url = "https://www.tagesschau.de" + entry["link"]
    try:
        async with session.get(url) as response:
            text = await response.text()
            soup = bs4.BeautifulSoup(text, features="html.parser")
            scripts = soup.find_all("script", type="application/ld+json")

            label_elem = soup.find("span", class_="label label--standard-primary")
            entry["label"] = label_elem.get_text(strip=True) if label_elem else ""

            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        for item in data:
                            if item.get("@type") == "NewsArticle":
                                entry["articleBody"] = item.get("articleBody", "")
                                entry["datePublished"] = item.get("datePublished", "")
                                entry["author"] = item.get("author", "")
                                entry["description"] = item.get("description", "")
                                entry["taglist"] = item.get("keywords", [])  # Hier wird die Taglist gespeichert
                                break
                    elif data.get("@type") == "NewsArticle":
                        entry["articleBody"] = data.get("articleBody", "")
                        entry["datePublished"] = data.get("datePublished", "")
                        entry["datePublished"] = data.get("dateModified", "")
                        entry["author"] = data.get("author", "")
                        entry["description"] = data.get("description", "")
                        entry["taglist"] = data.get("keywords", [])  # Falls `keywords` existiert, speichern
                        break
                except Exception:
                    continue
            return entry
    except Exception as e:
        print(f"Error fetching article {url}: {e}")
        entry["taglist"] = []
        entry["taglist"] = []
        return None


async def fetch_article_with_retry(session, entry, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            result = await fetch_article(session, entry)
            if not result:
                raise Exception
            return result
        except Exception as e:
            wait_time = 10 + 2 ** retries  # Exponentielles Backoff
            print(f"Rate limit erreicht. Warte {wait_time} Sekunden...")
            await asyncio.sleep(wait_time)
            retries += 1
    print("Maximale Anzahl an Wiederholungen erreicht, breche ab.")
    return None


async def fetch_all_articles(entries, concurrency):
    """
    Führt den asynchronen Abruf aller Artikel-Bodies mit einer maximalen Parallelität (concurrency) durch.
    Lädt jeweils 3000 Einträge, pausiert dann 20 Sekunden und nutzt eine neue ClientSession.
    """
    results = []
    batch_size = 3000

    for i in range(0, len(entries), batch_size):
        batch = entries[i:i + batch_size if i+batch_size < len(entries) else len(entries)-1]
        connector = aiohttp.TCPConnector(limit=concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [fetch_article_with_retry(session, entry) for entry in batch]
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks),
                               desc=f"Fetching batch {i // batch_size + 1}"):
                result = await future
                if not result:
                    break
                results.append(result)

        if i + batch_size < len(entries):  # Nur pausieren, wenn noch weitere Batches zu verarbeiten sind
            await asyncio.sleep(20)
        save_articles(results, f"cache_{i}.csv")

    return results

def load_links(links_filename):
    """
    Lädt die bereits gespeicherten Links (und zugehörige Metadaten) aus der links-Datei.
    """
    entries = []
    if os.path.exists(links_filename):
        with open(links_filename, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if not entry["link"].startswith("http"):
                        entries.append(entry)
                except:
                    continue

    return entries


def save_articles(articles, output_filename):
    """
    Speichert die Artikel inklusive aller Metadaten als CSV (oder alternativ als Pickle).
    """
    df = pd.DataFrame(articles)
    if output_filename.endswith(".csv"):
        df = df.sort_values(by='date_api')
        df.to_csv(output_filename, sep="\t", index=False)
    else:
        df.to_pickle(output_filename)
        print("Saved as pickle file.")


def main():
    parser = argparse.ArgumentParser(description="Tagesschau Archiv Scraper")
    parser.add_argument("--mode", type=str, choices=["collect", "collect_after","fetch"], required=True,
                        help="Modus: 'collect' sammelt Links, 'collect_after' sammelt fehlerhafter Links, 'fetch' lädt Artikel-Bodies")
    parser.add_argument("--start_date", type=str, default="2023-10-01", help="Startdatum im Format YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, default=datetime.date.today().strftime("%Y-%m-%d"),
                        help="Enddatum im Format YYYY-MM-DD")
    parser.add_argument("--links_file", type=str, default="links.json",
                        help="Datei zum Speichern der gesammelten Links")
    parser.add_argument("--output", type=str, default="tagesschau_articles.csv", help="Ausgabedatei für Artikel")
    args = parser.parse_args()

    start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()

    if args.mode == "collect":
        print("Sammle Links …")
        collect_links(start_date, end_date, args.links_file)
        print(f"Links wurden inkrementell in {args.links_file} gespeichert.")
    if args.mode =="collect_after":

        print("Sammle Links …, die zuvor Fehlerhaft waren")
        collect_links_with_error(args.links_file)
        print(f"Links wurden inkrementell in {args.links_file} gespeichert.")

    elif args.mode == "fetch":
        print("Lade Links aus der Datei …")
        entries = load_links(args.links_file)
        print(f"{len(entries)} Links wurden geladen.")
        print("Lade Artikel parallel mit asyncio …")
        articles = asyncio.run(fetch_all_articles(entries, concurrency=40))
        save_articles(articles, args.output)
        print(f"Artikel wurden in {args.output} gespeichert.")


if __name__ == "__main__":
    main()
