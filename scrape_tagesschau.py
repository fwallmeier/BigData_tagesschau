#!/usr/bin/env python3
import asyncio
import aiohttp
import bs4
import urllib.request
import datetime
import json
import argparse
import pandas as pd
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
            "date": date_api if date_api else date,
            "headline": headline,
            "short_headline": short_headline,
            "short_text": short_text,
            "link": link
        })
    return links, monthly_summary


import concurrent.futures

def collect_links(start_date, end_date, links_filename):

    """
    Sammelt für alle Tage im angegebenen Zeitraum die verfügbaren Artikel-Links.
    Parallelisiert das Abrufen der Seiteninhalte für eine schnellere Verarbeitung.
    """
    processed_months = set()  # Set (Jahr, Monat), um doppelte Monatsübersichten zu vermeiden
    all_links = []
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    error_file = "error_day.json"

    with tqdm(total=total_days, desc="Verarbeite Tage", unit="Tag") as pbar, concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            year_month = (current_date.year, current_date.month)

            if year_month in processed_months:
                current_date += datetime.timedelta(days=1)
                pbar.update(1)
                continue

            futures[executor.submit(get_links_from_page, date_str, 1)] = (date_str, 1, year_month)
            current_date += datetime.timedelta(days=1)

        for future in concurrent.futures.as_completed(futures):
            date_str, page, year_month = futures[future]
            try:
                links, monthly_summary = future.result()
                if links:
                    all_links.extend(links)
                    with open(links_filename, "a", encoding="utf-8") as f:
                        for entry in links:
                            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                if monthly_summary:
                    processed_months.add(year_month)
            except Exception as e:
                error_data = {"date": date_str, "page": page, "error": str(e)}
                print(f"Fehler beim Verarbeiten von {date_str} Seite {page}: {e}")

                # Fehler in JSON-Datei speichern
                if os.path.exists(error_file):
                    with open(error_file, "r+", encoding="utf-8") as f:
                        try:
                            errors = json.load(f)
                        except json.JSONDecodeError:
                            errors = []
                        errors.append(error_data)
                        f.seek(0)
                        json.dump(errors, f, ensure_ascii=False, indent=2)
                else:
                    with open(error_file, "w", encoding="utf-8") as f:
                        json.dump([error_data], f, ensure_ascii=False, indent=2)

        pbar.update(1)

    return all_links


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
        return entry



async def fetch_all_articles(entries, concurrency=10):
    """
    Führt den asynchronen Abruf aller Artikel-Bodies mit einer maximalen Parallelität (concurrency) durch.
    """
    results = []
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_article(session, entry) for entry in entries]
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching articles"):
            result = await future
            results.append(result)
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
        df.to_csv(output_filename, sep="\t", index=False)
    else:
        df.to_pickle(output_filename)
        print("Saved as pickle file.")


def main():
    parser = argparse.ArgumentParser(description="Tagesschau Archiv Scraper")
    parser.add_argument("--mode", type=str, choices=["collect", "fetch"], required=True,
                        help="Modus: 'collect' sammelt Links, 'fetch' lädt Artikel-Bodies")
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
    elif args.mode == "fetch":
        print("Lade Links aus der Datei …")
        entries = load_links(args.links_file)
        print(f"{len(entries)} Links wurden geladen.")
        print("Lade Artikel parallel mit asyncio …")
        articles = asyncio.run(fetch_all_articles(entries))
        save_articles(articles, args.output)
        print(f"Artikel wurden in {args.output} gespeichert.")


if __name__ == "__main__":
    main()
