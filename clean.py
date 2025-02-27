import pandas as pd
import re
import argparse

def clean_text(text):
    """Hilfsfunktion zum Bereinigen von Textfeldern."""
    if isinstance(text, str):
        text = text.strip()
        text = re.sub(r"\nmehr$", "", text)  # Entferne "\nmehr" am Ende
    return text

def clean_csv(path):
    # Lade die CSV-Datei als Pandas DataFrame
    df = pd.read_csv(path, delimiter="\t")

    # Entferne doppelte Einträge basierend auf der 'article'-Spalte
    df = df.drop_duplicates(subset=['article'], ignore_index=True)

    # Bereinige relevante Textspalten
    text_columns = ["short_text", "headline", "short_headline", "article"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    # Sortiere nach Datum absteigend (falls Spalte existiert)
    if "date" in df.columns:
        df = df.sort_values(by="date", ascending=False)

    print("READY")
    return df

def main(args):
    df = clean_csv(args.path)
    if args.upload_path:
        print("Upload-Funktion mit Pandas nicht unterstützt. Datei wird lokal gespeichert.")
    else:
        print(len(df))
        df.to_csv(args.save_path, sep="\t", index=False)  # Speichern als saubere CSV

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean the scraped data and save it to disk."
    )
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="The path to the CSV file.",
    )
    parser.add_argument(
        "--save_path",
        type=str,
        default="./gesamt_daten/cleanTest/tagesschau_06-25.csv",
        help="The path where the cleaned data should be saved.",
    )
    parser.add_argument(
        "--upload_path",
        type=str,
        default=None,
        help="(Not supported in Pandas version) The path to upload the cleaned data.",
    )
    args = parser.parse_args()
    main(args)
