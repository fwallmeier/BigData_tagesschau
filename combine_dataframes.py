import pandas as pd
import os
import unicodedata

def list_all_files(directory):
    """
    Listet alle Dateien in einem Verzeichnis und seinen Unterverzeichnissen auf.

    Args:
        directory (str): Der Pfad zum Verzeichnis

    Returns:
        list: Eine Liste mit allen gefundenen Dateipfaden
    """
    all_files = []

    # os.walk durchläuft den Ordner und alle Unterordner
    for root, dirs, files in os.walk(directory):
        for file in files:
            # Erstelle den vollständigen Pfad für jede Datei
            file_path = os.path.join(root, file)
            # Konvertiere Backslashes zu Forward Slashes und normalisiere Unicode
            normalized_path = unicodedata.normalize("NFC", file_path.replace(os.sep, '/'))
            all_files.append(normalized_path)

    return all_files


def combine():
    # Parquet-Datei einlesen
    df_list = []
    for file in list_all_files("./raw_data"):
        if file.endswith(".csv"):
            df_list.append(pd.read_csv(file, sep="\t"))
        elif file.endswith(".parquet"):
            df_list.append(pd.read_parquet(file))
        else:
            print("error")


    df_gesamt = pd.concat(df_list, ignore_index=True)
    df_gesamt.drop_duplicates(inplace=True)
    df_gesamt = df_gesamt.sort_values(by="date")  # Falls es eine Spalte "Datum" gibt
    df_gesamt.to_csv("./gesamt_daten/cleanTest/test.csv", index=False, sep="\t")


if __name__ == "__main__":
    combine()