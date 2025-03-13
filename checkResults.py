import pandas as pd


if __name__ == "__main__":
    # CSV-Datei einlesen (ersetze 'datei.csv' mit deinem Dateinamen)
    df = pd.read_csv("gesamt_daten/cleanTest/test.csv", sep="\t")

    # Datumsbereich von 01.01.2006 bis 20.02.2025 erstellen
    start_date = "2006-01-01"
    end_date = "2025-02-20"
    date_range = pd.date_range(start=start_date, end=end_date)

    # Fehlende Tage identifizieren
    missing_dates = date_range.difference(df["date"])

    # Fehlende Tage in zusammenhängende Bereiche gruppieren
    if missing_dates.empty:
        print("Es fehlen keine Tage.")
    else:
        print("Fehlende Datumsbereiche:")

        start = missing_dates[0]
        for i in range(1, len(missing_dates)):
            if missing_dates[i] != missing_dates[i - 1] + pd.Timedelta(days=1):
                # Bereich ausgeben, wenn Lücke gefunden
                print(f"{start.strftime('%Y-%m-%d')} - {missing_dates[i - 1].strftime('%Y-%m-%d')}")
                start = missing_dates[i]

        # Letzten Bereich ausgeben
        print(f"{start.strftime('%Y-%m-%d')} - {missing_dates[-1].strftime('%Y-%m-%d')}")
