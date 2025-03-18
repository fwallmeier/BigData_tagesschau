from typing import List, Union

import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

# Statsmodels for ACF and PACF
from statsmodels.tsa.stattools import acf, pacf


def load_and_preprocess_data(
        csv_path: str,
        date_col: str = 'date_api',
        link_col: str = 'link'
) -> pd.DataFrame:
    """
    Loads the Tagesschau CSV data, converts date column to datetime,
    and does basic cleaning.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file.
    date_col : str, optional
        Name of the column containing the publication date.
    link_col : str, optional
        Name of the column that contains category info in its path.

    Returns
    -------
    pd.DataFrame
        A dataframe with parsed dates.
    """
    df = pd.read_csv(csv_path, sep="\t")

    # Convert date column to datetime
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Some rows might have invalid or missing dates
    df = df.dropna(subset=[date_col])

    # Make sure DataFrame is sorted by date
    df = df.sort_values(by=date_col)

    # Infer a simple category from the link column
    # (This is a basic approach – adapt or extend as needed.)
    def infer_category(link: str):
        link_lower = str(link).lower()
        if '/inland/' in link_lower:
            return 'inland'
        elif '/ausland/' in link_lower:
            return 'ausland'
        elif '/wirtschaft/' in link_lower:
            return 'wirtschaft'
        else:
            return 'other'

    df['inferred_category'] = df[link_col].apply(infer_category)

    return df


def filter_by_category(
        df: pd.DataFrame,
        category: str,
        category_col: str = 'inferred_category'
) -> pd.DataFrame:
    """
    Filters the dataframe by an inferred category (inland, ausland, wirtschaft, etc.).

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    category : str
        Category string (e.g., 'inland', 'ausland', 'wirtschaft', 'other').
    category_col : str, optional
        Name of the column containing inferred category.

    Returns
    -------
    pd.DataFrame
        Filtered dataframe.
    """
    if category == 'all':
        return df
    return df[df[category_col] == category]


import pandas as pd

def aggregate_sentiment(
    df: pd.DataFrame,
    date_col: str,
    sentiment_col: str | list,
    agg_func: str = "mean",
    freq: str = "D",
    start_date: str = None,
    end_date: str = None,
    handle_missing: str = "fill"
):
    """
    Aggregates sentiment values by time period (day, week, month).

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing date and sentiment columns.
    date_col : str
        The column name containing the date.
    sentiment_col : str or list
        One or more sentiment columns to aggregate.
    agg_func : str, optional
        The aggregation function ('mean' or 'median').
    freq : str, optional
        The frequency for aggregation ('D' for daily, 'W' for weekly, 'M' for monthly).
    start_date : str, optional
        The start date for filtering (format: 'YYYY-MM-DD').
    end_date : str, optional
        The end date for filtering (format: 'YYYY-MM-DD').
    handle_missing : str, optional
        How to handle missing values: 'fill' (forward-fill), 'drop' (remove NaNs).

    Returns
    -------
    pd.DataFrame
        Aggregated sentiment time series.
    """

    # Sicherstellen, dass sentiment_col eine Liste ist
    if isinstance(sentiment_col, str):
        sentiment_col = [sentiment_col]

    # Datum filtern
    if start_date:
        df = df[df[date_col] >= start_date]
    if end_date:
        df = df[df[date_col] <= end_date]

    # Nur relevante Spalten kopieren
    df = df[[date_col] + sentiment_col].copy()

    # Alle Sentiment-Spalten in numerische Werte umwandeln
    for col in sentiment_col:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # NaN-Werte entfernen
    df.dropna(subset=sentiment_col, inplace=True)

    # Datum als Index setzen
    df.set_index(date_col, inplace=True)

    # Aggregation für alle Spalten durchführen
    if agg_func == "mean":
        ts = df.resample(freq).mean()
    elif agg_func == "median":
        ts = df.resample(freq).median()
    else:
        raise ValueError("agg_func must be either 'mean' or 'median'")

    # Falls gewünscht: Vollständige Zeitreihe mit NaN-Werten ergänzen
    if start_date and end_date:
        full_idx = pd.date_range(start=start_date, end=end_date, freq=freq)
        ts = ts.reindex(full_idx)

    # Fehlende Werte behandeln
    if handle_missing == "ffill":
        ts = ts.fillna(method="ffill")
    elif handle_missing == "drop":
        ts = ts.dropna()

    # Spaltennamen anpassen
    ts.columns = [f"{col}_{agg_func}" for col in sentiment_col]

    return ts


def plot_sentiment_trend(ts_sentiment: pd.DataFrame, title_prefix: str = ''):
    """
    Plots the sentiment polarity development over time.

    Parameters
    ----------
    ts_sentiment : pd.DataFrame
        Aggregated sentiment time series (date as index, sentiment as columns).
    title_prefix : str, optional
        A string prefix for the plot title.
    """
    plt.figure(figsize=(10, 5))

    # Farben für unterschiedliche Spalten definieren
    colors = ['b', 'r', 'g', 'c', 'm', 'y', 'k']

    for i, col in enumerate(ts_sentiment.columns):
        plt.plot(ts_sentiment.index, ts_sentiment[col],
                 color=colors[i % len(colors)], label=col, linewidth=2)

    plt.xlabel('Zeit')
    plt.ylabel('Polarität')
    plt.title(f"{title_prefix} Sentiment-Entwicklung über die Zeit")
    plt.legend()
    plt.grid(True)

    plt.show()


def plot_acf_and_pacf(
        series: pd.Series | pd.DataFrame | list[pd.Series],
        lags: int = 40,
        title_prefix: str = ''
):
    """
    Plots ACF and PACF for a given time series or multiple time series.

    Parameters
    ----------
    series : pd.Series, pd.DataFrame, or list[pd.Series]
        The time series data. Can be a single series or multiple.
    lags : int, optional
        Number of lags to display in the ACF/PACF plots.
    title_prefix : str, optional
        A string prefix to give context in plot titles.
    """
    # Falls eine einzelne pd.Series übergeben wurde -> In Liste umwandeln
    if isinstance(series, pd.Series):
        series = [series]

    # Falls ein DataFrame übergeben wurde -> Liste von Spalten extrahieren
    elif isinstance(series, pd.DataFrame):
        series = [series[col] for col in series.columns]

    colors = ['b', 'r', 'g', 'c', 'm', 'y', 'k']  # Farben für mehrere Serien
    num_series = len(series)

    acf_values_list = []
    pacf_values_list = []
    labels = []

    # ACF & PACF für jede Serie berechnen
    for idx, s in enumerate(series):
        if not isinstance(s, pd.Series):  # Sicherstellen, dass s eine Serie ist
            raise ValueError(f"Element {idx} in 'series' ist kein pd.Series, sondern {type(s)}")

        data = s.dropna().values  # Fehlende Werte entfernen
        acf_values_list.append(acf(data, nlags=lags))
        pacf_values_list.append(pacf(data, nlags=lags))
        labels.append(s.name if s.name else f"Serie {idx + 1}")  # Spaltennamen verwenden

    # ACF-Plot
    plt.figure(figsize=(7, 4))
    for i, (acf_values, label) in enumerate(zip(acf_values_list, labels)):
        offset = (i - num_series / 2) * 0.2  # Versatz für bessere Sichtbarkeit
        x_values = np.arange(len(acf_values)) + offset  # Offset in x-Werte einfügen
        plt.stem(x_values, acf_values, linefmt=colors[i % len(colors)] + "-",
                 markerfmt=colors[i % len(colors)] + "o", basefmt="k-", label=label)

    plt.xlabel("Lag")
    plt.ylabel("ACF")
    plt.title(f"{title_prefix} Autocorrelation (ACF)")
    plt.legend()
    plt.show()

    # PACF-Plot
    plt.figure(figsize=(7, 4))
    for i, (pacf_values, label) in enumerate(zip(pacf_values_list, labels)):
        offset = (i - num_series / 2) * 0.2  # Versatz für bessere Sichtbarkeit
        x_values = np.arange(len(pacf_values)) + offset  # Offset in x-Werte einfügen
        plt.stem(x_values, pacf_values, linefmt=colors[i % len(colors)] + "-",
                 markerfmt=colors[i % len(colors)] + "o", basefmt="k-", label=label)

    plt.xlabel("Lag")
    plt.ylabel("PACF")
    plt.title(f"{title_prefix} Partial Autocorrelation (PACF)")
    plt.legend()
    plt.show()


if __name__ == "__main__":
    # --- Usage Example ---

    # Path to your CSV file
    CSV_PATH = "../TagesschauDaten/tagesschau_articles_sentiment_clear.csv"
    agg_func = 'median'
    start_date = '2006-01-01'
    end_date = '2025-03-01'
    freq = "M"
    category_to_analyze = 'inland'  # e.g., 'inland', 'ausland', 'wirtschaft', 'other', or 'all'
    sentiment_columns = ['articleBody_polarity', 'short_headline_polarity'] #short_headline_polarity, articleBody_polarity

    # 1. Load and preprocess data
    df = load_and_preprocess_data(
        csv_path=CSV_PATH,
        date_col='date',  # or 'date', depending on your data
        link_col='link'
    )

    # 2. Filter by category (inland, ausland, wirtschaft, other, or 'all')
    df_filtered = filter_by_category(df, category=category_to_analyze)

    # 4. Aggregate sentiment by day/week/month using mean/median
    ts_sentiment = aggregate_sentiment(
        df_filtered,
        date_col='date',
        sentiment_col=sentiment_columns,
        agg_func=agg_func,  # 'mean' oder 'median'
        freq=freq,  # 'D', 'W', 'M'
        start_date=start_date,
        end_date=end_date,
        handle_missing='fill'  # 'drop' oder 'ffill'
    )

    print(ts_sentiment.head(10))

    # 6. Perform and plot ACF and PACF for multiple sentiment columns
    plot_acf_and_pacf(
        series=ts_sentiment[[f"{col}_{agg_func}" for col in sentiment_columns]],  # Mehrere Zeitreihen
        lags=53,
        title_prefix=f"S: {start_date}, E: {end_date}, Freq: {freq} ({agg_func.capitalize()})"
    )

    plot_sentiment_trend(ts_sentiment)
