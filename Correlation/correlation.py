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


def aggregate_sentiment(
        df: pd.DataFrame,
        date_col: str,
        sentiment_col: str,
        agg_func: str = 'mean',
        freq: str = 'D',
        start_date: str = None,
        end_date: str = None,
        handle_missing: str = 'drop'
) -> pd.DataFrame:
    """
    Aggregates sentiment over a given frequency (day, week, month) using mean/median.
    Optionally filters the time range and handles missing dates by forward fill or drop.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    date_col : str
        The name of the datetime column for resampling.
    sentiment_col : str
        The name of the sentiment score column to aggregate.
    agg_func : str, optional
        'mean' or 'median'
    freq : str, optional
        Resample frequency, e.g., 'D' (day), 'W' (week), 'M' (month).
    start_date : str, optional
        Start date (inclusive). e.g. '2010-01-01'
    end_date : str, optional
        End date (inclusive). e.g. '2020-12-31'
    handle_missing : str, optional
        How to handle missing dates.
        'drop' removes them, 'ffill' forward fills them.

    Returns
    -------
    pd.DataFrame
        A dataframe with datetime index and one column of aggregated sentiment.
    """
    # Filter date range if given
    if start_date:
        df = df[df[date_col] >= start_date]
    if end_date:
        df = df[df[date_col] <= end_date]

    # We only need the date and the sentiment column
    df = df[[date_col, sentiment_col]].copy()

    # Convert sentiment column to numeric (in case it's parsed as string)
    df[sentiment_col] = pd.to_numeric(df[sentiment_col], errors='coerce')

    # Drop rows where sentiment is NaN
    df.dropna(subset=[sentiment_col], inplace=True)

    # Set date column as index
    df.set_index(date_col, inplace=True)

    # Resample to the requested frequency
    if agg_func == 'mean':
        ts = df.resample(freq).mean()
    elif agg_func == 'median':
        ts = df.resample(freq).median()
    else:
        raise ValueError("agg_func must be either 'mean' or 'median'")

    # If we want to ensure we have a complete date range from start to end:
    if start_date and end_date:
        # Build a date range index
        full_idx = pd.date_range(start=start_date, end=end_date, freq=freq)
        ts = ts.reindex(full_idx)

    # Handle missing
    if handle_missing == 'ffill':
        ts = ts.fillna(method='ffill')
    elif handle_missing == 'drop':
        ts = ts.dropna()

    # Rename the sentiment column to something standardized
    ts.columns = [f"{sentiment_col}_{agg_func}"]

    return ts


def plot_acf_and_pacf(
        series: pd.Series,
        lags: int = 40,
        title_prefix: str = ''
):
    """
    Plots ACF and PACF for a given time series.

    Parameters
    ----------
    series : pd.Series
        The time series data.
    lags : int, optional
        Number of lags to display in the ACF/PACF plots.
    title_prefix : str, optional
        A string prefix to give context in plot titles.
    """
    # Convert series to numpy array (dropping NaNs if any remain)
    data = series.dropna().values

    # Compute ACF and PACF
    acf_values = acf(data, nlags=lags)
    pacf_values = pacf(data, nlags=lags)
    print(acf_values)
    print(pacf_values)

    # Plot ACF
    plt.figure(figsize=(7, 4))
    plt.stem(range(len(acf_values)), acf_values)
    plt.xlabel('Lag')
    plt.ylabel('ACF')
    plt.title(f'{title_prefix} Autocorrelation (ACF)')
    plt.show()

    # Plot PACF
    plt.figure(figsize=(7, 4))
    plt.stem(range(len(pacf_values)), pacf_values)
    plt.xlabel('Lag')
    plt.ylabel('PACF')
    plt.title(f'{title_prefix} Partial Autocorrelation (PACF)')
    plt.show()


if __name__ == "__main__":
    # --- Usage Example ---

    # Path to your CSV file
    CSV_PATH = "../TagesschauDaten/tagesschau_articles_sentiment_clear.csv"

    # 1. Load and preprocess data
    df = load_and_preprocess_data(
        csv_path=CSV_PATH,
        date_col='date',  # or 'date', depending on your data
        link_col='link'
    )

    # 2. Filter by category (inland, ausland, wirtschaft, other, or 'all')
    category_to_analyze = 'all'  # e.g., 'inland', 'ausland', 'wirtschaft', 'other', or 'all'
    df_filtered = filter_by_category(df, category=category_to_analyze)

    # 3. Pick which sentiment column you want to analyze
    #    (Replace with the column name that holds the numeric score you want)
    sentiment_column = 'articleBody_polarity'

    # 4. Aggregate sentiment by day/week/month using mean/median
    #    You can change freq to 'W' (weekly) or 'M' (monthly)
    #    and agg_func to 'median' if desired.
    ts_sentiment = aggregate_sentiment(
        df_filtered,
        date_col='date',
        sentiment_col=sentiment_column,
        agg_func='mean',  # 'mean' or 'median'
        freq='D',  # 'D', 'W', 'M'
        start_date='2006-01-01',
        end_date='2025-12-31',
        handle_missing='fill'  # 'drop' or 'ffill'
    )

    # 6. Perform and plot ACF and PACF
    plot_acf_and_pacf(
        series=ts_sentiment[sentiment_column + '_mean'],
        lags=20,
        title_prefix=f"{category_to_analyze.capitalize()} {sentiment_column} (Mean)"
    )
