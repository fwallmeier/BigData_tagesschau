import pandas as pd
import datetime
import json


#Calculate Polority and Remove unsued Columns for ACF and PAFC analysis

def parse_json_and_compute_polarity(json_str: str) -> float:
    """
    Parses a JSON string like:
        '{"positive": 0.0165, "negative": 0.9765, "neutral": 0.0069}'
    and returns a polarity in the range [-1, 1].

    polarity = positive - negative
    """
    if not isinstance(json_str, str):
        # Handle NaN or non-string values
        return None
    try:
        sentiment_dict = json.loads(json_str)
        pos = sentiment_dict.get("positive", 0.0)
        neg = sentiment_dict.get("negative", 0.0)
        return pos - neg
    except (json.JSONDecodeError, TypeError):
        # Handle parsing errors
        return None


if __name__ == "__main__":
    df = pd.read_csv("../TagesschauDaten/tagesschau_articles_sentiment.csv", sep="\t", header="infer")
    print(df.columns)
    df['date'] = df['date'].apply(lambda x: datetime.datetime.strptime(x, "%d.%m.%Y • %H:%M Uhr"))
    df =  df.drop(["page_api", "short_headline", "articleBody", "description", ], axis=1)
    df["headline_polarity"] = df["headline_sentiment_score"].apply(parse_json_and_compute_polarity)
    df["short_headline_polarity"] = df["short_headline_sentiment_score"].apply(parse_json_and_compute_polarity)
    df["short_text_polarity"] = df["short_text_sentiment_score"].apply(parse_json_and_compute_polarity)
    df["articleBody_polarity"] = df["articleBody_sentiment_score"].apply(parse_json_and_compute_polarity)
    df.to_csv("../TagesschauDaten/tagesschau_articles_sentiment_clear.csv", sep="\t", index=False)