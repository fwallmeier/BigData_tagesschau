import pandas as pd
from tqdm import tqdm
import time
import os
import json
import re

FILEPATH = "tagesschau_articles.csv"
OUTPUT_FILE = "tagesschau_articles_sentiment.csv"
# Maximum tokens to process at once - BERT model has a limit of 256 tokens
MAX_TOKENS = 200  # Setting slightly lower than 256 to account for special tokens

def load_data():
    """Load data from CSV file."""
    print("Loading data...")
    df = pd.read_csv(FILEPATH, sep="\t", encoding="utf-8")
    print(f"Loaded {len(df)} articles.")
    return df

def split_text(text, max_tokens=MAX_TOKENS):
    """
    Split text into chunks that don't exceed max_tokens.
    Using a simple approximation: one word is roughly one token.
    For more precise tokenization, you'd need a proper tokenizer.
    """
    if not isinstance(text, str) or not text.strip():
        return []
    
    # Simple tokenization by splitting on whitespace
    words = text.split()
    
    # If text is shorter than max_tokens, return as is
    if len(words) <= max_tokens:
        return [text]
    
    # Split into chunks
    chunks = []
    current_chunk = []
    current_length = 0
    
    for word in words:
        if current_length + 1 > max_tokens:
            # Current chunk is full, add it to chunks and start a new one
            chunks.append(" ".join(current_chunk))
            current_chunk = [word]
            current_length = 1
        else:
            # Add word to current chunk
            current_chunk.append(word)
            current_length += 1
    
    # Add the last chunk if it's not empty
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    
    return chunks

def analyze_text(text, model):
    """Analyze a single text field."""
    if not isinstance(text, str) or not text.strip():
        return "neutral", {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
    
    # Split text if it's too long
    chunks = split_text(text)
    
    # If no valid chunks, return neutral
    if not chunks:
        return "neutral", {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
    
    # If only one chunk, process it normally
    if len(chunks) == 1:
        try:
            # The model expects a list of texts
            classes, probabilities = model.predict_sentiment([text], output_probabilities=True)
            
            # Extract the first result since we only passed one text
            sentiment_class = classes[0]
            
            # Extract and format the probabilities
            prob_dict = {}
            for label_prob_pair in probabilities[0]:
                label = label_prob_pair[0]
                prob = float(label_prob_pair[1])  # Convert to Python float
                prob_dict[label] = prob
                
            return sentiment_class, prob_dict
        
        except Exception as e:
            print(f"Error analyzing text: {str(e)[:100]}...")
            return "neutral", {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
    
    # Multiple chunks: analyze each and aggregate results
    all_probs = {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
    chunk_count = 0
    
    try:
        for chunk in chunks:
            # Analyze this chunk
            classes, probabilities = model.predict_sentiment([chunk], output_probabilities=True)
            
            # Accumulate probabilities
            for label_prob_pair in probabilities[0]:
                label = label_prob_pair[0]
                prob = float(label_prob_pair[1])
                all_probs[label] += prob
                
            chunk_count += 1
            
        # Average the probabilities
        for label in all_probs:
            all_probs[label] /= chunk_count
            
        # Normalize to ensure probabilities sum to 1.0
        prob_sum = sum(all_probs.values())
        if prob_sum > 0:  # Avoid division by zero
            for label in all_probs:
                all_probs[label] /= prob_sum
            
        # Determine the overall sentiment class
        sentiment_class = max(all_probs, key=all_probs.get)
            
        return sentiment_class, all_probs
    
    except Exception as e:
        print(f"Error analyzing chunked text: {str(e)[:100]}...")
        return "neutral", {"positive": 0.0, "negative": 0.0, "neutral": 0.0}

def process_row(row, model):
    """Process all text fields in a row."""
    results = {}
    
    # Fields to analyze
    fields = ["headline", "short_headline", "short_text", "articleBody"]
    
    for field in fields:
        if field in row and isinstance(row[field], str) and row[field].strip():
            sentiment_class, probabilities = analyze_text(row[field], model)
            
            # Store results
            results[f"{field}_sentiment_category"] = sentiment_class
            results[f"{field}_sentiment_score"] = json.dumps(probabilities)
        else:
            # Default values for missing fields
            default_probs = json.dumps({"positive": 0.0, "negative": 0.0, "neutral": 0.0})
            results[f"{field}_sentiment_category"] = "neutral"
            results[f"{field}_sentiment_score"] = default_probs
    
    return results

def main():
    # Initialize counters for each field
    field_counts = {
        "headline": {"positive": 0, "negative": 0, "neutral": 0},
        "short_headline": {"positive": 0, "negative": 0, "neutral": 0},
        "short_text": {"positive": 0, "negative": 0, "neutral": 0},
        "articleBody": {"positive": 0, "negative": 0, "neutral": 0}
    }
    
    processed_count = 0
    
    # Start timer
    start_time = time.time()
    
    # Load the model once
    print("Loading sentiment model...")
    from germansentiment import SentimentModel
    model = SentimentModel()
    print("Model loaded, starting analysis...")
    
    # Check if there's a partially processed file
    if os.path.exists(OUTPUT_FILE):
        print(f"Found existing output file. Loading processed data...")
        processed_df = pd.read_csv(OUTPUT_FILE, sep="\t", encoding="utf-8")
        
        # Count already processed articles
        for field in field_counts.keys():
            if f"{field}_sentiment_category" in processed_df.columns:
                field_counts[field]["positive"] = len(processed_df[processed_df[f"{field}_sentiment_category"] == "positive"])
                field_counts[field]["negative"] = len(processed_df[processed_df[f"{field}_sentiment_category"] == "negative"])
                field_counts[field]["neutral"] = len(processed_df[processed_df[f"{field}_sentiment_category"] == "neutral"])
        
        processed_count = len(processed_df)
        print(f"Resuming from {processed_count} already processed articles")
        
        # Load the original data
        df = load_data()
        
        # Keep only unprocessed rows
        df = df.iloc[processed_count:]
    else:
        # Load the full dataset
        df = load_data()
    
    # Skip processing if all rows are already done
    if len(df) == 0:
        print("All articles already processed. No work to do.")
        return
    
    print(f"Processing {len(df)} articles one by one...")
    
    # Create the output file if it doesn't exist
    if not os.path.exists(OUTPUT_FILE):
        # Create column names for sentiment results
        fields = ["headline", "short_headline", "short_text", "articleBody"]
        additional_columns = []
        for field in fields:
            additional_columns.append(f"{field}_sentiment_category")
            additional_columns.append(f"{field}_sentiment_score")
        
        # Create a DataFrame with extended headers
        header_df = pd.DataFrame(columns=list(df.columns) + additional_columns)
        header_df.to_csv(OUTPUT_FILE, sep="\t", encoding="utf-8", index=False)
    
    # Process each row and immediately save to file
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Analyzing sentiment"):
        # Process all text fields in the row
        sentiment_results = process_row(row, model)
        
        # Update counters
        for field in field_counts.keys():
            category = sentiment_results.get(f"{field}_sentiment_category", "neutral")
            if category == "positive":
                field_counts[field]["positive"] += 1
            elif category == "negative":
                field_counts[field]["negative"] += 1
            else:
                field_counts[field]["neutral"] += 1
        
        processed_count += 1
        
        # Create a row with the results
        result_row = row.copy()
        for key, value in sentiment_results.items():
            result_row[key] = value
        
        # Append the row to the output file
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
            result_row.to_frame().T.to_csv(f, sep="\t", header=False, index=False)
        
        # Print progress every 50 articles
        if processed_count % 50 == 0:
            elapsed_time = time.time() - start_time
            avg_time = elapsed_time / processed_count
            remaining = (len(df) - (index + 1)) * avg_time
            
            print(f"\nProgress update - {processed_count} articles processed")
            for field in field_counts.keys():
                print(f"{field}: Positive: {field_counts[field]['positive']}, " +
                      f"Negative: {field_counts[field]['negative']}, " +
                      f"Neutral: {field_counts[field]['neutral']}")
            print(f"Average processing time: {avg_time:.4f} seconds per article")
            print(f"Estimated time remaining: {remaining/60:.1f} minutes")
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Print final statistics
    print("\nAnalysis Results:")
    print(f"Number of articles processed in this run: {len(df)}")
    print(f"Total articles processed: {processed_count}")
    
    print("\nSentiment statistics by field:")
    for field in field_counts.keys():
        print(f"\n{field}:")
        print(f"  Positive: {field_counts[field]['positive']}")
        print(f"  Negative: {field_counts[field]['negative']}")
        print(f"  Neutral: {field_counts[field]['neutral']}")
    
    avg_time_per_article = elapsed_time / len(df) if len(df) else 0
    print(f"\nProcessing completed in {elapsed_time:.2f} seconds ({avg_time_per_article:.4f} seconds per article)")

if __name__ == "__main__":
    main()