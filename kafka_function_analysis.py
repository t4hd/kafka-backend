from confluent_kafka import Consumer
import json
import spacy
from collections import defaultdict
import sqlite3

conf_Con = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'analitic',
    'client.id': 'analitic',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf_Con)
consumer.subscribe(['chat-messages'])

def decode_message(msg):
    key = msg.key().decode("utf-8") if msg.key() else None
    value = json.loads(msg.value().decode("utf-8"))
    return key, value

# Load the transformer-based NLP pipeline (load only once)
nlp = spacy.load("en_core_web_trf")

def get_sentiment_label(sentiment_scores):
    if sentiment_scores["positive"] > sentiment_scores["negative"] and sentiment_scores["positive"] > sentiment_scores["neutral"]:
        return "positive"
    elif sentiment_scores["negative"] > sentiment_scores["positive"] and sentiment_scores["negative"] > sentiment_scores["neutral"]:
        return "negative"
    else:
        return "neutral"

def extract_sentiment_words(sentiment_data):
    """
    sentiment_data: dict of the form:
    {
        "S1": {
            "negative": float,
            "neutral": float,
            "positive": float,
            "sentence": str
        },
        ...
    }

    Returns:
        dict: {word_lemma: {'positive': int, 'negative': int, 'neutral': int}}
    """
    word_stats = defaultdict(lambda: {'positive': 0, 'negative': 0, 'neutral': 0})

    for entry in sentiment_data.values():
        sentence = entry["sentence"]
        label = get_sentiment_label(entry)

        doc = nlp(sentence)

        for token in doc:
            if token.pos_ in {"NOUN", "PROPN", "VERB"} and not token.is_stop and token.is_alpha:
                lemma = token.lemma_.lower()
                word_stats[lemma][label] += 1

    return word_stats


def parse_llmresponse(llmresponse_raw):
    if not isinstance(llmresponse_raw, str):
        return llmresponse_raw  # Already parsed

    try:
        # Fix overly escaped string (double-encoded with escaped quotes)
        cleaned = llmresponse_raw.strip()

        # Optional: remove leading/trailing quotes if present
        if cleaned.startswith('"') and cleaned.endswith('"'):
            cleaned = cleaned[1:-1]

        # Unescape embedded quotes and backslashes
        cleaned = cleaned.encode('utf-8').decode('unicode_escape')

        # Finally parse the actual JSON
        return json.loads(cleaned)
    except Exception as e:
        print("❌ Final JSON decode failed:", e)
        print("🔎 Raw input (truncated):", llmresponse_raw[:300])
        return None
    
def store_word_stats(analitic, db_path="word_stats.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS word_sentiments (
            word TEXT PRIMARY KEY,
            positive INTEGER DEFAULT 0,
            negative INTEGER DEFAULT 0,
            neutral INTEGER DEFAULT 0
        )
    """)

    for word, counts in analitic.items():
        # Try to update
        cursor.execute("""
            INSERT INTO word_sentiments (word, positive, negative, neutral)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(word) DO UPDATE SET
                positive = positive + excluded.positive,
                negative = negative + excluded.negative,
                neutral = neutral + excluded.neutral
        """, (word, counts['positive'], counts['negative'], counts['neutral']))

    conn.commit()
    conn.close()

__all__ = [
    "consumer",
    "decode_message",
    "extract_sentiment_words",
    "parse_llmresponse",
    "store_word_stats"
]