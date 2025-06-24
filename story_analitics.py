from kafka_function_analysis import *
import json
import requests

try:
    while True:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        key, value = decode_message(msg)

        if value.get('status') != "processed":
            continue
        
        if value.get('pipeline') is True and value.get('llmresponse'):
            pipeline_raw = value.get('llmresponse')
            pipeline = parse_llmresponse(pipeline_raw)

            if not pipeline:
                continue  # skip if parsing failed

            sentiment_data = pipeline.get('sentiment_analysis', {})
            sentiment_data.pop("overall", None)

            analitic = extract_sentiment_words(sentiment_data)
            store_word_stats(analitic)
            print(analitic)


        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()