import json
from confluent_kafka import Consumer, Producer
from enum import Enum
import time
import calendar
from typing import Optional, Tuple
import requests
import base64
import io

conf_Con = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'chat_reader_group',
    'client.id': 'chat_backend',
    'auto.offset.reset': 'earliest'
}

conf_Pro = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'chat_answer_group',
    'client.id': 'chat_backend',
}

class MessageStatus(Enum):
    INCOMING = "incoming"
    PROCESSING = "processing"
    PROCESSED = "processed"
    ERROR = "error"
    TIMEOUT = "timeout"
    REPROCESSED = "reprocessed"

model_configs = {
    101: {"endpoint": "/analyze_full_pipeline", "field": "audio", "type": "file"},
    102: {"endpoint": "/analyze_speech_to_text", "field": "audio", "type": "file"},
    103: {"endpoint": "/analyze_sentiment", "field": "sentiment", "type": "text"},
    104: {"endpoint": "/analyze_summary", "field": "summury", "type": "text"},
    105: {"endpoint": "/analyze_translation", "field": "translate", "type": "text"},
    106: {"endpoint": "/analyze_cover_topic", "field": "topic", "type": "text"},
    107: {"endpoint": "/analyze_speech_to_text_summary_sentiment", "field": "audio", "type": "file"},
}

ollama_model_configs = {
    1: {"model": "llama3.2:3b"},
    2: {"model": "deepseek-r1:1.5b"},
    3: {"model": "wizardlm2:7b"},
    # add more as needed
}


#ChatAI = "http://localhost:9000/chat";
OllamaAI = "http://localhost:11434/api/generate"
PYTHON_API = "http://localhost:1011"

consumer = Consumer(conf_Con)
producer = Producer(conf_Pro)
consumer.subscribe(['chat-messages'])

def build_kafka_message(key: str, values: dict, error: Optional[str] = None, response: Optional[str] = None) -> Tuple[bytes, bytes]:
    payload = values.copy()

    if error:
        payload["errorMsg"] = error
        payload["status"] = MessageStatus.ERROR.value
        payload["timestamp"] = calendar.timegm(time.gmtime())
    elif response:
        payload["llmresponse"]=response
        payload["status"] = MessageStatus.PROCESSED.value
        payload["timestamp"] = calendar.timegm(time.gmtime())

    key_bytes = key.encode("utf-8")
    value_bytes = json.dumps(payload).encode("utf-8")

    return key_bytes, value_bytes

try:
    while True:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        value = json.loads(msg.value().decode("utf-8"))

        if value.get('status') != MessageStatus.INCOMING.value:
            continue

        if value.get('pipeline') is True:
            model_number = value.get('modelNumber')
            if model_number is None or model_number not in model_configs:
                err = "Missing or invalid modelNumber in message"
                print(err)
                key_bytes, value_bytes = build_kafka_message(key, value, error=err)
                producer.produce(topic="chat-messages", key=key_bytes, value=value_bytes)
                producer.flush()
                continue

            config = model_configs[model_number]
            endpoint = config["endpoint"]
            form_field = config["field"]
            input_type = config["type"]
            url = PYTHON_API + endpoint

            try:
                if input_type == "file":
                    base64_file_content = value.get(form_field)
                    if not base64_file_content:
                        raise ValueError(f"Missing base64 encoded '{form_field}' field")
                    
                    file_bytes = base64.b64decode(base64_file_content)
                    files = {form_field: (f"{form_field}.bin", io.BytesIO(file_bytes))}
                    response = requests.post(url, files=files)

                elif input_type == "text":
                    text = value.get(form_field) or value.get("text") or ""
                    if not text:
                        raise ValueError("Missing or empty text for text input")

                    data = {form_field: text}
                    response = requests.post(url, data=data)
                else:
                    raise ValueError(f"Unsupported input type: {input_type}")

                if response.ok:
                    result = response.json()
                    key_bytes, value_bytes = build_kafka_message(key, value, response=json.dumps(result))
                    producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                    producer.flush()
                elif response.status_code == 408:
                    key_bytes, value_bytes = build_kafka_message(key, value, error=MessageStatus.TIMEOUT.value)
                    producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                    producer.flush()
                else:
                    raise Exception(f"API returned {response.status_code}: {response.text}")
            
            except ValueError as ve:
                print(f"ValueError: {ve}")
                key_bytes, value_bytes = build_kafka_message(key, value, error=str(ve))
                producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                producer.flush()

            except Exception as e:
                print(f"Error processing message: {e}")
                key_bytes, value_bytes = build_kafka_message(key, value, error=str(e))
                producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                producer.flush()
                
        elif model_number in ollama_model_configs:
            try:
                prompt = value.get("text") or ""
                if not prompt:
                    raise ValueError("Missing or empty 'text' field for Ollama model")

                ollama_model = ollama_model_configs[model_number]["model"]
                ollama_payload = {
                    "model": ollama_model,
                    "prompt": prompt,
                    "stream": False
                }
                response = requests.post(OllamaAI, json=ollama_payload)

                if response.ok:
                    result = response.json()
                    generated = result.get("response", "")
                    key_bytes, value_bytes = build_kafka_message(key, value, response=generated)
                    producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                    producer.flush()
                elif response.status_code == 408:
                    key_bytes, value_bytes = build_kafka_message(key, value, error=MessageStatus.TIMEOUT.value)
                    producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                    producer.flush()
                else:
                    raise Exception(f"Ollama error {response.status_code}: {response.text}")
            except ValueError as ve:
                print(f"ValueError: {ve}")
                key_bytes, value_bytes = build_kafka_message(key, value, error=str(ve))
                producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                producer.flush()

            except Exception as e:
                print(f"Error processing message: {e}")
                key_bytes, value_bytes = build_kafka_message(key, value, error=str(e))
                producer.produce("chat-messages", key=key_bytes, value=value_bytes)
                producer.flush()


except KeyboardInterrupt:
    pass
finally:
    consumer.close()
