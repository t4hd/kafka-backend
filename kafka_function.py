import time
import calendar
import json
from enum import Enum
from confluent_kafka import Consumer, Producer
from typing import Optional, Tuple

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

def decode_message(msg):
    key = msg.key().decode("utf-8") if msg.key() else None
    value = json.loads(msg.value().decode("utf-8"))
    return key, value

def send_kafka_result(key, value, *, error=None, response=None):
    if error:
        key_bytes, value_bytes = build_kafka_message(key, value, error=error)
    elif response:
        key_bytes, value_bytes = build_kafka_message(key, value, response=response)
    producer.produce("chat-messages", key=key_bytes, value=value_bytes)
    producer.flush()


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

class MessageStatus(Enum):
    INCOMING = "incoming"
    PROCESSING = "processing"
    PROCESSED = "processed"
    ERROR = "error"
    TIMEOUT = "timeout"
    REPROCESSED = "reprocessed"
    
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

consumer = Consumer(conf_Con)
producer = Producer(conf_Pro)
consumer.subscribe(['chat-messages'])



__all__ = [
    "build_kafka_message",
    "ollama_model_configs",
    "model_configs",
    "MessageStatus",
    "consumer",
    "producer",
    "decode_message",
    "send_kafka_result"
]
