import json
import requests
import base64
import io
from kafka_function import *

try:
    while True:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        key, value = decode_message(msg)

        if value.get('status') != MessageStatus.INCOMING.value:
            continue

        if value.get('pipeline') is True:
            model_number = value.get('modelNumber')
            if model_number is None or model_number not in model_configs:
                err = "Missing or invalid modelNumber in message"
                print(err)
                send_kafka_result(key, value, error=str(err))
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
                    send_kafka_result(key, value, response=json.dumps(result))
                elif response.status_code == 408:
                    send_kafka_result(key, value, error=MessageStatus.TIMEOUT.value)
                else:
                    raise Exception(f"API returned {response.status_code}: {response.text}")
            
            except ValueError as ve:
                print(f"ValueError: {ve}")
                send_kafka_result(key, value, error=str(ve))

            except Exception as e:
                print(f"Error processing message: {e}")
                send_kafka_result(key, value, error=str(e))
                
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
                response = requests.post(ChatAI, json=ollama_payload)

                if response.ok:
                    result = response.json()
                    generated = result.get("response", "")
                    send_kafka_result(key, value, response=generated)
                elif response.status_code == 408:
                    send_kafka_result(key, value, error=MessageStatus.TIMEOUT.value)
                else:
                    raise Exception(f"Ollama error {response.status_code}: {response.text}")
            except ValueError as ve:
                print(f"ValueError: {ve}")
                send_kafka_result(key, value, error=str(ve))

            except Exception as e:
                print(f"Error processing message: {e}")
                send_kafka_result(key, value, error=str(e))


except KeyboardInterrupt:
    pass
finally:
    consumer.close()
