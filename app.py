from flask import Flask
from dotenv import load_dotenv
from dotenv import dotenv_values
from google.cloud import pubsub_v1
import os
import sys
import json
from Adafruit_IO_Modified import MQTTClient
from console_logging.console import Console
import threading

app = Flask(__name__)

port = os.environ.get("PORT", 8080)

load_dotenv()
# config = dotenv_values(".env")
# print(config)


CREDENTIAL_PATH = os.getenv("CREDENTIAL_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_PATH
PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")
ADAFRUIT_IO_KEY = os.getenv("AIOKEY")
ADAFRUIT_IO_USERNAME = os.getenv("AIOUSER")
ADAFRUIT_IO_GROUP = os.getenv("DEVICEGROUP")
ADAFRUIT_IO_DEVICEID = ADAFRUIT_IO_USERNAME + ADAFRUIT_IO_GROUP + "-01"


def gcpPublish(attributes, PROJECT_ID, TOPIC_ID):
    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{PROJECT_ID}/topics/{TOPIC_ID}`
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    data = "A bedroom sensor is ready!"
    # Data must be a bytestring
    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data, **attributes)
    print(future.result())
    print(f"Published messages to {topic_path}.")


combined_data = {
    "userID": ADAFRUIT_IO_USERNAME,
    "EdgeDeviceID": ADAFRUIT_IO_DEVICEID,
    "dht11humi": "Null",
    "dht11temp": "Null",
    "soundsensor": "Null",
}

console = Console()


def connected(client):
    print(
        "Connected to Adafruit IO!  Listening for {0} changes...".format(
            ADAFRUIT_IO_GROUP
        )
    )
    client.subscribe_group(ADAFRUIT_IO_GROUP)


def disconnected(client):
    print("Disconnected from Adafruit IO!")
    sys.exit(1)


def message(client, feed_id, message):
    topic = list(json.loads(message)["feeds"].keys())[0]
    payload = json.loads(message)["feeds"][topic]
    try:
        combined_data[topic] = payload
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON from topic {topic}: {str(e)}")

    print("Feed {0} received new value: {1}".format(feed_id, message))
    json_object = json.dumps(combined_data, indent=4)
    # console.log(combined_data)
    gcpPublish(combined_data, PROJECT_ID, TOPIC_ID)


def run_mqtt_client():
    client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY, secure=False)
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = message
    client.connect()
    client.loop_blocking()


threading.Thread(target=run_mqtt_client).start()


@app.route("/")
def hello_world():
    return "Hello World! I am running on port " + str(port)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port, debug=True)
