from threading import Thread
import tomllib
import requests
import json
import time
import os

from tqdm import tqdm

from utils import test_download_speed
from worker_thread import WorkerContext

config = None
with open("./config.toml", 'rb') as file:
    config = tomllib.load(file)
COORDINATOR_ROOT = config["general"]["coordinator_url"]
RETRY_TIME = config["general"]["retry_time"]
VERSION = 1
USER_AGENT = f"HyperscrapeWorker/v{VERSION} (Created by Hackerdude for Minerva)"

print("""
=========================
=   HYPERSCRAPE WORKER  =
= Created By Hackerdude =
=========================
""")

params = {}

def save_params():
    with open("./params.json", 'w') as file:
        file.write(json.dumps(params))

print("Reading params...")
try:
    with open("./params.json", 'r') as file:
        params = json.loads(file.read())
except:
    print("Could not read params! Regenerating...")
    print("Testing raw download speed")
    params["download_speed"] = test_download_speed(config["general"]["speed_test_url"])
    print("Testing Myrient download speed")
    params["myrient_download_speed"] = test_download_speed(config["general"]["myrient_speed_test_url"])*2 # @FIXME: *2?
    save_params()

CHUNK_COUNT = int(params["download_speed"]//params["myrient_download_speed"]) # Request enough chunks to saturate
if (config["general"]["max_chunk_count"] != 0):
    CHUNK_COUNT = config["general"]["max_chunk_count"]

os.makedirs("./chunks", exist_ok=True)

CHUNK_THREADS = {}
for MAIN_RETRIES in range(3):
    print("Connecting to coordinator")
    worker_id = None
    for CONNECT_RETRIES in range(3):
        response = requests.post(f"{COORDINATOR_ROOT}/workers", json={
            "version": VERSION,
            "max_upload": 100, # HARDCODED FOR NOW!
            "max_download": params["download_speed"],
            "max_per_file_speed": params["myrient_download_speed"],
            "threads": os.cpu_count()
        })
        if (response.status_code != 200):
            print(f"Error: Unable to connect to coordinator ({response.status_code}), retrying in {RETRY_TIME}s...")
            print(f"{response.text}")
            time.sleep(RETRY_TIME)
            continue
        worker_id = response.json()["token"]

    print(f"Connected to coordinator with ID: {worker_id}")
    print(f"This worker can request up to {CHUNK_COUNT} chunks at once - This can be overriden in the configuration file")
    while True:
        print(f"Requesting up to {CHUNK_COUNT} new chunks")
        response = requests.get(f"{COORDINATOR_ROOT}/chunks", params={
            "n": CHUNK_COUNT
        }, headers={
            "authorization": f"Bearer {worker_id}"
        })
        if (response.status_code != 200):
            print(f"Error retrieving chunks ({response.status_code}):")
            print(response.text)
            time.sleep(RETRY_TIME)
            continue
        chunks = response.json()
        if (len(chunks) == 0):
            print("No new chunks detected from server.")
            time.sleep(RETRY_TIME)
            continue

        for chunk_id in chunks:
            chunk = chunks[chunk_id]
            file_location = f"./chunks/{chunk_id}.bin"
            context = WorkerContext(chunk_id,
                                    chunk["url"],
                                    file_location,
                                    chunk["range"][0],
                                    chunk["range"][1],
                                    chunk["destination"],
                                    COORDINATOR_ROOT + "/status",
                                    worker_id,
                                    USER_AGENT,
                                    tqdm()
                                )