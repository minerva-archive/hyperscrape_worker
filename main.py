from threading import Thread
import tomllib
import requests
import json
import time
import os

from tqdm import tqdm

from utils import test_download_speed
from worker_thread import WorkerContext, worker_thread

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

CHUNK_THREADS: dict[str, Thread] = {}
while True:
    print("\nTrying to connect to coordinator")
    worker_id = None
    for CONNECT_RETRIES in range(3):
        try:
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
            worker_id = response.json()["worker_id"]
            auth_token = response.json()["auth_token"]
        except Exception as e:
            print(f"Error: Unable to connect to coordinator ({e}), retrying in {RETRY_TIME}s...")
            time.sleep(RETRY_TIME)
    if (worker_id == None):
        print("COULD NOT CONNECT TO COORDINATOR!")
        print("Will try again in one minute...")
        time.sleep(60)

    print(f"Connected to coordinator with ID: {worker_id}")
    print(f"This worker can request up to {CHUNK_COUNT} chunks at once - This can be overriden in the configuration file")
    while True:
        for chunk_id in list(CHUNK_THREADS.keys()):
            chunk_thread = CHUNK_THREADS[chunk_id]
            if (not chunk_thread.is_alive()):
                del CHUNK_THREADS[chunk_id]
        
        if (len(CHUNK_THREADS) == CHUNK_COUNT):
            time.sleep(1) # Don't explode the CPU
            continue

        # We have space to get more chunk threads
        chunks_to_requeset = CHUNK_COUNT - len(CHUNK_THREADS)
        try:
            response = requests.get(f"{COORDINATOR_ROOT}/chunks", params={
                "n": chunks_to_requeset
            }, headers={
                "authorization": f"Bearer {auth_token}"
            })
            if (response.status_code != 200):
                print(f"Error retrieving chunks ({response.status_code}):")
                print(response.text)
                time.sleep(RETRY_TIME)
                break
            chunks = response.json()
            if (len(chunks) == 0):
                if (len(CHUNK_THREADS) == 0):
                    print("Waiting for new chunks to download...")
                time.sleep(RETRY_TIME)
                continue
        except Exception as e:
            print(f"Error retrieving chunks ({e}):")
            time.sleep(RETRY_TIME)
            break

        if (len(chunks) > 0):
            print(f"Got {len(chunks)}/{chunks_to_requeset} chunks to download")
        for chunk_id in chunks:
            if (chunk_id in CHUNK_THREADS):
                continue
            chunk = chunks[chunk_id]
            file_location = f"./chunks/{chunk_id}_{worker_id}.bin"
            context = WorkerContext(chunk_id,
                                    chunk["file_id"],
                                    chunk["url"],
                                    file_location,
                                    chunk["range"][0],
                                    chunk["range"][1],
                                    COORDINATOR_ROOT + "/upload",
                                    COORDINATOR_ROOT + "/status",
                                    auth_token,
                                    USER_AGENT,
                                    config["general"]["status_interval"],
                                    tqdm(unit="B", unit_scale=True)
                                )
            CHUNK_THREADS[chunk_id] = Thread(target=worker_thread, args=(context,))
            CHUNK_THREADS[chunk_id].start()