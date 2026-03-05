import json
import os
import socket
import sys
import time
from threading import Lock, Thread

import requests
import tomllib
from tqdm import tqdm
from utils import test_download_speed
from websockets.sync.client import connect
from worker_thread import WorkerThread
from ws_message import WSMessage, WSMessageType

if sys.platform == "linux":
    os.system("ulimit -n 16384")  # Update ulimit lol

###
# DNS caching
# Inspired by: https://stackoverflow.com/a/22614367
###
dns_cache: dict[str, str] = {}
dns_cache_times: dict[str, int] = {}
from urllib3.util import connection

_super_create_connection = connection.create_connection


def cached_create_connection(
    address: tuple[str, int],
    timeout: connection._TYPE_TIMEOUT = connection._DEFAULT_TIMEOUT,
    source_address: tuple[str, int] | None = None,
    socket_options: connection._TYPE_SOCKET_OPTIONS | None = None,
) -> socket.socket:
    host, port = address
    hostname = ""
    if (not host in dns_cache) or time.time() - dns_cache_times[
        host
    ] > 60:  # Refresh DNS cache every 60 seconds
        addr_info = socket.getaddrinfo(host, port)
        dns_cache[host] = addr_info[0][-1][0]
        dns_cache_times[host] = time.time()
    hostname = dns_cache[host]
    return _super_create_connection(
        (hostname, port), timeout, source_address, socket_options
    )


connection.create_connection = cached_create_connection
###
###
###

config = None
with open("./config.toml", "rb") as file:
    config = tomllib.load(file)
COORDINATOR_ROOT = config["general"]["coordinator_url"]
RETRY_TIME = config["general"]["retry_time"]
VERSION = 2
USER_AGENT = f"HyperscrapeWorker/v{VERSION} (Created by Hackerdude for Minerva)"

print("""
=========================
=   HYPERSCRAPE WORKER  =
= Created By Hackerdude =
=========================
""")

print(
    "Would you like to connect to your Discord account and appear on the leaderboard?"
)
discord_token = None
answer = input("[Y/n] ")
if answer.lower() != "n":
    print("Please open the following URL in your browser")
    print(
        "https://discord.com/oauth2/authorize?client_id=1478862142793977998&response_type=code&redirect_uri=https%3A%2F%2Ffirehose.minerva-archive.org%2Fcode&scope=identify"
    )
    print("Once you have authorized the app, enter the returned code here:")
    discord_token = input("Enter code: ")
else:
    print(
        "[WARN] Running without Discord integration! YOU WILL NOT APPEAR ON THE LEADERBOARD :("
    )

params = {}


def save_params():
    with open("./params.json", "w") as file:
        file.write(json.dumps(params))


print("Reading params...")
try:
    with open("./params.json", "r") as file:
        params = json.loads(file.read())
except:
    print("Could not read params! Regenerating...")
    print("Testing raw download speed")
    params["download_speed"] = test_download_speed(config["general"]["speed_test_url"])
    print("Testing Myrient download speed")
    params["myrient_download_speed"] = test_download_speed(
        config["general"]["myrient_speed_test_url"]
    )
    save_params()

CHUNK_COUNT = int(
    params["download_speed"] // params["myrient_download_speed"]
)  # Request enough chunks to saturate
if config["general"]["max_chunk_count"] != 0:
    CHUNK_COUNT = min(CHUNK_COUNT, config["general"]["max_chunk_count"])

os.makedirs("./chunks", exist_ok=True)

CHUNK_THREADS: dict[str, WorkerThread] = {}


def main():
    while True:
        for id in list(CHUNK_THREADS.keys()):
            CHUNK_THREADS[id].stop()
            del CHUNK_THREADS[id]
        print("\nTrying to connect to coordinator")
        with connect(f"{COORDINATOR_ROOT}") as websocket:
            websocket_lock = Lock()

            worker_id = None
            try:
                with websocket_lock:
                    websocket.send(
                        WSMessage(
                            WSMessageType.REGISTER,
                            {
                                "version": VERSION,
                                "max_concurrent": CHUNK_COUNT,
                                "discord_token": discord_token,
                            },
                        ).encode()
                    )
                    response: WSMessage = WSMessage.decode(websocket.recv())
                    if response.get_type() != WSMessageType.REGISTER_RESPONSE:
                        print(
                            f"Error: Unable to connect to coordinator ({response.get_payload()}), retrying in {RETRY_TIME}s..."
                        )
                        time.sleep(RETRY_TIME)
                        continue
                worker_id = response.get_payload()["worker_id"]
            except Exception as e:
                print(
                    f"Error: Unable to connect to coordinator ({e}), retrying in {RETRY_TIME}s..."
                )
                time.sleep(RETRY_TIME)
            if worker_id == None:
                print("COULD NOT CONNECT TO COORDINATOR!")
                print("Will try again in one minute...")
                time.sleep(60)

            print(f"Connected to coordinator with ID: {worker_id}")
            print(
                f"This worker can request up to {CHUNK_COUNT} chunks at once - This can be overriden in the configuration file"
            )
            while True:
                for chunk_id in list(CHUNK_THREADS.keys()):
                    chunk_thread = CHUNK_THREADS[chunk_id]
                    if chunk_thread.get_websocket_failed():
                        for id in list(CHUNK_THREADS.keys()):
                            CHUNK_THREADS[id].stop()
                            del CHUNK_THREADS[id]
                        print(
                            f"[ERR] Websocket failure - Trying to reconnect in {RETRY_TIME}s"
                        )
                        time.sleep(RETRY_TIME)
                        break  # Reconnect
                    if not chunk_thread.is_alive():
                        del CHUNK_THREADS[chunk_id]

                if len(CHUNK_THREADS) == CHUNK_COUNT:
                    time.sleep(1)  # Don't explode the CPU
                    continue

                # We have space to get more chunk threads
                chunks_to_request = CHUNK_COUNT - len(CHUNK_THREADS)
                chunks = {}
                with websocket_lock:
                    try:
                        websocket.send(
                            WSMessage(
                                WSMessageType.GET_CHUNKS, {"count": chunks_to_request}
                            ).encode()
                        )
                        response: WSMessage = WSMessage.decode(websocket.recv())
                        if response.get_type() != WSMessageType.CHUNK_RESPONSE:
                            print(
                                f"Error retrieving chunks ({response.get_payload()}):"
                            )
                            time.sleep(RETRY_TIME)
                            break
                        chunks = response.get_payload()
                        if len(chunks) == 0:
                            if len(CHUNK_THREADS) == 0:
                                print("Waiting for new chunks to download...")
                            time.sleep(RETRY_TIME)
                            continue
                    except Exception as e:
                        print(f"Error retrieving chunks ({e}):")
                        time.sleep(RETRY_TIME)
                        break

                if len(chunks) > 0:
                    print(f"Got {len(chunks)}/{chunks_to_request} chunks to download")
                for chunk_id in chunks:
                    if chunk_id in CHUNK_THREADS:
                        continue
                    chunk = chunks[chunk_id]
                    CHUNK_THREADS[chunk_id] = WorkerThread(
                        chunk_id,
                        chunk["file_id"],
                        chunk["url"],
                        chunk["range"][0],
                        chunk["range"][1],
                        websocket,
                        websocket_lock,
                        USER_AGENT,
                        config["general"]["subchunk_size"],
                    )
                    CHUNK_THREADS[chunk_id].start()


if __name__ == "__main__":
    main()
