from threading import Lock, Thread
import requests
from tqdm import tqdm
import os
from websockets import ClientConnection

import urllib.parse

import websockets

from ws_message import WSMessage, WSMessageType

class WorkerThread():
    def __init__(self, chunk_id: str, file_id: str, url: str, range_start: int, range_end: int, websocket: ClientConnection, websocket_lock: Lock, user_agent: str, subchunk_size: int):
        self.chunk_id = chunk_id
        self.file_id = file_id
        self.url = url
        self.range_start = range_start
        self.range_end = range_end
        self.websocket = websocket
        self.websocket_lock = websocket_lock
        self.user_agent = user_agent
        self.pbar = tqdm(unit="B", unit_scale=True, total=self.range_end - self.range_start)
        self.pbar.desc = f"Streaming from {urllib.parse.unquote(os.path.basename(self.url))}"
        self.should_run = True
        self.thread = Thread(target=self.worker_thread)
        self.subchunk_size = subchunk_size
        self.websocket_failed = False

    def get_websocket_failed(self):
        return self.websocket_failed

    def is_alive(self):
        return self.thread.is_alive()

    def start(self):
        self.thread.start()

    def stop(self):
        self.should_run = False

    def worker_thread(self):
        try:
            response = requests.get(self.url, headers={
                "User-Agent": self.user_agent,
                "Range": f"bytes={self.range_start}-{self.range_end-1}" # We do -1 because it seems range is inclusive
            }, stream=True)
            if (response.status_code != 206): # We expect 206 here because we requested a range, full content would break file on upload
                raise Exception(f"Received non 206 code ({response.status_code})")
            
            # Validate content size
            expected_size = self.range_end - self.range_start
            content_length = response.headers.get('Content-Length')
            if content_length:
                content_length = int(content_length)
                if content_length != expected_size:
                    raise Exception(f"Incorrect Content-Length, expected {expected_size} bytes, got {content_length} bytes")

            for chunk in response.iter_content(self.subchunk_size): # read and upload 8KB at a time
                if (not self.should_run):
                    response.close()
                    self.pbar.close() # Cleanup pbar properly
                    del self.pbar
                    return # Terminate
                with self.websocket_lock:
                    self.websocket.send(WSMessage(WSMessageType.UPLOAD_SUBCHUNK, {
                        "chunk_id": self.chunk_id,
                        "file_id": self.file_id,
                        "payload": chunk
                    }).encode())
                    ws_response: WSMessage = WSMessage.decode(self.websocket.recv())
                if (ws_response.get_type() != WSMessageType.OK_RESPONSE):
                    print(f"[ERR]: Could not upload {urllib.parse.unquote(os.path.basename(self.url))}")
                    print(ws_response.get_payload())
                    self.pbar.close()
                    with self.websocket_lock:
                        self.websocket.send(WSMessage(WSMessageType.DETACH_CHUNK, {
                            "chunk_id": self.chunk_id
                        }).encode())
                        ws_response: WSMessage = WSMessage.decode(self.websocket.recv()) # Just wait for the next message
                    self.pbar.close() # Cleanup pbar properly
                    del self.pbar
                    return
                self.pbar.update(len(chunk))
        except websockets.exceptions.WebSocketException as e:
            self.websocket_failed = True
            self.should_run = False
            print(f"[ERR]: Failed to connect to the server!")
            print(e)
        except Exception as e:
            print(f"[ERR]: Could not download {urllib.parse.unquote(os.path.basename(self.url))}")
            print(e)
            try:
                with self.websocket_lock:
                    self.websocket.send(WSMessage(WSMessageType.DETACH_CHUNK, {
                        "chunk_id": self.chunk_id
                    }).encode())
                    ws_response: WSMessage = WSMessage.decode(self.websocket.recv()) # Just wait for the next message
            except:
                self.websocket_failed = True
                self.should_run = False
                print(f"[ERR]: Failed to connect to the server!")
                print(e)
        self.pbar.close() # Cleanup pbar properly
        del self.pbar