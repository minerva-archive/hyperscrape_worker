import requests
from tqdm import tqdm
import time
import os

import urllib.parse

from status_handler import StatusHandler

class WorkerContext():
    def __init__(self, chunk_id: str, file_id: str, url: str, file_location: str, range_start: int, range_end: int, upload_endpoint: str, status_handler: StatusHandler, auth_token: str, user_agent: str, status_interval: int, pbar: tqdm):
        self.chunk_id = chunk_id
        self.file_id = file_id
        self.url = url
        self.file_location = file_location
        self.range_start = range_start
        self.range_end = range_end
        self.upload_endpoint = upload_endpoint
        self.status_handler = status_handler
        self.auth_token = auth_token
        self.user_agent = user_agent
        self.status_interval = status_interval
        self.pbar = pbar

    def read_file_with_progress(self):
        downloaded = self.range_end - self.range_start
        uploaded = 0
        with open(self.file_location, 'rb') as file:
            read_size = 1023**2 # upload in 1MiB chunks
            data = file.read(read_size)
            while (len(data) > 0):
                yield data
                uploaded += len(data)
                self.pbar.update(len(data))
                data = file.read(read_size)

def worker_thread(context: WorkerContext):
    context.pbar.clear()
    context.pbar.total = context.range_end - context.range_start
    context.pbar.desc = f"Downloading from {urllib.parse.unquote(os.path.basename(context.url))}"
    response = requests.get(context.url, headers={
        "User-Agent": context.user_agent,
        "Range": f"bytes={context.range_start}-{context.range_end-1}" # We do -1 because it seems range is inclusive
    }, stream=True)

    with open(context.file_location, 'wb') as file:
        downloaded = 0
        try:
            for chunk in response.iter_content(8192): # 8192KB size seems good
                file.write(chunk)
                context.pbar.update(len(chunk))
                downloaded += len(chunk)
                context.status_handler.update_status(context.chunk_id, downloaded)
        except:
            context.status_handler.remove_status(context.chunk_id)
            print(f"[ERR] Failed to download {context.url}")
            requests.put(context.status_endpoint, headers={
                    "authorization": f"Bearer {context.auth_token}"
                }, json = {
                    context.chunk_id: None
            })
            context.pbar.close()
            # Delete the file
            os.remove(context.file_location)
            return
    
    context.status_handler.wait_status_sent()
    context.status_handler.remove_status(context.chunk_id)
    
    # It's done, upload it now
    try:
        with open(context.file_location, "rb") as file:
            context.pbar.reset()
            context.pbar.desc = f"Uploading {context.chunk_id}"
            requests.put(context.upload_endpoint, headers={
                "authorization": f"Bearer {context.auth_token}"
            }, params={
                "chunk_id": context.chunk_id,
                "file_id": context.file_id
            }, data=context.read_file_with_progress())
    except:
        print(f"[ERR] Failed to download {context.url}")
        requests.put(context.status_endpoint, headers={
                "authorization": f"Bearer {context.auth_token}"
            }, json = {
                context.chunk_id: None
        })
    context.pbar.close()
    # Delete the file
    os.remove(context.file_location)