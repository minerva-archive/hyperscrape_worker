import requests
from tqdm import tqdm
import os

import urllib.parse

from status_handler import StatusHandler

class WorkerContext():
    def __init__(self, chunk_id: str, file_id: str, url: str, range_start: int, range_end: int, upload_endpoint: str, status_handler: StatusHandler, auth_token: str, user_agent: str, status_interval: int, pbar: tqdm):
        self.chunk_id = chunk_id
        self.file_id = file_id
        self.url = url
        self.range_start = range_start
        self.range_end = range_end
        self.upload_endpoint = upload_endpoint
        self.status_handler = status_handler
        self.auth_token = auth_token
        self.user_agent = user_agent
        self.status_interval = status_interval
        self.pbar = pbar

    def download_file_with_progress(self):
        self.pbar.clear()
        self.pbar.total = self.range_end - self.range_start
        self.pbar.desc = f"Streaming from {urllib.parse.unquote(os.path.basename(self.url))}"
        try:
            response = requests.get(self.url, headers={
                "User-Agent": self.user_agent,
                "Range": f"bytes={self.range_start}-{self.range_end-1}" # We do -1 because it seems range is inclusive
            }, stream=True)

            downloaded = 0
            try:
                for chunk in response.iter_content(8192): # read and upload 8KB at a time
                    yield chunk
                    self.pbar.update(len(chunk))
                    downloaded += len(chunk)
                    self.status_handler.update_status(self.chunk_id, downloaded)
            except Exception as e:
                self.pbar.close()
                self.status_handler.nullify_status(self.chunk_id)
                print(f"[ERR] Failed to download {self.url}")
                print(e)
                return
        except Exception as e:
            self.pbar.close()
            self.status_handler.nullify_status(self.chunk_id)
            print(f"[ERR]: Could not download {urllib.parse.unquote(os.path.basename(self.url))}")
            print(e)
            raise e

def worker_thread(context: WorkerContext):
    # We stream directly to the server from teh downloaded data
    try:
        requests.put(context.upload_endpoint, headers={
            "authorization": f"Bearer {context.auth_token}"
        }, params={
            "chunk_id": context.chunk_id,
            "file_id": context.file_id
        }, data=context.download_file_with_progress())
    except Exception as e:
        print(f"[ERR] Failed to upload {context.url}")
        print(e)
        context.status_handler.nullify_status(context.chunk_id)
    context.pbar.close()