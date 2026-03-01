import requests
from tqdm import tqdm
import os

import urllib.parse

class WorkerContext():
    def __init__(self, chunk_id: str, url: str, file_location: str, range_start: int, range_end: int, upload_endpoint: str, status_endpoint: str, auth_token: str, user_agent: str, pbar: tqdm):
        self.chunk_id = chunk_id
        self.url = url
        self.file_location = file_location
        self.range_start = range_start
        self.range_end = range_end
        self.upload_endpoint = upload_endpoint
        self.status_endpoint = status_endpoint
        self.auth_token = auth_token
        self.user_agent = user_agent
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

                # Update status to reflect update amount
                try:
                    requests.get(self.status_endpoint, headers={
                        "authorization": f"Bearer {self.auth_token}"
                    }, data = {
                        self.chunk_id: {
                            "downloaded": downloaded,
                            "uploaded": uploaded
                        }
                    })
                except Exception as e:
                    print(f"[WARN]: Could not update status: {e}")

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
        for chunk in response.iter_content(8192): # 8192KB size seems good
            file.write(chunk)
            context.pbar.update(len(chunk))
            downloaded += len(chunk)
            try:
                requests.put(context.status_endpoint, headers={
                    "authorization": f"Bearer {context.auth_token}"
                }, json = {
                    context.chunk_id: {
                        "downloaded": downloaded,
                        "uploaded": 0
                    }
                })
            except Exception as e:
                print(f"[WARN]: Could not update status: {e}")
    
    # It's done, upload it now
    with open(context.file_location, "rb") as file:
        context.pbar.reset()
        context.pbar.desc = f"Uploading {context.chunk_id}"
        requests.put(context.upload_endpoint, headers={
            "authorization": f"Bearer {context.auth_token}"
        }, params={
            "chunk_id": context.chunk_id
        }, data=context.read_file_with_progress())
    context.pbar.close()
    # Delete the file
    os.remove(context.file_location)