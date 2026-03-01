from threading import Lock, Thread
import time

import requests


class StatusHandler():
    def __init__(self, status_endpoint: str, status_interval: int):
        self.statuses = {}
        self.status_endpoint = status_endpoint
        self.status_interval = status_interval
        self.last_status_time = 0
        self.auth_token = ""

        self._thread = Thread(target=self.main_thread)
        self._thread.start()

    def clear_all(self):
        self.statuses = {}

    def set_auth_token(self, auth_token: str):
        self.auth_token = auth_token

    def main_thread(self):
        while True:
            try:
                if (len(self.statuses) == 0):
                    continue
                requests.put(self.status_endpoint, headers={
                            "authorization": f"Bearer {self.auth_token}"
                }, json=self.statuses)
                self.last_status_time = time.time()
                time.sleep(self.status_interval)
            except Exception as e:
                print("[WARN] Could not send status:")
                print(e)

    def update_status(self, chunk_id: str, downloaded: int):
        self.statuses[chunk_id] = {
            "downloaded": downloaded
        }

    def remove_status(self, chunk_id: str):
        if (chunk_id in self.statuses):
            del self.statuses[chunk_id]

    def wait_status_sent(self):
        time_until_next = self.status_interval - (time.time() - self.last_status_time)
        time.sleep(time_until_next)