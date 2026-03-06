from threading import Thread

from worker_thread import WorkerThread


class UXHandler():
    def __init__(self, workers: dict[str, WorkerThread]):
        self.chunk_statuses: dict
        self.thread = Thread(target=self.main_thread)
        self.thread.start()
        self.workers = workers
        
    def main_thread(self):
        while True:
            print("Hyperscrape Worker")
            print("Created by Hackerdude for Minerva")
            print()
            print(f"Currently downloading {len(self.workers)} files")
            print("Top 10 workers:")
            