import json
import random
import time

import requests
from tqdm import tqdm



print("Could not load params! - Regenerating...")
print("Testing your speeds...")
url = "https://myrient.erista.me/files/No-Intro/Sony%20-%20PlayStation%20Mobile%20%28PSN%29/4PhotoTweet%20%28Japan%29%20%28v1.01%29.zip"
response = requests.head(url, allow_redirects=True)
max_length = int(response.headers.get("content-length"))
print(f"Downloading up to {round(max_length/1000000, 2)}MB")

length_step = 1024
last_length = 0
last_speed = 0
length = 1024*200 # 200KB default
while length < max_length:
    start = time.time()
    pbar = tqdm(total=length, unit="B", unit_scale=True)
    random_offset = random.randint(0, max_length-length-1)
    with requests.get(url, allow_redirects=True, stream=True, headers={
        "User-Agent": "Hyperscrape Speed Tester/v1 (Created By Hackerdude)",
        "Range": f"bytes={random_offset}-{random_offset+length}"
    }) as r:
        for chunk in r.iter_content(8192):
            pbar.update(len(chunk))
    pbar.close()
    end = time.time()
    time_taken = end-start
    calculated_speed = (length/1000000)/time_taken
    print(f"Took {round(time_taken,2)}s to download {length}B ({calculated_speed}MB/s)")
    if (calculated_speed < last_speed):
        break
    last_length = length
    last_speed = calculated_speed
    length += length_step

print(f"Got ideal length: {last_length}")