import time

import requests
from tqdm import tqdm


def test_download_speed(url):
    response = requests.head(url, allow_redirects=True)
    length = int(response.headers.get("content-length"))
    print(f"Downloading {round(length / 1000000, 2)}MB")
    start = time.time()
    pbar = tqdm(total=length, unit="B", unit_scale=True)
    with requests.get(
        url,
        allow_redirects=True,
        stream=True,
        headers={"User-Agent": "Hyperscrape Speed Tester/v1 (Created By Hackerdude)"},
    ) as r:
        for chunk in r.iter_content(8192):
            pbar.update(len(chunk))
    pbar.close()
    end = time.time()
    time_taken = end - start
    calculated_speed = (length / 1000000) / time_taken
    print(f"Took {time_taken}s to download {length}B ({calculated_speed}MB/s)")
    return calculated_speed
