#!/usr/bin/env python3
"""Download fixed mainnet windows with bounded requests and fail closed."""
import concurrent.futures
import os
import pathlib
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = 'https://checkpoints.mainnet.sui.io/'
PRIMARY = os.environ.get('TH_PRIMARY') == '1'
STARTS = ([331300000 + 1020000 * i for i in range(12)] if PRIMARY else
          [339500000, 339950000, 340400000, 340850000, 341300000, 341750000, 342200000, 342650000])
COUNT = 2000 if PRIMARY else 100
LIMIT = 10_000_000_000
stop = threading.Event()
lock = threading.Lock()
total = 0

def fetch(seq, root):
    global total
    if stop.is_set(): return
    path = root / f'{seq}.binpb.zst'
    if path.exists(): return
    for attempt in range(4):
        if stop.is_set(): return
        try:
            with urllib.request.urlopen(BASE + path.name, timeout=45) as response:
                chunks = []
                while True:
                    with lock:
                        reserved = min(65536, LIMIT - total)
                        if reserved <= 0:
                            stop.set()
                            raise RuntimeError('download limit reached')
                        total += reserved
                    chunk = response.read(reserved)
                    with lock:
                        total -= reserved - len(chunk)
                    if not chunk: break
                    chunks.append(chunk)
            path.write_bytes(b''.join(chunks))
            return
        except urllib.error.HTTPError as e:
            if e.code == 429 or (500 <= e.code < 600 and attempt == 3):
                stop.set(); raise RuntimeError(f'{seq}: HTTP {e.code}') from e
            if not 500 <= e.code < 600:
                stop.set(); raise
        except (OSError, TimeoutError):
            if attempt == 3:
                stop.set(); raise
        time.sleep(2 ** attempt)

def main():
    global total
    root = pathlib.Path(sys.argv[1]) / 'raw'
    root.mkdir(parents=True, exist_ok=True)
    if PRIMARY:
        # Count retained pilot and already downloaded primary files against the cap.
        total = 10_000_000 + sum(p.stat().st_size for p in (root.parent.parent / 'raw').glob('*') if p.is_file())
        total += sum(p.stat().st_size for p in root.glob('*') if p.is_file())
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for result in pool.map(lambda n: fetch(n, root), (n for s in STARTS for n in range(s, s + COUNT))):
            pass
    print(f'accounted download bytes {total}')

if __name__ == '__main__': main()
