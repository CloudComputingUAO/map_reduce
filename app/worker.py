import json
import os
import socket
import time
from collections import Counter

import redis

client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), decode_responses=True)
worker = socket.gethostname()

print(f"Worker {worker} ready", flush=True)
while True:
    _, payload = client.brpop("mapreduce:tasks")
    task = json.loads(payload)
    counts = Counter(word.lower().strip('.,!?;:"()') for word in task["lines"].split())
    counts.pop("", None)
    time.sleep(1)
    client.hset(f'mapreduce:{task["run"]}:mapped', task["partition"], json.dumps(counts))
    print(f'{worker} mapped partition {task["partition"]}: {dict(counts)}', flush=True)
