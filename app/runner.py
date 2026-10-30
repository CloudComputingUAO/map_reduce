import json
import os
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

import redis

client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), decode_responses=True)
file = Path(sys.argv[1] if len(sys.argv) > 1 else "data/sample.txt")
parts = int(sys.argv[2]) if len(sys.argv) > 2 else 6
if parts < 1:
    raise SystemExit("Partitions must be greater than zero")
lines = file.read_text(encoding="utf-8").splitlines()
if not lines:
    raise SystemExit("Input file is empty")
run = uuid.uuid4().hex[:10]
key = f"mapreduce:{run}:mapped"
chunks = [lines[i::parts] for i in range(parts)]
chunks = [chunk for chunk in chunks if chunk]
print(f"RUN {run} | SPLIT: {len(lines)} lines into {len(chunks)} partitions", flush=True)
for index, chunk in enumerate(chunks):
    client.lpush("mapreduce:tasks", json.dumps({"run": run, "partition": index, "lines": "\n".join(chunk)}))
print("MAP: waiting for workers", flush=True)
start = time.monotonic()
while client.hlen(key) < len(chunks):
    if time.monotonic() - start > 90:
        raise SystemExit("Timed out waiting for mappers. Check worker logs.")
    time.sleep(0.2)
intermediate = client.hgetall(key)
print("SHUFFLE: grouping values by key", flush=True)
groups = defaultdict(list)
for partition, payload in sorted(intermediate.items(), key=lambda item: int(item[0])):
    mapped = json.loads(payload)
    print(f"  partition {partition}: {mapped}", flush=True)
    for word, count in mapped.items():
        groups[word].append(count)
print("REDUCE: summing grouped values", flush=True)
results = {word: sum(values) for word, values in sorted(groups.items())}
for word, count in results.items():
    print(f"  {word}: {groups[word]} -> {count}", flush=True)
print(f"RESULT: {json.dumps(results, sort_keys=True)}", flush=True)
client.delete(key)
