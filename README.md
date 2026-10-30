# MapReduce with Python, Redis and Docker Compose

A classroom lab that builds on the distributed-workers exercise. No Hadoop required.

## What changes from the previous lab?

Previously, each worker handled an independent task. Here, each worker **maps a text partition into word counts**. The runner then **shuffles** intermediate pairs by word and **reduces** each group to a total count.

This is a teaching implementation of the MapReduce pattern, **not Hadoop MapReduce**. Redis is a shared queue and intermediate-results store. The runner performs shuffle and reduce centrally; the map phase runs across multiple worker containers. Containers may share the same physical host.

## Architecture

Open `docs/architecture.mmd` and `docs/pipeline.mmd` in a Mermaid-compatible editor.

```text
Text file -> Split -> Redis queue -> Map workers
                                   |
                                   v
                         Redis intermediate counts
                                   |
                                   v
                          Shuffle by word -> Reduce -> Totals
```

## 1. Start with one worker

```bash
docker compose up -d --build --scale worker=1 redis worker
docker compose ps
```

## 2. Run WordCount

```bash
docker compose run --rm runner
```

Follow the worker logs in a second terminal:

```bash
docker compose logs -f worker
```

Observe `SPLIT`, `MAP`, `SHUFFLE`, `REDUCE` and `RESULT` in the runner output. Each map task waits one second so work distribution is visible.

## 3. Scale the mappers

```bash
docker compose up -d --scale worker=3 worker
docker compose run --rm runner
```

Compare execution times with one and three workers. Note that shuffle and reduce still run in one process; this is intentionally a simplified design.

## 4. Try a different dataset

Edit `data/sample.txt`, then run:

```bash
docker compose run --rm runner python -u app/runner.py data/sample.txt 4
```

The final argument is the maximum number of partitions.

## 5. Inspect Redis

```bash
docker compose exec redis redis-cli LLEN mapreduce:tasks
```

Mapped results are temporary and removed after each successful run.

## 6. Classroom questions

- Which stage can run concurrently across workers?
- Why does the runner need to group keys before reducing?
- Would three workers always make the whole job three times faster?
- What happens if a worker crashes after taking a task?
- What would need to change to distribute reducers as well?

## 7. Student assignment

Modify the program to analyze application logs instead of words. Each log line begins with `INFO`, `WARN`, or `ERROR`. Implement a mapper that emits `(level, 1)` and produce total counts per level. Run with one and three workers, report observed times, and explain the shuffle stage.

## Limitations

The queue uses `BRPOP`, which removes tasks immediately. If a worker crashes after consuming a task, the runner may time out because there is no acknowledgement or retry mechanism. Do not describe this implementation as fault tolerant. For production, use reliable delivery, acknowledgements, retries, durable storage, and distributed reducer partitioning.

## Cleanup

```bash
docker compose down -v
```
