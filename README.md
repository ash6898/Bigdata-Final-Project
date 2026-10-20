# News API Streaming Pipeline: Kafka, Python and Spark Structured Streaming

Final project for **PROG8451 Big Data Integration & Storage** in the **Big Data Solutions Architecture** program at **Conestoga College**. It was built as a two-person team project with Harshitha Anishetty. See [`Deliverables/technical contribution.docx`](Deliverables/) for who did what.

The pipeline pulls news articles from [NewsAPI](https://newsapi.org/), streams them through Apache Kafka, flattens them into JSON Lines files with pandas, and then runs windowed aggregations on them with Spark Structured Streaming.

## Architecture

```
NewsAPI /v2/everything
        │  (HTTP, paginated, 1 request/second)
        ▼
producer.py ──► Kafka topic "newsapi-sink" (localhost:9092)
                        │
                        ▼
                consumer.py ──► newsapi_data<N>.json (one JSON Lines file per page, local disk)
                                        │  (copied into HDFS: hdfs:///final_assignment/)
                                        ▼
                        Spark Structured Streaming ("Spark Streaming.txt")
                          ├─ articles per author, 10-second windows ──► output_author/ (JSON)
                          └─ articles per source, 10-second windows ──► output_source/ (JSON)
```

| Component | File | What it does |
|---|---|---|
| Producer | `producer.py` | Calls NewsAPI `/v2/everything` (query `football`, sorted by popularity, 50 articles per page). It publishes each page of results as one JSON message to the Kafka topic `newsapi-sink`, moves to the next page every second, and sends a `fetch_done` marker to the topic when the API returns HTTP 426 (plan limit reached). |
| Consumer | `consumer.py` | Subscribes to `newsapi-sink` and flattens each page's `articles` array with `pandas.json_normalize`. It writes each page to a numbered JSON Lines file (`newsapi_data1.json`, `newsapi_data2.json`, ...) and logs the `fetch_done` marker. |
| Stream processing | `Spark Streaming.txt` | `spark-shell` (Scala) session that reads the JSON files from `hdfs:///final_assignment/` as a stream using an explicit article schema. It adds an event-time column with a 10-second watermark, counts articles per author and per source in 10-second windows, and writes the results as JSON with checkpointing (append mode). |
| Report | `Deliverables/` | Final project report (which also covers loading the data into Hive for analysis) and the technical-contribution summary. |

## Tech stack

- **Python 3**: `kafka-python`, `requests`, `pandas`
- **Apache Kafka** (with ZooKeeper) as the message broker
- **Apache Spark 3.5** Structured Streaming (Scala, `spark-shell`) with the `spark-sql-kafka-0-10_2.12:3.5.0` package
- **HDFS** as the streaming source directory
- **NewsAPI** as the data source

## Prerequisites

- Python 3 with `pip install kafka-python requests pandas`
- A Kafka broker on `localhost:9092` (plus ZooKeeper, depending on your Kafka version)
- Hadoop/HDFS and Spark 3.5.x for the streaming step
- A NewsAPI key ([free developer key](https://newsapi.org/register))

## Configuration

| Variable | Required | Used by | Description |
|---|---|---|---|
| `NEWS_API_KEY` | Yes | `producer.py` | Your NewsAPI key. The producer exits with an error if it isn't set. |

Copy `.env.example` to `.env`, fill in your key, and load it into your shell. `.env` is git-ignored, so the key never gets committed.

```sh
cp .env.example .env        # then edit .env
set -a; source .env; set +a # export the variables into the current shell
```

Other settings are constants at the top of the scripts: the Kafka topic and broker in both scripts, the NewsAPI query parameters in `producer.py`, the output directory (`json_file_path`) in `consumer.py`, and the HDFS and output paths in `Spark Streaming.txt`. Change them to fit your environment.

## How to run

1. **Start Kafka** (and ZooKeeper if your setup needs it), then create the topic `newsapi-sink` if auto-creation is disabled.
2. **Start the consumer** (it waits for messages). First set `json_file_path` in `consumer.py` to a directory that exists on your machine.
   ```sh
   python3 consumer.py
   ```
3. **Run the producer** in another terminal:
   ```sh
   export NEWS_API_KEY=your_key_here   # or source .env as shown above
   python3 producer.py
   ```
4. **Copy the JSON files into HDFS:**
   ```sh
   hdfs dfs -mkdir -p /final_assignment
   hdfs dfs -put /path/to/newsapi_data*.json /final_assignment/
   ```
5. **Run the streaming aggregations.** Start `spark-shell` with the command on the first line of `Spark Streaming.txt`, then paste in the rest of the file:
   ```sh
   spark-shell --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
   ```
   Results land in `output_author/` and `output_source/` under the configured output path.

## Notes

- NewsAPI's free developer plan limits how many results you can page through. The producer treats HTTP 426 as "done" and sends the `fetch_done` marker.
- Paths in `consumer.py` and `Spark Streaming.txt` point to the original course cluster's home directory, so update them before running.
