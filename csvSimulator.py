"""
csvSimulator.py — Data ingestion simulator

Reads a CSV sequentially and emits gzip-compressed CSV sub-batches at 
approximately the requested frequency times the number of threads given.

Usage:
    python csvSimulator.py --topic <topic>
        => to test with default parameters on desired topic

    python csvSimulator.py --topic <topic> --frequency 1000 --num-threads 10 --key-column-id 3 --max-batches 0
        => total frequency of 10k rows/s balanced between 10 threads, keyed by column 3, going through all the CSV
"""
from argparse import ArgumentParser
from collections.abc import Iterator
from csv import reader as get_csv_reader, writer as get_csv_writer
from gzip import compress as gzip_compress
from io import StringIO
from time import sleep
from dataclasses import dataclass
from pathlib import Path
from numpy.random import lognormal, poisson
from threading import Thread, Lock, current_thread
from confluent_kafka import Producer
from os import environ
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider    

SIGMA_LOG = 1.2  # log-normal shape parameter (burstiness knob)
DEFAULT_FREQUENCY = 500  # target rows/second per thread
DEFAULT_NUM_THREADS = 5  # NUmber of concurrent threads to run (each simulating an independent data stream with the same frequency)
DEFAULT_KEY_COLUMN_ID = 1  # column name to use as Kafka message key
DEFAULT_MAX_BATCHES = 100  # number of batches to go through through the CSV (for testing) per thread. If set = 0, goes until the end.
KAFKA_BOOTSTRAP_SERVERS = environ["BOOTSTRAP_SERVER"]
REGION = environ["REGION"]
CSV_INPUT_PATH = Path(environ["CSV_INPUT_PATH"])
CSV_DELMITER = "^"

def oauth_callback(config):
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, expiry_ms / 1000  # confluent expects seconds, not ms

@dataclass
class Config:
    """
    Configuration for the CSV simulator, parsed from command-line arguments
    """

    topic: str
    frequency: float
    num_threads: int
    key_column_id: int
    max_batches: int


def parse_args() -> Config:
    """
    Parses the arguments. See csvSimulator.py --help for usage instructions
    """
    p = ArgumentParser(
        description="Simulate CSV gzip batches arriving from external sources."
    )
    p.add_argument("--topic", required=True, help="Kafka topic to send messages to")
    p.add_argument(
        "--frequency",
        type=float,
        default=DEFAULT_FREQUENCY,
        help=f"Target rows/second emitted per thread (default: {DEFAULT_FREQUENCY})",
    )
    p.add_argument(
        "--num-threads",
        type=int,
        default=DEFAULT_NUM_THREADS,
        help=f"Number of worker threads (default: {DEFAULT_NUM_THREADS})",
    )
    p.add_argument(
        "--key-column-id",
        type=int,
        default=DEFAULT_KEY_COLUMN_ID,
        help=f"Column ID to use as Kafka message key (default: {DEFAULT_KEY_COLUMN_ID})",
    )
    p.add_argument(
        "--max-batches",
        type=int,
        default=DEFAULT_MAX_BATCHES,
        help=f"Number of batches to go through the CSV (default: {DEFAULT_MAX_BATCHES}). If set = 0, goes until the end.",
    )

    a = p.parse_args()
    return Config(
        topic=a.topic,
        frequency=a.frequency,
        num_threads=a.num_threads,
        key_column_id=a.key_column_id,
        max_batches=a.max_batches,
    )


def next_batch(frequency: float) -> tuple[int, float]:
    """
    Generate a (batch_size, time_to_wait) batch parameters that mimics a real input data stream.

    The inter-batch wait time time_to_wait is drawn from a log-normal distribution
    (heavy-tailed, always positive), and batch_size is derived from it so that the
    long-run average throughput converges to F rows/second.

    Strategy
    --------
    Draw time_to_wait ~ LogNormal(mu_log, sigma_log) where mu_log is chosen so that
    E[time_to_wait] = 1/F (i.e. one "nominal" batch per second on average, each
    carrying ~1 row — then we scale).

    More precisely, we target E[time_to_wait] = T_mean = 1.0 s (a fixed nominal
    window), and set batch_size = round(F * time_to_wait), so that:

        E[batch_size / time_to_wait] = E[F] = F  ✓  (exactly, by construction)

    Parameters
    ----------
    frequency: float  — target throughput in rows/second
    SIGMA_LOG: float  — log-normal shape parameter (volatility knob)

    Returns
    -------
    batch_size : int    — number of rows to read in this batch
    time_to_wait : float  — seconds to wait *after* sending this batch
    """
    # E[time_to_wait] = 1 s, variance controlled by sigma_log
    # For LogNormal: E[X] = exp(mu + sigma²/2)
    # => mu = -sigma²/2  so that E[time_to_wait] = 1
    mu_log = -(SIGMA_LOG**2) / 2
    time_to_wait = lognormal(mean=mu_log, sigma=SIGMA_LOG)

    # batch_size is proportional to time_to_wait so the ratio batch_size/time_to_wait = F is maintained on avg.
    # Adding a small Poisson scatter on top makes counts integer and noisy.
    expected_rows = frequency * time_to_wait
    batch_size = max(1, poisson(lam=max(expected_rows, 1e-6)))

    return batch_size, time_to_wait


def send_to_kafka(producer: Producer, topic: str, key: str, rows: list):
    """Gzip-compress rows and send them to Kafka keyed by UUID."""
    buf = StringIO()
    writer = get_csv_writer(buf, delimiter=CSV_DELMITER)
    writer.writerows(rows)
    payload = gzip_compress(buf.getvalue().encode())
    producer.produce(topic, key=key, value=payload)


def worker(config: Config, reader: Iterator, producer: Producer, reader_lock: Lock):
    """
    - Worker thread function that reads big batches of rows from the shared CSV reader, writes them to gzip files, and sleeps between batches.
    - Each (potentially sub-) batch (from the big current batch) *SENT* has the same search_id because :
    > -> the worker will cut the current big batch it reads from the CSV into several gzipped sub-batches (if needed).
    - Each worker thread simulates an independent data stream with the same frequency, reading from the same CSV file sequentially by acquiring
    the lock on the CSV reader.
    > -> Overall script's frequency : num_threads * frequency
    """
    loop_ctn = 0

    while True:

        batch_size, time_to_wait = next_batch(config.frequency)

        with reader_lock:

            if (first_row := next(reader, None)) is None:
                break

            batch_id = first_row[config.key_column_id]
            batch_rows = [first_row]

            for _ in range(batch_size - 1):
                if (next_row := next(reader, None)) is None:
                    break

                if (next_row_id := next_row[config.key_column_id]) != batch_id:
                    # flush current sub-batch, start a new one
                    send_to_kafka(producer, config.topic, batch_id, batch_rows)
                    batch_id = next_row_id
                    batch_rows = [next_row]
                else:
                    batch_rows.append(next_row)

            # flush the only or last sub-batch
            send_to_kafka(producer, config.topic, batch_id, batch_rows)
        
        producer.poll(0)  # trigger delivery callbacks without blocking
        
        print(f"[{current_thread().name}] SENT {batch_size}r | NOW SLEEP {time_to_wait:.3f}s")

        loop_ctn += 1

        if config.max_batches > 0 and loop_ctn >= config.max_batches:
            break

        sleep(time_to_wait)


def run():
    """
    Main function to set up configuration, reader and producer objects, common locks, and threads for the CSV simulator.
    """
    config: Config = parse_args()
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # don't bother putting them as params i just don't know what the fuck it is
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_callback,
        "on_delivery": (lambda err, msg: print(f"[ERROR] {err} \n {msg}") if err else None)
    })

    reader_lock = Lock()

    with open(CSV_INPUT_PATH, newline="") as fh:

        reader = get_csv_reader(fh, delimiter="^")

        threads = [
            Thread(
                target=worker,
                args=(config, reader, producer, reader_lock),
                name=str(i),
            )
            for i in range(config.num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    producer.flush()  # wait for all in-flight messages to be delivered


if __name__ == "__main__":
    run()
