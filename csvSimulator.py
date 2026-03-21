"""
csvSimulator.py — Data ingestion simulator

Reads a CSV sequentially and emits gzip-compressed CSV batches
at approximately the requested frequency.

Usage:
    python csvSimulator.py --input data.csv
    python csvSimulator.py --input data.csv --frequency 200 --output-dir ./out --num-threads 3
"""

from argparse import ArgumentParser
# Same function names so we import both whole modules
import csv
import gzip
from io import StringIO
from time import sleep
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from numpy.random import lognormal, poisson
from threading import Thread, Lock, current_thread

SIGMA_LOG = 1.2  # log-normal shape parameter (burstiness knob)
DEFAULT_FREQUENCY = 500  # target rows/second per thread
DEFAULT_OUTPUT_DIR = "./batches"
DEFAULT_NUM_THREADS = 5 # NUmber of concurrent threads to run (each simulating an independent data stream with the same frequency)


@dataclass
class Config:
    """Configuration for the CSV simulator, parsed from command-line arguments."""
    input:        Path
    frequency:    float
    output_dir:   Path
    num_threads:  int
    started_at:   str


def parse_args() -> Config:
    """
    Parses the arguments and sets the staretd_at timestamp for output file naming.
    See csvSimulator.py --help for usage instructions.
    """
    p = ArgumentParser(description="Simulate CSV gzip batches arriving from external sources.")
    p.add_argument("--input",      required=True,           help="Source CSV file path")
    p.add_argument("--frequency",  type=float, default=DEFAULT_FREQUENCY, help=f"Target rows/second emitted per thread (default: {DEFAULT_FREQUENCY})")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,     help=f"Directory for gzip output files (default: {DEFAULT_OUTPUT_DIR})")
    p.add_argument("--num-threads", type=int, default=DEFAULT_NUM_THREADS, help=f"Number of worker threads (default: {DEFAULT_NUM_THREADS})")

    a = p.parse_args()
    return Config(
        input=Path(a.input),
        frequency=a.frequency,
        output_dir=Path(a.output_dir),
        num_threads=a.num_threads,
        started_at=datetime.now().strftime("%Y%m%dT%H%M%S")
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
    mu_log = -(SIGMA_LOG ** 2) / 2
    time_to_wait = lognormal(mean=mu_log, sigma=SIGMA_LOG)

    # batch_size is proportional to time_to_wait so the ratio batch_size/time_to_wait = F is maintained on avg.
    # Adding a small Poisson scatter on top makes counts integer and noisy.
    expected_rows = frequency * time_to_wait
    batch_size = max(1, poisson(lam=max(expected_rows, 1e-6)))

    return batch_size, time_to_wait

def worker(config: Config, reader, reader_lock: Lock, batch_lock: Lock, batch_idx: list[int]):
    """
    Worker thread function that reads batches of rows from the shared CSV reader, writes them to gzip files, and sleeps between batches.
    Each worker thread simulates an independent data stream with the same frequency, reading from the same CSV file sequentially by acquiring
    the lock on the CSV reader.
    > -> Overall script's frequency : num_threads * frequency
    """
    while True:
        rows = []
        batch_size, time_to_wait = next_batch(config.frequency)

        with reader_lock:
            for _ in range(batch_size):
                if (row := next(reader, None)) is None:
                    break
                rows.append(row)

        if not rows:
            break

        buf = StringIO()
        writer = csv.writer(buf, delimiter="^")
        writer.writerows(rows)
        csv_bytes = buf.getvalue().encode()

        with batch_lock:
            idx = batch_idx[0]
            batch_idx[0] += 1

        out_path = config.output_dir / f"batch_{idx}_{config.started_at}.csv.gz"
        with gzip.open(out_path, "wb") as gz:
            gz.write(csv_bytes)

        print(f"[thread-{current_thread().name}] batch {idx} — {batch_size} rows, sleeping {time_to_wait:.2f}s")
        sleep(time_to_wait)

def run():
    """
    Main function to set up configuration, common locks, and threads for the CSV simulator.
    """
    config: Config = parse_args()
    config.output_dir.mkdir(parents=True, exist_ok=True)

    reader_lock = Lock()
    batch_lock  = Lock()
    batch_idx   = [0]  # mutable int shared across threads

    with open(config.input, newline="") as fh:

        reader = csv.reader(fh, delimiter="^")

        threads = [Thread(target=worker, args=(config, reader, reader_lock, batch_lock, batch_idx), name=str(i)) for i in range(config.num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

if __name__ == "__main__":
    run()