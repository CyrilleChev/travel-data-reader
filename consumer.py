#!/usr/bin/env python3
"""
consumer.py — Travel data consumer

Two modes:
    1. Kafka mode   : reads from a Kafka topic (gzip-compressed CSV messages)
    2. Local mode   : reads directly from a local gzip CSV file

Usage:
    # Kafka mode
    python consumer.py --mode kafka --broker localhost:9092 --topic travel-recos --rates-file etc/eurofxref.csv

    # Local file mode
    python consumer.py --mode local --input-file travel_data_example.csv.gz --rates-file etc/eurofxref.csv
"""

import argparse
import gzip
import threading
import datetime
import time
import uuid
import psycopg2
import boto3
from collections import defaultdict
from os import environ
import os
from recoReader import decode_line, group_and_decorate, load_rates
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

S3_BUCKET = environ.get("S3_RAW_BUCKET", "travel-recos-raw")
S3_PREFIX = environ.get("S3_RAW_PREFIX", "raw")

s3_client = boto3.client("s3", region_name=environ.get("REGION", "eu-west-3"))

def save_raw_to_s3(raw_bytes, partition, offset):
    """Save the raw gzip message to S3 before any processing."""
    now = datetime.datetime.now(datetime.timezone.utc)
    key = (
        f"{S3_PREFIX}/"
        f"{now.strftime('%Y/%m/%d/%H')}/"
        f"p{partition}-o{offset}-{uuid.uuid4().hex[:8]}.csv.gz"
    )
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=raw_bytes)
    return key

REGION = environ.get("REGION", "eu-west-3")

def oauth_callback(config):
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, expiry_ms / 1000


# ── Config ────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     "amadeusdb.cluster-c7ms6magi7wm.eu-west-3.rds.amazonaws.com",
    "port":     5432,
    "dbname":   "amadeus",
    "user":     "postgres",
    "password": "amadeusdb",
    "sslmode":  "require",
}

QUOTA = 1000  # max strict de searches retenues par OnD par jour


# ── Sampling ──────────────────────────────────────────────────────────────────

counters = defaultdict(int)

def should_accept(ond, date):
    key = (ond, date)
    if counters[key] < QUOTA:
        counters[key] += 1
        return True
    return False

def reset_at_midnight():
    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        midnight = (now + datetime.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        time.sleep((midnight - now).total_seconds())
        counters.clear()
        print("[SAMPLING] Compteurs remis à zéro (minuit UTC)")


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_search(cursor, conn, search):
    for reco in search.get("recos", []):
        cursor.execute("""
            INSERT INTO recos (
                search_id, search_date, search_country,
                ond, trip_type, advance_purchase,
                stay_duration, airline, cabin,
                price_eur, nb_connections
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            search["search_id"],
            search["search_date"],
            search["search_country"],
            search["OnD"],
            search["trip_type"],
            search["advance_purchase"],
            search["stay_duration"],
            reco.get("main_marketing_airline", ""),
            reco.get("main_cabin", ""),
            reco.get("price_EUR", 0.0),
            max(0, len(reco.get("flights", [])) - 1)
        ))
    conn.commit()


# ── Processing ────────────────────────────────────────────────────────────────

def process_search(search, cursor, conn, stats):
    stats["seen"] += 1
    ond = search.get("OnD", "")
    date = search.get("search_date", "")

    if should_accept(ond, date):
        insert_search(cursor, conn, search)
        stats["inserted_searches"] += 1
        stats["inserted_recos"] += len(search.get("recos", []))

    if stats["seen"] % 100 == 0:
        print(
            f"[STATS] searches seen={stats['seen']} | "
            f"searches inserted={stats['inserted_searches']} | "
            f"recos inserted={stats['inserted_recos']} | "
            f"ratio={stats['inserted_searches'] / stats['seen']:.1%}"
        )


# ── Mode LOCAL ────────────────────────────────────────────────────────────────

def run_local(args, rates, cursor, conn, stats):
    print(f"[LOCAL] Lecture du fichier : {args.input_file}")
    recos_buffer = []
    current_search_id = None

    with gzip.open(args.input_file, 'r') as f:
        for raw_line in f:
            reco = decode_line(raw_line)
            if reco is None:
                continue

            sid = reco["search_id"]

            if sid != current_search_id:
                if recos_buffer:
                    search = group_and_decorate(recos_buffer, rates)
                    if search:
                        process_search(search, cursor, conn, stats)

                recos_buffer = []
                current_search_id = sid

            recos_buffer.append(reco)

    if recos_buffer:
        search = group_and_decorate(recos_buffer, rates)
        if search:
            process_search(search, cursor, conn, stats)

    print(
        f"[LOCAL] Terminé. seen={stats['seen']} | "
        f"searches inserted={stats['inserted_searches']} | "
        f"recos inserted={stats['inserted_recos']}"
    )


# ── Mode KAFKA ────────────────────────────────────────────────────────────────

def run_kafka(args, rates, cursor, conn, stats):
    from confluent_kafka import Consumer

    print(f"[KAFKA] Connexion à {args.broker} | topic: {args.topic}")

    consumer = Consumer({
        "bootstrap.servers":    args.broker,
        "group.id":             "amadeus-consumer-group",
        "auto.offset.reset":    "earliest",
        "enable.partition.eof": False,
        "security.protocol":    "SASL_SSL",
        "sasl.mechanism":       "OAUTHBEARER",
        "oauth_cb":             oauth_callback,
    })
    consumer.subscribe([args.topic])

    print("[KAFKA] En attente de messages... (Ctrl+C pour arrêter)")

    try:
        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                print("[KAFKA] Pas de message depuis 5s, toujours en attente...")
                continue

            if msg.error():
                print(f"[KAFKA ERROR] {msg.error()}")
                continue

            print(
                f"[KAFKA] Message reçu — "
                f"partition={msg.partition()} "
                f"offset={msg.offset()} "
                f"size={len(msg.value())}B"
            )

            # Save raw to S3 before any processing
            try:
                s3_key = save_raw_to_s3(msg.value(), msg.partition(), msg.offset())
                print(f"[S3] Saved raw → s3://{S3_BUCKET}/{s3_key}")
            except Exception as e:
                print(f"[S3 ERROR] Failed to save raw: {e}")

            try:
                raw_csv = gzip.decompress(msg.value()).decode("utf-8")
            except Exception as e:
                print(f"[KAFKA] Erreur décompression : {e}")
                continue

            recos_batch = []
            for line in raw_csv.splitlines():
                reco = decode_line(line)
                if reco:
                    recos_batch.append(reco)

            if not recos_batch:
                continue

            search = group_and_decorate(recos_batch, rates)
            if search:
                process_search(search, cursor, conn, stats)

    except KeyboardInterrupt:
        print(
            f"\n[KAFKA] Arrêt manuel. "
            f"seen={stats['seen']} | "
            f"searches inserted={stats['inserted_searches']} | "
            f"recos inserted={stats['inserted_recos']}"
        )
    finally:
        consumer.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Consumer: lit depuis Kafka ou un fichier local, insère dans PostgreSQL"
    )
    parser.add_argument("--mode", required=True, choices=["kafka", "local"])
    parser.add_argument("--broker", default=os.environ["BOOTSTRAP_SERVER"])
    parser.add_argument("--topic", default=os.environ["TOPIC"])
    parser.add_argument("--input-file", default="travel_data_example.csv.gz")
    parser.add_argument("--rates-file", default=os.environ["RATES_FILE"])
    return parser.parse_args()


def main():
    args = parse_args()

    rates = load_rates(args.rates_file)
    conn = get_db_connection()
    cursor = conn.cursor()

    threading.Thread(target=reset_at_midnight, daemon=True).start()

    stats = {"seen": 0, "inserted_searches": 0, "inserted_recos": 0}

    print(f"[START] Mode : {args.mode.upper()}")

    if args.mode == "local":
        run_local(args, rates, cursor, conn, stats)
    else:
        run_kafka(args, rates, cursor, conn, stats)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()