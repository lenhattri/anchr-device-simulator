#!/usr/bin/env python3
"""
ANCHR End-to-End Reconciliation Script.

Compares the sequence log written by the simulator with messages
received by kafka-ingestor (via Kafka topic consumption) to detect
data loss at the individual message level.

Usage:
    python reconcile.py --seq-log /tmp/anchr-sim-seq.jsonl \
                        --kafka-brokers kafka-broker-0:9092 \
                        [--topics anchr.mqtt.telemetry.raw.v1,anchr.mqtt.tx.raw.v1]

Requires: confluent-kafka  (pip install confluent-kafka)
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

try:
    from confluent_kafka import Consumer, KafkaException, TopicPartition
except ImportError:
    Consumer = None


def load_seq_log(path):
    """Load simulator sequence log and return {device_id -> set(seq)}."""
    sent = defaultdict(set)
    total = 0
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            sent[record["device_id"]].add(record["seq"])
            total += 1
    return sent, total


def consume_kafka_messages(brokers, topics, timeout_s=30):
    """Consume all available messages from specified Kafka topics.

    Returns {device_id -> set(seq)} extracted from message envelopes.
    """
    if Consumer is None:
        print("ERROR: confluent-kafka not installed. Install with: pip install confluent-kafka")
        sys.exit(1)

    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": f"anchr-reconcile-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    received = defaultdict(set)
    total = 0

    try:
        consumer.subscribe(topics)
        empty_polls = 0
        max_empty = 3  # Stop after 3 consecutive empty polls

        while empty_polls < max_empty:
            msg = consumer.poll(timeout=timeout_s / max_empty)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                print(f"  Kafka error: {msg.error()}")
                empty_polls += 1
                continue

            empty_polls = 0
            try:
                envelope = json.loads(msg.value().decode("utf-8"))
                device_id = envelope.get("device_id", "")
                seq = envelope.get("seq")
                if device_id and seq is not None:
                    received[device_id].add(seq)
                    total += 1
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass  # Skip malformed messages

    finally:
        consumer.close()

    return received, total


def reconcile(sent, received):
    """Compare sent vs received and produce a report."""
    all_devices = set(sent.keys()) | set(received.keys())

    total_sent = sum(len(seqs) for seqs in sent.values())
    total_received = sum(len(seqs) for seqs in received.values())

    missing = {}  # device_id -> list of missing seqs
    extra = {}    # device_id -> list of extra seqs (received but not sent)

    for device_id in all_devices:
        s = sent.get(device_id, set())
        r = received.get(device_id, set())

        missing_seqs = sorted(s - r)
        extra_seqs = sorted(r - s)

        if missing_seqs:
            missing[device_id] = missing_seqs
        if extra_seqs:
            extra[device_id] = extra_seqs

    total_missing = sum(len(v) for v in missing.values())
    total_extra = sum(len(v) for v in extra.values())

    return {
        "total_sent": total_sent,
        "total_received": total_received,
        "total_missing": total_missing,
        "total_extra": total_extra,
        "devices_with_loss": len(missing),
        "missing_details": missing,
        "extra_details": extra,
        "zero_data_loss": total_missing == 0,
    }


def print_report(report):
    """Print reconciliation report."""
    print("\n" + "=" * 60)
    print("ANCHR End-to-End Reconciliation Report")
    print("=" * 60)
    print(f"  Total Sent:       {report['total_sent']:>10,}")
    print(f"  Total Received:   {report['total_received']:>10,}")
    print(f"  Missing:          {report['total_missing']:>10,}")
    print(f"  Extra/Duplicate:  {report['total_extra']:>10,}")
    print(f"  Devices w/ Loss:  {report['devices_with_loss']:>10}")
    print("-" * 60)

    if report["zero_data_loss"]:
        print("  Result:           ✅ ZERO DATA LOSS")
    else:
        print("  Result:           ❌ DATA LOSS DETECTED")
        print()
        # Show up to 10 devices with loss
        for i, (device_id, seqs) in enumerate(sorted(report["missing_details"].items())):
            if i >= 10:
                remaining = len(report["missing_details"]) - 10
                print(f"  ... and {remaining} more devices")
                break
            preview = seqs[:5]
            suffix = f" ... +{len(seqs) - 5} more" if len(seqs) > 5 else ""
            print(f"  {device_id}: missing seq {preview}{suffix}")

    print("=" * 60)
    return 0 if report["zero_data_loss"] else 1


def main():
    parser = argparse.ArgumentParser(description="ANCHR End-to-End Reconciliation")
    parser.add_argument("--seq-log", required=True, help="Path to simulator sequence log (JSONL)")
    parser.add_argument("--kafka-brokers", required=True, help="Kafka bootstrap servers")
    parser.add_argument(
        "--topics",
        default="anchr.mqtt.telemetry.raw.v1,anchr.mqtt.tx.raw.v1,anchr.mqtt.ack.raw.v1",
        help="Comma-separated Kafka topics to consume",
    )
    parser.add_argument("--timeout", type=int, default=30, help="Kafka consume timeout (seconds)")
    parser.add_argument("--json", action="store_true", help="Output as JSON instead of table")
    args = parser.parse_args()

    # Load simulator log
    print(f"Loading seq log: {args.seq_log}")
    sent, sent_count = load_seq_log(args.seq_log)
    print(f"  Loaded {sent_count:,} records from {len(sent)} devices")

    # Consume from Kafka
    topics = [t.strip() for t in args.topics.split(",")]
    print(f"Consuming from Kafka: {args.kafka_brokers}")
    print(f"  Topics: {topics}")
    received, recv_count = consume_kafka_messages(args.kafka_brokers, topics, args.timeout)
    print(f"  Consumed {recv_count:,} messages from {len(received)} devices")

    # Reconcile
    report = reconcile(sent, received)

    if args.json:
        # Convert sets to lists for JSON serialization
        output = {k: v for k, v in report.items() if k not in ("missing_details", "extra_details")}
        print(json.dumps(output, indent=2))
        sys.exit(0 if report["zero_data_loss"] else 1)
    else:
        sys.exit(print_report(report))


if __name__ == "__main__":
    main()
