"""Create the Kafka topics declared in config/kafka/topics.yaml (idempotent; existing topics are left alone)."""

from __future__ import annotations

import os
import time

import yaml
from confluent_kafka.admin import AdminClient, NewTopic

from harbor_haze.common import ROOT, log, setup_logging


def main() -> None:
    setup_logging()
    spec = yaml.safe_load(open(ROOT / "config" / "kafka" / "topics.yaml"))["topics"]
    admin = AdminClient({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")})
    for _ in range(30):
        try:
            existing = set(admin.list_topics(timeout=5).topics)
            break
        except Exception as e:  # broker still starting
            log.info("waiting for Kafka (%s)", e.__class__.__name__)
            time.sleep(2)
    else:
        raise SystemExit("Kafka is not reachable")
    new = [
        NewTopic(t["name"], num_partitions=t["partitions"], replication_factor=1, config=t.get("config", {}))
        for t in spec
        if t["name"] not in existing
    ]
    for name, fut in admin.create_topics(new).items() if new else []:
        fut.result()
        log.info("created topic %s", name)
    log.info("topics ready: %s", ", ".join(t["name"] for t in spec))


if __name__ == "__main__":
    main()
