"""Simple DHT scraper."""
import logging
import os
import threading
from logging.handlers import TimedRotatingFileHandler
from queue import Queue
from random import randint
from secrets import token_hex
from signal import SIGINT, SIGTERM, signal
from typing import List

from dht_node import DHTNode
from dht_node.data_structures import Counter
from dht_node.utils import log_stats
from diskcache import Cache

from src import handlers, utils

if __name__ == "__main__":
    cache = Cache("cache", eviction_policy="none", size_limit=5 * 10 ** 10)
    counters = {"all": Counter(), "saved": Counter()}
    found_torrents: Queue = Queue(maxsize=10 ** 6)
    started_nodes: List[DHTNode] = []

    # Generate folders, if necessary
    for folder in ["logs", "results"]:
        if not os.path.exists(folder):
            os.makedirs(folder)

    # Generate random node details, if necessary
    if not os.path.exists("nodes.csv") or os.path.getsize("nodes.csv") == 0:
        with open("nodes.csv", "w", encoding="utf8") as source_file:
            source_file.write(f"{token_hex(20)},{randint(1025, 65535)}\n")

    # Configure logging
    log_f = os.path.join("logs/log.txt")
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[TimedRotatingFileHandler(log_f, utc=True, when="midnight")],
        level=logging.INFO,
    )

    # Handle close signal gracefully
    stop = threading.Event()
    signal(SIGINT, lambda *args: utils.signal_handler(started_nodes, stop))
    signal(SIGTERM, lambda *args: utils.signal_handler(started_nodes, stop))

    # Add existing info hashes to the cache
    utils.update_cache(cache, stop)

    # Start result queue handler
    threading.Thread(
        target=handlers.process_found_torrents,
        args=(cache, counters, found_torrents, stop),
    ).start()

    # Load list of nodes from the source file
    with open("nodes.csv", "r", encoding="utf8") as source_file:
        # Initialize and start them
        if not stop.is_set():
            for row in source_file:
                if not row.strip():
                    continue
                node_id = row.split(",")[0].strip()
                node_port = int(row.split(",")[1].strip())
                new_node = DHTNode(node_id, node_port)
                new_node.add_message_handler(
                    lambda m, n: utils.on_dht_message(found_torrents, m, n),
                )
                new_node.start()
                started_nodes.append(new_node)

    while not stop.is_set():
        # Log the progress
        logging.info("%s threads", threading.active_count())
        log_stats(*started_nodes)
        logging.info(
            "Processed info hashes: %s all, %s saved",
            counters["all"].value,
            counters["saved"].value,
        )
        logging.info("Queue length: %s info hashes\n", found_torrents.qsize())

        # Reset counters
        for counter in counters.values():
            counter.reset()

        # Wait until the next check
        stop.wait(60)

    logging.info("Exiting!\n")
