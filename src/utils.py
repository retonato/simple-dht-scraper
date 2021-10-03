"""Util functions used by DHT scraper"""
import glob
import logging
import random
from dataclasses import dataclass
from datetime import date


@dataclass
class Torrent:
    """Data container for torrents, found in the incoming messages"""

    date: date
    info_hash: str  # hex
    node_ip: str


def on_dht_message(found_torrents, message, node):
    """Called on each incoming message, adds found torrents to the queue"""
    if message.get(b"y") == b"q":
        if message.get(b"q") in [b"announce_peer", b"get_peers"]:
            if message.get(b"a", {}).get(b"info_hash"):
                found_torrents.put(
                    Torrent(
                        date=date.today(),
                        info_hash=message[b"a"][b"info_hash"].hex(),
                        node_ip=node.ip,
                    )
                )


def signal_handler(started_nodes, stop):
    """Called when the stop signal is received, stops all nodes gracefully"""
    for node in started_nodes:
        node.stop()
    stop.set()


def update_cache(cache, stop):
    """Add all info hashes found in results folder to the cache"""
    for filepath in sorted(glob.glob("results/**/*.txt", recursive=True)):
        if stop.is_set():
            break
        logging.info("Processing %s", filepath)
        with open(filepath, "r", encoding="utf8") as result_file:
            info_hashes = []
            for line in result_file:
                if len(line.strip()) == 40:
                    info_hashes.append(line.strip())

        random.shuffle(info_hashes)
        counter_l = 0
        counter_s = 0

        for info_hash in info_hashes:
            if cache.get(info_hash) != "p":
                cache.set(info_hash, "p", expire=None)
                counter_l += 1
            else:
                counter_s += 1
            if counter_l == 0 and counter_s >= 1000:
                logging.info("All info hashes are already loaded")
                break
            if counter_l > 1 and counter_l % 100000 == 0:
                logging.info("Loaded %s, skipped %s", counter_l, counter_s)
                if stop.is_set():
                    break

        logging.info("Loaded %s, skipped %s\n", counter_l, counter_s)
