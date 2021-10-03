"""Incoming message handler"""
from datetime import date


def process_found_torrents(cache, counters, found_torrents, stop):
    """
    Process found torrents, save new to the result file
    Cache structure - {info_hash: node_ip / "p"}
    """
    while not stop.is_set():
        if found_torrents.qsize() < 1000:
            stop.wait(5)
            continue

        dst = str(date.today())[:7]
        with open(f"results/{dst}.txt", "a", encoding="utf8") as result_file:
            for _ in range(1000):
                tor = found_torrents.get_nowait()
                counters["all"].increment()

                # Skip fake info hashes
                if (
                    tor.info_hash.endswith("000000")
                    or len(tor.info_hash) != 40
                ):
                    continue

                # Check for info hashes in cache
                cache_value = cache.get(tor.info_hash)
                if not cache_value:
                    cache.set(
                        tor.info_hash, tor.node_ip, expire=3600 * 24 * 30
                    )
                    continue
                if cache_value in ["p", tor.node_ip]:
                    continue

                # Save info hash to the result file
                result_file.write(tor.info_hash + "\n")
                cache.set(tor.info_hash, "p", expire=None)
                counters["saved"].increment()
