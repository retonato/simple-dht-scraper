A simple Bittorrent DHT scraper.

Usage:
- install Python 3.7 (or newer)
- pip install -r requirements.txt
- python -m src.dht_scraper

It starts multiple DHT nodes and saves info hashes from incoming announce_peer
and get_peers messages. To reduce the number of fake info hashes scraper saves
only those which arrived from 2 different DHT nodes within the last month.

A list of DHT nodes to start can be provided in nodes.csv file (id, port).
If it is absent - a new list with a random node id/port will be created.
Don't forget to forward/open those ports in your router/firewall.

Found info hashes are saved to "results" folder, one file per month.
All info hashes, which are stored in that folder, are unique.

Logs are saved to "logs" folder, one file per day.
