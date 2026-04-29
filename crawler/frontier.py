import os
import shelve
import time

from threading import Condition, RLock
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.lock = RLock()
        self.url_available = Condition(self.lock)
        self.active_urls = 0
        self.next_domain_fetch = dict()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _get_domain(self, url):
        return (urlparse(url).hostname or "").lower()

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        with self.url_available:
            while True:
                if not self.to_be_downloaded:
                    if self.active_urls == 0:
                        return None
                    self.url_available.wait()
                    continue

                now = time.monotonic()
                selected_index = None
                next_ready_time = None

                for index, url in enumerate(self.to_be_downloaded):
                    domain = self._get_domain(url)
                    ready_time = self.next_domain_fetch.get(domain, 0)
                    if ready_time <= now:
                        selected_index = index
                        break
                    if next_ready_time is None or ready_time < next_ready_time:
                        next_ready_time = ready_time

                if selected_index is not None:
                    url = self.to_be_downloaded.pop(selected_index)
                    domain = self._get_domain(url)
                    self.next_domain_fetch[domain] = (
                        time.monotonic() + self.config.time_delay)
                    self.active_urls += 1
                    return url

                wait_time = max(0, next_ready_time - now)
                self.url_available.wait(timeout=wait_time)

    def add_url(self, url):
        with self.url_available:
            url = normalize(url)
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.append(url)
                self.url_available.notify_all()
    
    def mark_url_complete(self, url):
        with self.url_available:
            urlhash = get_urlhash(url)
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()
            self.active_urls -= 1
            self.url_available.notify_all()
