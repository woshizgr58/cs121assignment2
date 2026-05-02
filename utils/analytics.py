import os
import re
import sqlite3
from collections import Counter
from threading import RLock
from urllib.parse import urldefrag, urlparse


ANALYTICS_DB = "analytics.sqlite3"

STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am",
    "an", "and", "any", "are", "aren", "as", "at", "be", "because",
    "been", "before", "being", "below", "between", "both", "but", "by",
    "can", "couldn", "did", "didn", "do", "does", "doesn", "doing",
    "don", "down", "during", "each", "few", "for", "from", "further",
    "had", "hadn", "has", "hasn", "have", "haven", "having", "he",
    "her", "here", "hers", "herself", "him", "himself", "his", "how",
    "i", "if", "in", "into", "is", "isn", "it", "its", "itself",
    "just", "ll", "m", "ma", "me", "mightn", "more", "most", "mustn",
    "my", "myself", "needn", "no", "nor", "not", "now", "o", "of",
    "off", "on", "once", "only", "or", "other", "our", "ours",
    "ourselves", "out", "over", "own", "re", "s", "same", "shan",
    "she", "should", "shouldn", "so", "some", "such", "t", "than",
    "that", "the", "their", "theirs", "them", "themselves", "then",
    "there", "these", "they", "this", "those", "through", "to", "too",
    "under", "until", "up", "ve", "very", "was", "wasn", "we", "were",
    "weren", "what", "when", "where", "which", "while", "who", "whom",
    "why", "will", "with", "won", "wouldn", "y", "you", "your",
    "yours", "yourself", "yourselves",
}

WORD_RE = re.compile(r"[a-zA-Z]+(?:'[a-zA-Z]+)?")
_LOCK = RLock()
_INITIALIZED = False


def _connect():
    return sqlite3.connect(ANALYTICS_DB, timeout=30)


def reset_analytics():
    global _INITIALIZED
    with _LOCK:
        _INITIALIZED = False
        for path in (ANALYTICS_DB, f"{ANALYTICS_DB}-shm", f"{ANALYTICS_DB}-wal"):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass


def _ensure_schema(conn):
    global _INITIALIZED
    if _INITIALIZED:
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pages (
            url TEXT PRIMARY KEY,
            subdomain TEXT NOT NULL,
            word_count INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS words (
            word TEXT PRIMARY KEY,
            count INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    _INITIALIZED = True


def _canonical_url(url):
    return urldefrag(url)[0]


def _is_countable_html(soup, raw_content=None):
    if soup.find("html") is None or soup.find("body") is None:
        return False

    if raw_content:
        # Some cached pages claim text/html but are Word/Office exports or
        # UTF-16-ish blobs. BeautifulSoup can recover text from them, but they
        # are not useful HTML pages for assignment statistics.
        nul_ratio = raw_content.count(b"\x00") / len(raw_content)
        if nul_ratio > 0.05:
            return False

        head = raw_content[:4096].lower()
        office_markers = (
            b"urn:schemas-microsoft-com:office",
            b"mso-",
            b"generator\" content=\"microsoft",
        )
        if any(marker in head for marker in office_markers):
            return False

    return True


def _visible_text(soup):
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)


def _page_words(text):
    return [match.group(0).lower() for match in WORD_RE.finditer(text)]


def _report_words(words):
    return Counter(
        word for word in words
        if len(word) > 1 and word not in STOP_WORDS
    )


def record_page(url, soup, raw_content=None):
    if not _is_countable_html(soup, raw_content):
        return

    canonical_url = _canonical_url(url)
    subdomain = (urlparse(canonical_url).hostname or "").lower()
    words = _page_words(_visible_text(soup))
    word_count = len(words)
    report_word_counts = _report_words(words)

    with _LOCK:
        with _connect() as conn:
            _ensure_schema(conn)
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO pages (url, subdomain, word_count)
                VALUES (?, ?, ?)
                """,
                (canonical_url, subdomain, word_count),
            )
            if cursor.rowcount == 0:
                return

            for word, count in report_word_counts.items():
                conn.execute(
                    """
                    INSERT INTO words (word, count)
                    VALUES (?, ?)
                    ON CONFLICT(word) DO UPDATE
                    SET count = count + excluded.count
                    """,
                    (word, count),
                )
            conn.commit()
