import os
import sqlite3

from utils.analytics import ANALYTICS_DB


def print_report():
    if not os.path.exists(ANALYTICS_DB):
        raise SystemExit(
            f"Could not find {ANALYTICS_DB}. Run the crawler first."
        )

    with sqlite3.connect(ANALYTICS_DB) as conn:
        total_pages = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        longest_page = conn.execute(
            """
            SELECT url, word_count
            FROM pages
            ORDER BY word_count DESC, url ASC
            LIMIT 1
            """
        ).fetchone()
        top_words = conn.execute(
            """
            SELECT word, count
            FROM words
            ORDER BY count DESC, word ASC
            LIMIT 50
            """
        ).fetchall()
        subdomains = conn.execute(
            """
            SELECT subdomain, COUNT(*)
            FROM pages
            WHERE subdomain = 'ics.uci.edu'
               OR subdomain LIKE '%.ics.uci.edu'
            GROUP BY subdomain
            ORDER BY subdomain ASC
            """
        ).fetchall()

    print(f"Unique pages: {total_pages}")
    if longest_page:
        print(f"Longest page: {longest_page[0]}, {longest_page[1]} words")
    else:
        print("Longest page: none")

    print("\nTop 50 words:")
    for word, count in top_words:
        print(f"{word}, {count}")

    print(f"\nSubdomain count: {len(subdomains)}")
    print("\nSubdomains:")
    for subdomain, count in subdomains:
        print(f"{subdomain}, {count}")


if __name__ == "__main__":
    print_report()
