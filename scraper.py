import re
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urlparse
from urllib.parse import urljoin, urldefrag
from utils.analytics import record_page

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    
    # Base case
    if not resp or resp.error or resp.raw_response is None or resp.status != 200:
        return []

    content_type = resp.raw_response.headers.get("content-type", "").lower()
    if content_type and not (
        "text/html" in content_type or "application/xhtml+xml" in content_type
    ):
        return []
    
    # Parse
    links = []
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    base_url = resp.url if getattr(resp, "url", None) else url
    if is_valid(base_url):
        try:
            record_page(base_url, soup, resp.raw_response.content)
        except Exception as exc:
            print(f"Analytics error for {base_url}: {exc}", flush=True)

    for a in soup.find_all("a", href = True):
        href = a.get("href")
        if not href: 
            continue
        if href.startswith(("javascript:", "mailto:", "tel:")): # skip useless
            continue
    
        full_url = urljoin(base_url, href) # get full url (base + /.....)
        
        full_url, _ = urldefrag(full_url) # clean url
        
        links.append(full_url)

    
    return links

def is_valid(url):
    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        hostname = parsed.hostname or ""
        if hostname != "ics.uci.edu" and not hostname.endswith(".ics.uci.edu"):
            return False
        if hostname in {"gitlab.ics.uci.edu", "grape.ics.uci.edu"}:
            return False

        # Detect and avoid traps: repeated path segments
        path = parsed.path
        path_lower = path.lower()
        path_parts = [p for p in path.split("/") if p]
        if len(path_parts) != len(set(path_parts)):
            return False  # repeated segment = likely trap

        # The Events Calendar archive views generate unbounded date/list pages.
        if (
            "/events/" in path_lower
            or path_lower.endswith("/events")
            or "/events/list" in path_lower
            or "/events/month" in path_lower
            or "/events/category/" in path_lower
            or "/events/tag/" in path_lower
            or re.search(r"/events/\d{4}-\d{2}(?:-\d{2})?/?$", path_lower)
        ):
            return False

        if "/doku.php" in path_lower:
            return False

        if "/~eppstein/pix" in path_lower or "/%7eeppstein/pix" in path_lower:
            return False

        if hostname == "fano.ics.uci.edu" and path_lower.startswith("/ca/rules/"):
            return False

        archive_page = re.search(
            r"/(?:blog|author/[^/]+|category/[^/]+|tag/[^/]+|[^/]+)/page/(\d+)/?$",
            path_lower,
        )
        if archive_page and int(archive_page.group(1)) > 10:
            return False

        if re.search(r"/files/zimage", path_lower):
            return False

        if "/lib/exe/" in path_lower:
            return False

        # Avoid very long/generated URLs.
        if len(url) > 250:
            return False

        query = parse_qs(parsed.query, keep_blank_values=True)

        blocked_query_keys = {
            "do",
            "eventDisplay",
            "ical",
            "idx",
            "image",
            "mediado",
            "ns",
            "outlook-ical",
            "rev",
            "rev2[0]",
            "rev2[1]",
            "tab_details",
            "tab_files",
        }
        if any(key in query for key in blocked_query_keys):
            return False

        # Department seed pages link to finite people directories with
        # filter[units]. Keep those, but avoid broader faceted-search traps.
        allowed_filter_keys = {"filter[units]"}
        blocked_query_prefixes = (
            "tribe-",
            "tribe_",
        )
        if any(
            key.startswith("filter[") and key not in allowed_filter_keys
            for key in query
        ):
            return False
        if any(key.startswith(blocked_query_prefixes) for key in query):
            return False

        blocked_query_values = (
            "facebook.com/share_channel",
            "eventDisplay=day",
            "ical=1",
            "outlook-ical=1",
        )
        if any(value in parsed.query for value in blocked_query_values):
            return False
        if any(
            "facebook.com/share_channel" in value
            for values in query.values()
            for value in values
        ):
            return False

        # Filter out non-webpage file types
        return not re.search(
            r"\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex"
            r"|ppt|pptx|pps|ppsx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar"
            r"|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|thmx"
            r"|mso|arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower(),
        )

    except (TypeError, ValueError):
        return False

