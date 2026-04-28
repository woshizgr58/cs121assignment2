import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.parse import urljoin, urldefrag

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    
    # Base case
    if not resp or resp.error or resp.raw_response is None or resp.status != 200:
        return []
    
    # Parse
    links = []
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    base_url = resp.url if getattr(resp, "url", None) else url
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

        # Only allow these domains
        allowed_domains = [
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
        ]
        hostname = parsed.hostname or ""
        if not any(hostname == d or hostname.endswith("." + d) for d in allowed_domains):
            return False

        # Detect and avoid traps: repeated path segments
        path = parsed.path
        path_parts = [p for p in path.split("/") if p]
        if len(path_parts) != len(set(path_parts)):
            return False  # repeated segment = likely trap

        # Avoid very long URLs (often traps)
        # if len(url) > 200:
        #     return False

        # Filter out non-webpage file types
        return not re.search(
            r"\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex"
            r"|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar"
            r"|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|thmx"
            r"|mso|arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower(),
        )

    except TypeError:
        return False

