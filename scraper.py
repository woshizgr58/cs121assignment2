import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.parse import urljoin, urldef

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    
    # Base case
    if resp.error or res.raw_response == [] or not resp or resp.status != 200:
        return []
    
    # Parse
    links = []
    soup = BeautifulSoup(res.raw_response.content, "html.parser")
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
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
