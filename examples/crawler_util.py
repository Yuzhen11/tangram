import argparse
from bs4 import BeautifulSoup
from urlparse import urljoin
from urllib2 import urlopen

def get_page(url):
    """Get the text of the web page at the given URL
    return a string containing the content"""

    fd = urlopen(url)
    content = fd.read()
    fd.close()

    return content.decode('utf8',"ignore")

def get_links(url):
    """Scan the text for http URLs and return a set
    of URLs found, without duplicates"""

    # look for any http URL in the page
    links = set()

    text = get_page(url)
    soup = BeautifulSoup(text, "lxml")

    for link in soup.find_all('a'):
        if 'href' in link.attrs:
            newurl = link.attrs['href']
            # resolve relative URLs
            if newurl.startswith('/'):
                newurl = urljoin(url, newurl)
            # ignore any URL that doesn't now start with http
            if newurl.startswith('http'):
                links.add(newurl)

    return links

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('url', help='url to crawl')

    args = parser.parse_args()
    links = get_links(args.url)
    print u' '.join(links).encode('utf8')


