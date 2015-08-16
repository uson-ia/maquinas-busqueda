# -*- coding: utf-8 -*-

import urllib2
from bs4 import *
from urlparse import urljoin

test_url = "http://eduardoacye.github.io"

def get_page(url):
    try:
        resource = urllib2.urlopen(url)
    except:
        raise Exception("Could not open %s" % url)
    encoding = resource.headers["content-type"].split("charset=")[-1]
    content = unicode(resource.read(), encoding)
    return content, encoding

def parse_page(content):
    html_tree = BeautifulSoup(content)
    return html_tree

def has_href(link):
    return "href" in link.attrs

def link_url(base_url, link):
    url = urljoin(base_url, link["href"])
    if url.find("'") != -1:
        raise Exception("Malformed URL %s" % url)
    url = url.split("#")[0]
    return url

def is_http(url):
    return url[0:4] == "http"

def crawl(urls, depth=2):
    print "CRAWL"
    for i in range(depth):
        print "  DEPTH %d" % i
        new_urls = set()
        print "  URLs %s" % urls
        for url in urls:
            content, encoding = get_page(url)
            html = parse_page(content)
            print "    VISITED %s" % url

            links = html.select("a")
            for link in links:
                if has_href(link):
                    ref_url = link_url(url, link)
                    new_urls.add(ref_url)
        urls = list(new_urls)

