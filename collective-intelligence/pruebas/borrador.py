# -*- coding: utf-8 -*-

import urllib2
from bs4 import *
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite
import re

test_urls = ["http://eduardoacye.github.io"]
test_db = "searchindex.db"
ignore_words = set(["the", "of", "to", "and", "a", "in", "is", "it"])

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

def db_connect(db_name):
    return sqlite.connect(db_name)

def db_close(connection):
    connection.close()

def db_commit(connection):
    connection.commit()

def db_create_tables(connection):
    connection.execute("create table urllist(url)")
    connection.execute("create table wordlist(word)")
    connection.execute("create table wordlocation(urlid, wordid, location)")
    connection.execute("create table link(fromid integer, toid integer)")
    connection.execute("create table linkwords(wordid, linkid)")
    connection.execute("create index wordidx on wordlist(word)")
    connection.execute("create index urlidx on urllist(url)")
    connection.execute("create index wordurlidx on wordlocation(wordid)")
    connection.execute("create index urltoidx on link(toid)")
    connection.execute("create index urlfromidx on link(fromid)")
    db_commit(connection)

def db_get_table(connection, table):
    table = connection.execute("select * from %s" % table)
    return table.fetchall()

def db_get_tables(connection):
    return (db_get_table(connection, "urllist"),
            db_get_table(connection, "wordlist"),
            db_get_table(connection, "wordlocation"),
            db_get_table(connection, "link"),
            db_get_table(connection, "linkwords"))


def is_indexed(connection, url):
    table = connection.execute("select rowid from urllist where url='%s'" % url)
    result = table.fetchone()
    if result is not None:
        url_id = connection.execute("select * from wordlocation where urlid = %d"
                                    % result[0]).fetchone()
        if url_id is not None:
            return True
    return False

def strip_html_tags(html):
    html_inside_tag = html.string
    if html_inside_tag is None:
        resulting_text = ""
        for tag in html.contents:
            sub_text = strip_html_tags(tag)
            resulting_text += sub_text + "\n"
        return resulting_text
    else:
        return html_inside_tag.strip()

def separate_words(text):
    splitter = re.compile(ur"\W*", re.UNICODE)
    splitted = splitter.split(text)
    return [s.lower() for s in splitted if s != ""]

def select_entry_id(connection, table, column, value):
    table = connection.execute("select rowid from %s where %s = '%s'"
                               % (table, column, value))
    result = table.fetchone()
    if result is None:
        return result
    else:
        return result[0]

def insert_entry(connection, table, column, value):
    table = connection.execute("insert into %s (%s) values ('%s')"
                               % (table, column, value))
    return table.lastrowid

def index_page_words(connection, url_id, words):
    print "      INDEXING WORDS FROM URL ID %s" % url_id
    for i in range(len(words)):
        word = words[i]
        if word in ignore_words:
            continue
        word_id = select_entry_id(connection, "wordlist", "word", word)
        if word_id is None:
            print "        %s" % word
            insert_entry(connection, "wordlist", "word", word)
            word_id = select_entry_id(connection, "wordlist", "word", word)
        connection.execute("insert into wordlocation(urlid, wordid, location) values (%d,%d,%d)"
                           % (url_id, word_id, i))

def index_page(connection, url, html):
    if is_indexed(connection, url):
        return
    print "    INDEXING %s" % url
    text = strip_html_tags(html)
    words = separate_words(text)

    url_id = select_entry_id(connection, "urllist", "url", url)
    if url_id is None:
        insert_entry(connection, "urllist", "url", url)
        url_id = select_entry_id(connection, "urllist", "url", url)
    index_page_words(connection, url_id, words)
    print "      INDEXED! %s" % url

def index_link_words(connection, link_id, words):
    for word in words:
        if word in ignore_words:
            continue
        word_id = select_entry_id(connection, "wordlist", "word", word)
        if word_id is None:
            print "        PALABRA %s" % word
            insert_entry(connection, "wordlist", "word", word)
            word_id = select_entry_id(connection, "wordlist", "word", word)
        connection.execute("insert into linkwords(wordid, linkid) values (%d,%d)"
                           % (word_id, link_id))

def index_link(connection, from_url, to_url, link):
    text = strip_html_tags(link)
    words = separate_words(text)

    from_url_id = select_entry_id(connection, "urllist", "url", from_url)
    if from_url_id is None:
        insert_entry(connection, "urllist", "url", from_url)
        from_url_id = select_entry_id(connection, "urllist", "url", from_url)
    to_url_id = select_entry_id(connection, "urllist", "url", to_url)
    if to_url_id is None:
        insert_entry(connection, "urllist", "url", to_url)
        to_url_id = select_entry_id(connection, "urllist", "url", to_url)

    if from_url_id == to_url_id:
        return
    table = connection.execute("insert into link (fromid, toid) values (%d,%d)"
                               % (from_url_id, to_url_id))
    link_id = table.lastrowid
    index_link_words(connection, link_id, words)

def crawl(urls, connection, depth=2):
    print "CRAWL"
    for i in range(depth):
        print "  DEPTH %d" % i
        new_urls = set()
        print "  URLs %s" % urls
        for url in urls:
            content, encoding = get_page(url)
            html = parse_page(content)
            print "    VISITED %s" % url
            index_page(connection, url, html)

            links = html.select("a")
            for link in links:
                if has_href(link):
                    ref_url = link_url(url, link)
                    if is_http(ref_url) and not is_indexed(connection, ref_url):
                        new_urls.add(ref_url)
                    index_link(connection, url, ref_url, link)
        urls = list(new_urls)

