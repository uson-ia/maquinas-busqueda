# -*- coding: utf-8 -*-

import urllib2
from bs4 import *
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite
import re
import os

dirpath =  os.path.dirname(os.path.abspath(__file__))

test_urls    = ["http://eduardoacye.github.io"]
test_db      = "searchindex.db"
ignore_words = set([line.strip() for line in
                    open(dirpath + "/stop-words/stop-words_spanish_1_es.txt", "r")]
                   +
                   [line.strip() for line in
                    open(dirpath + "/stop-words/stop-words_spanish_2_es.txt", "r")])

class crawler:
    def __init__(self, db_name):
        self.connection = sqlite.connect(dirpath + "/" + db_name)

    def __del__(self):
        self.connection.close()

    def db_commit(self):
        self.connection.commit()

    def select_entry_id(self, table, column, value):
        """
        table es una cadena de caracteres que representa el nombre de una tabla de
        la base de datos

        column es una cadena de caracteres que representa el nombre de una columna
        de la tabla especificada de la base de datos

        value es un valor que puede estar almacenado en la columna especificada de
        la tabla especificada de la base de datos

        regresa None si no se encontró una entrada con el valor en la columna de la
        tabla, de lo contrario se regresa el id de la entrada
        """
        table = self.connection.execute("select rowid from %s where %s = '%s'"
                                        % (table, column, value))
        result = table.fetchone()
        if result is None:
            return result
        else:
            return result[0]

    def insert_entry(self, table, column, value):
        """
        table es una cadena de caracteres que representa el nombre de una tabla de
        la base de datos

        column es una cadena de caracteres que representa el nombre de una columna
        de la tabla especificada de la base de datos

        value es un valor que puede estar almacenado en la columna especificada de
        la tabla especificada de la base de datos

        crea una nueva entrada en la tabla especificada con el valor dado en la columna
        especificada
        """
        table = self.connection.execute("insert into %s (%s) values ('%s')"
                                        % (table, column, value))
        return table.lastrowid

    def index_page_words(self, url_id, words):
        """
        url_id es un id de la base de datos que está asociada a una página web

        words es una lista de palabras

        relaciona las palabras con la url en la base de datos
        """
        print "      INDEXING WORDS FROM URL ID %s" % url_id
        for i in range(len(words)):
            word = words[i]
            if word in ignore_words:
                continue
            word_id = self.select_entry_id("wordlist", "word", word)
            if word_id is None:
                print "        %s" % word
                self.insert_entry("wordlist", "word", word)
                word_id = self.select_entry_id("wordlist", "word", word)
            self.connection.execute("insert into wordlocation(urlid, wordid, location) values (%d,%d,%d)"
                                    % (url_id, word_id, i))

    def index_page(self, url, html):
        """
        url es una cadena de caracteres que representa una URL

        html es un objeto BeautifulSoup

        procesa el documento de HTML para relacionar las palabras en el
        documento con la URL
        """
        if self.is_indexed(url):
            return
        print "    INDEXING %s" % url
        text = self.strip_html_tags(html)
        words = self.separate_words(text)

        url_id = self.select_entry_id("urllist", "url", url)
        if url_id is None:
            self.insert_entry("urllist", "url", url)
            url_id = self.select_entry_id("urllist", "url", url)
        self.index_page_words(url_id, words)
        print "      INDEXED! %s" % url

    def strip_html_tags(self, html):
        """
        html es un objeto BeautifulSoup

        regresa una cadena unicode con el contenido del HTML sin las etiquetas
        """
        html_inside_tag = html.string
        if html_inside_tag is None:
            resulting_text = ""
            for tag in html.contents:
                sub_text = self.strip_html_tags(tag)
                resulting_text += sub_text + "\n"
            return resulting_text
        else:
            return html_inside_tag.strip()

    def separate_words(self, text):
        """
        text es una cadena unicode que representa algún texto

        regresa una lista con las palabras del texto
        """
        splitter = re.compile(ur"\W*", re.UNICODE)
        splitted = splitter.split(text)
        return [s.lower() for s in splitted if s != ""]

    def is_indexed(self, url):
        """
        url es una cadena de caracteres que representa una URL

        regresa un booleano que determina si la URL ya fué inspeccionada por el crawler
        """
        table = self.connection.execute("select rowid from urllist where url='%s'" % url)
        result = table.fetchone()
        if result is not None:
            url_id = self.connection.execute("select * from wordlocation where urlid = %d"
                                             % result[0]).fetchone()
            if url_id is not None:
                return True
        return False

    def index_link_words(self, link_id, words):
        """
        link_id es un id de la base de datos que está asociada a enlace

        words es una lista de palabras

        relaciona las palabras con el enlace en la base de datos
        """
        for word in words:
            if word in ignore_words:
                continue
            word_id = self.select_entry_id("wordlist", "word", word)
            if word_id is None:
                print "        PALABRA %s" % word
                self.insert_entry("wordlist", "word", word)
                word_id = self.select_entry_id("wordlist", "word", word)
            self.connection.execute("insert into linkwords(wordid, linkid) values (%d,%d)"
                                    % (word_id, link_id))

    def index_link(self, from_url, to_url, link):
        """
        from_url es una cadena de caracteres que representa una URL

        to_url es una cadena de caracteres que representa una URL

        link es un objeto BeautifulSoup.Tag

        procesa la etiqueta de HTML <a...>...</a> para relacionar las palabras en el
        enlace con las URLs involucradas
        """
        text  = self.strip_html_tags(link)
        words = self.separate_words(text)

        from_url_id = self.select_entry_id("urllist", "url", from_url)
        if from_url_id is None:
            self.insert_entry("urllist", "url", from_url)
            from_url_id = self.select_entry_id("urllist", "url", from_url)
        to_url_id = self.select_entry_id("urllist", "url", to_url)
        if to_url_id is None:
            self.insert_entry("urllist", "url", to_url)
            to_url_id = self.select_entry_id("urllist", "url", to_url)

        if from_url_id == to_url_id:
            return
        table = self.connection.execute("insert into link (fromid, toid) values (%d,%d)"
                                        % (from_url_id, to_url_id))
        link_id = table.lastrowid
        self.index_link_words(link_id, words)

    def get_page(self, url):
        """
        url es una cadena de caracteres.

        regresa el contenido y la codificación de la página asociada a la URL
        """
        try:
            resource = urllib2.urlopen(url)
        except:
            #raise Exception("Could not open %s" % url)
            return None, None
        content_type = resource.headers["content-type"]

        if content_type[:9] != "text/html":
            return None, None

        charset_idx = content_type.find("charset")
        try:
            if charset_idx == -1:
                data = resource.read()
                charset_idx = data.find("charset=")
                if charset_idx == -1:
                    encoding = "ascii"
                    content = unicode(data, encoding)
                else:
                    encoding = re.split(";|\"", data[charset_idx+8:])[0]
                    content = unicode(data, encoding)
            else:
                encoding = content_type[charset_idx+8:].split(";")[0]
                content = unicode(resource.read(), encoding)
                #encoding = content_type.split("charset=")[-1]
        except:
            return None, None
        return content, encoding

    def parse_page(self, content):
        """
        content es una cadena unicode cuyo contenido son los caracteres que conforman
        a una página en HTML

        regresa un objeto BeautifulSoup que representa el HTML parseado
        """
        html_tree = BeautifulSoup(content)
        return html_tree

    def has_href(self, link):
        """
        link es un objeto BeautifulSoup.Tag

        regresa un booleano determinando si el enlace contiene un atributo href
        """
        return "href" in link.attrs

    def link_url(self, base_url, link):
        """
        base_url es una cadena de caracteres que representa una URL

        link es un objeto BeautifulSoup.Tag

        regresa una cadena de caracteres que representa la URL del enlace
        """
        url = urljoin(base_url, link["href"])
        if url.find("'") != -1:
            #raise Exception("Malformed URL %s" % url)
            return None
        url = url.split("#")[0]
        return url

    def is_http(self, url):
        """
        url es una cadena de caracteres que representa una URL

        regresa un booleano determinando si el recurso web que indica la URL es
        una página web
        """
        return url[0:4] == "http"

    def crawl(self, urls, depth=2):
        """
        urls es una lista de cadenas de caracteres que representan URLs

        depth es un entero positivo que representa la profundidad a la que
        se procesarán las páginas (similar a la profundidad de una búsqueda
        en grafos)
        """
        print "CRAWL"
        for i in range(depth):
            print "  DEPTH %d" % i
            new_urls = set()
            print "  URLs %s" % urls
            for url in urls:
                print "%d  VISITING %s" % (i, url)
                content, encoding = self.get_page(url)
                if content is None and encoding is None:
                    continue
                html = self.parse_page(content)
                print "    VISITED %s" % url
                self.index_page(url, html)

                links = html.select("a")
                for link in links:
                    if self.has_href(link):
                        ref_url = self.link_url(url, link)
                        if ref_url is None: continue
                        if self.is_http(ref_url) and not self.is_indexed(ref_url):
                            new_urls.add(ref_url)
                        self.index_link(url, ref_url, link)
                self.db_commit()
            urls = list(new_urls)

    # Crear las tablas de base de datos
    def db_create_tables(self):
        self.connection.execute('create table urllist(url)')
        self.connection.execute('create table wordlist(word)')
        self.connection.execute('create table wordlocation(urlid, wordid, location)')
        self.connection.execute('create table link(fromid integer, toid integer)')
        self.connection.execute('create table linkwords(wordid, linkid)')
        self.connection.execute('create index wordidx on wordlist(word)')
        self.connection.execute('create index urlidx on urllist(url)')
        self.connection.execute('create index wordurlidx on wordlocation(wordid)')
        self.connection.execute('create index urltoidx on link(toid)')
        self.connection.execute('create index urlfromidx on link(fromid)')
        self.db_commit()

    def db_get_table(self, table):
        """
        table es una cadena de caracteres que representa el nombre de la tabla
        en la base de datos

        regresa una lista con el contenido de la tabla especificada
        """
        table = self.connection.execute("select * from %s" % table)
        return table.fetchall()

    def db_get_tables(self):
        """
        regresa una tupla con listas con el contenido de cada tabla en la base de datos
        """
        return (self.db_get_table("urllist"),
                self.db_get_table("wordlist"),
                self.db_get_table("wordlocation"),
                self.db_get_table("link"),
                self.db_get_table("linkwords"))

    def calculate_pagerank(self, iterations=20):
        self.connection.execute("drop table if exists pagerank")
        self.connection.execute("create table pagerank(urlid primary key, score)")

        self.connection.execute("insert into pagerank select rowid, 1.0 from urllist")
        self.db_commit()
        print "CALCULATING PAGERANK..."
        for i in range(iterations):
            print "  ITERATION %d" % i
            for (url_id,) in self.connection.execute("select rowid from urllist"):
                pr = 0.15
                for (linker,) in self.connection.execute("select distinct fromid from link where toid=%d" % url_id):
                    linking_pr = self.connection.execute("select score from pagerank where urlid=%d"
                                                         % linker).fetchone()[0]
                    linking_count = self.connection.execute(
                        "select count(*) from link where fromid=%d" % linker).fetchone()[0]
                    pr += 0.95*(linking_pr/linking_count)
                self.connection.execute("update pagerank set score=%f where urlid=%d" % (pr, url_id))
            self.db_commit()
        print "PAGERANK CALCULATION COMPLETED"

class searcher:
    def __init__(self, db_name):
        self.connection = sqlite.connect(dirpath + "/" + db_name)

    def __del__(self):
        self.connection.close()

    def get_matched_rows(self, search_query):
        field_list  = "w0.urlid"
        table_list  = ""
        clause_list = ""
        word_ids    = []

        words = search_query.split()
        table_number = 0

        for word in words:
            word_row = self.connection.execute("select rowid from wordlist where word='%s'"
                                               % word).fetchone()
            if word_row is not None:
                word_id = word_row[0]
                word_ids.append(word_id)
                if table_number > 0:
                    table_list  += ","
                    clause_list += " and "
                    clause_list += "w%d.urlid = w%d.urlid and " % (table_number-1, table_number)
                field_list   += ",w%d.location" % table_number
                table_list   += "wordlocation w%d" % table_number
                clause_list  += "w%d.wordid=%d" % (table_number, word_id)
                table_number += 1

        full_query = "select %s from %s where %s" % (field_list, table_list, clause_list)
        print "BEGIN FULL QUERY"
        print full_query
        print "END FULL QUERY"

        if table_list == "" or clause_list == "":
            return None, None
        else:
            table = self.connection.execute(full_query)
            rows = [row for row in table]
            return rows, word_ids

    def get_scored_list(self, rows, word_ids):
        total_scores = dict([(row[0], 0) for row in rows])
        weights = [(1.0, self.location_score(rows)),
                   (1.0, self.frequency_score(rows)),
                   (1.0, self.distance_score(rows)),
                   (1.0, self.inbound_link_score(rows)),
                   (1.0, self.pagerank_score(rows)),
                   (1.0, self.link_text_score(rows, word_ids))]
        for (weight, scores) in weights:
            for url in total_scores:
                total_scores[url] += weight*scores[url]
        return total_scores

    def get_url_name(self, id):
        return self.connection.execute("select url from urllist where rowid = %d" % id).fetchone()[0]

    def query(self, search_query):
        rows, word_ids = self.get_matched_rows(search_query)
        if rows is None and word_ids is None:
            print "No results where found"
        else:
            scores = self.get_scored_list(rows, word_ids)
            ranked_scores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
            for (score, url_id) in ranked_scores[0:10]:
                print '%f\t%s' % (score, self.get_url_name(url_id))

    def normalize_scores(self, scores, small_is_better = False):
        vsmall = 0.00001
        if small_is_better:
            min_score = min(scores.values())
            return dict([(u, float(min_score)/max(vsmall, l)) for (u, l) in scores.items()])
        else:
            max_score = max(scores.values())
            if max_score == 0: max_score = vsmall
            return dict([(u, float(c)/max_score) for (u, c) in scores.items()])

    def frequency_score(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows:
            counts[row[0]] += 1
        return self.normalize_scores(counts)

    def location_score(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]: locations[row[0]] = loc

        return self.normalize_scores(locations, small_is_better = True)

    def distance_score(self, rows):
        # Si hay solo una palabra, todos ganan
        if len(rows[0]) <= 2: return dict([(row[0], 1.0) for row in rows])

        # Inicializa el diccionario con valores grandes
        min_distance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            distance = sum([abs(row[i]-row[i-1]) for i in range(2, len(row))])
            if distance < min_distance[row[0]]: min_distance[row[0]] = distance

        return self.normalize_scores(min_distance, small_is_better = True)

    def inbound_link_score(self, rows):
        unique_urls = set([row[0] for row in rows])
        inbound_count = dict([(u, self.connection.execute("select count(*) from link where toid=%d"
                                                          % u).fetchone()[0])
                              for u in unique_urls])
        return self.normalize_scores(inbound_count)

    def pagerank_score(self, rows):
        pageranks = dict([(row[0], self.connection.execute("select score from pagerank where urlid=%d"
                                                           % row[0]).fetchone()[0]) for row in rows])
        max_rank = max(pageranks.values())
        normalized_scores = dict([(u, float(l)/max_rank) for (u,l) in pageranks.items()])
        return normalized_scores

    def link_text_score(self, rows, word_ids):
        link_scores = dict([(row[0], 0) for row in rows])
        for word_id in word_ids:
            table = self.connection.execute(
            "select link.fromid, link.toid from linkwords, link where wordid=%d and linkwords.linkid=link.rowid"
                % word_id)
            for (from_url_id, to_url_id) in table:
                if to_url_id in link_scores:
                    pr = self.connection.execute("select score from pagerank where urlid=%d"
                                                 % from_url_id).fetchone()[0]
                    link_scores[to_url_id] += pr
        max_score = max(link_scores.values())
        normalized_scores = dict([(u,float(l)/max_score) for (u,l) in link_scores.items()])
        return normalized_scores
