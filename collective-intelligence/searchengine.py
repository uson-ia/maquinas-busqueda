# -*- coding: utf-8 -*-

import urllib2
from bs4 import *
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite
import re

test_urls    = ["http://eduardoacye.github.io"]
test_db      = "searchindex.db"
ignore_words = set([line.strip() for line in
                    open("stop-words/stop-words_spanish_1_es.txt", "r")]
                   +
                   [line.strip() for line in
                    open("stop-words/stop-words_spanish_2_es.txt", "r")])

class crawler:
    # Inicializa el crawler con el nombre de la base de datos
    def __init__(self, db_name):
        self.connection = sqlite.connect(db_name)

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

    # Extraer el texto de una página de HTML (sin etiquetas)
    def strip_html_tags(self, html):
        html_inside_tag = html.string
        if html_inside_tag == None:
            contents = html.contents
            resulting_text = ''
            for tag in contents:
                sub_text = self.strip_html_tags(tag)
                resulting_text += sub_text + '\n'
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

    def index_link_words(link_id, words):
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

    def index_link(from_url, to_url, link):
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
            raise Exception("Could not open %s" % url)
        encoding = resource.headers["content-type"].split("charset=")[-1]
        content = unicode(resource.read(), encoding)
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
            raise Exception("Malformed URL %s" % url)
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
            for url in urls:
                content, encoding = self.get_page(url)
                html = self.parse_page(content)
                print "    VISITED %s" % url
                self.index_page(url, html)

                links = html.select("a")
                for link in links:
                    if self.has_href(link):
                        ref_url = self.link_url(url, link)
                        if self.is_http(ref_url) and not self.is_indexed(ref_url):
                            new_urls.add(ref_url)
                        self.index_link(url, ref_url, link)

                self.db_commit()

            urls = new_urls

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

class searcher:
    def __init__(self, db_name):
        self.connection = sqlite.connect(db_name)

    def __del__(self):
        self.connection.close()

    def getmatchrows(self, q):
        # Cadenas para construír la consulta
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        # Parte las palabras por espacios
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # Obtener el ID de la palabra
            wordrow = self.connection.execute("select rowid from wordlist where word = '%s'" % word).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid = w%d.urlid and ' % (tablenumber-1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid = %d' % (tablenumber, wordid)
                tablenumber += 1
        try:
            # Crear la consulta a partir de las partes separadas
            fullquery = 'select %s from %s where %s' % \
                (fieldlist, tablelist, clauselist)
            cur = self.connection.execute(fullquery)
            rows = [row for row in cur]
            return rows, wordids
        except:
            print "Error en la base de datos o frase no encontrada"


    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0],0) for row in rows])

        # Pesos para la métrica frequencyscore
        # weights = [(1.0, self.frequencyscore(rows))]

        # Pesos para la métrica locationscore
        #weights = [(1.0, self.locationscore(rows))]

        # Pesos para las métricas frequencyscore y locationscore
        weights = [(1.0, self.frequencyscore(rows)),
                   (1.5, self.locationscore(rows)),
                   (10.0, self.distancescore(rows)),
                   (1.0, self.inboundlinkscore(rows))]

        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight*scores[url]

        return totalscores

    def geturlname(self, id):
        return self.connection.execute("select url from urllist where rowid = %d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print '%f\t%s' % (score, self.geturlname(urlid))

    def normalizescores(self, scores, smallIsBetter = 0):
        vsmall = 0.00001 # Evita errores de división por cero
        if smallIsBetter:
            minscore = min(scores.values())
            return dict([(u, float(minscore)/max(vsmall, l)) for (u, l) in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore == 0: maxscore = vsmall
            return dict([(u, float(c)/maxscore) for (u, c) in scores.items()])

    def frequencyscore(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows: counts[row[0]] += 1
        return self.normalizescores(counts)

    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]: locations[row[0]] = loc

        return self.normalizescores(locations, smallIsBetter=1)

    def distancescore(self, rows):
        # Si hay solo una palabra, todos ganan
        if len(rows[0]) <= 2: return dict([(row[0], 1.0) for row in rows])

        # Inicializa el diccionario con valores grandes
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i]-row[i-1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]: mindistance[row[0]] = dist

        return self.normalizescores(mindistance, smallIsBetter = 1)

    def inboundlinkscore(self, rows):
        uniqueurls = set([row[0] for row in rows])
        inboundcount = dict([(u, self.connection.execute("select count(*) from link where toid=%d" % u).fetchone()[0])
                             for u in uniqueurls])
        return self.normalizescores(inboundcount)
