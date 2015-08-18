# -*- coding: utf-8 -*-

import urllib2
from bs4 import *
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite
import re

ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])

class crawler:
    # Inicializa el crawler con el nombre de la base de datos
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def db_commit(self):
        self.con.commit()

    # Función auxiliar para obtener la id de una entrada y añadirla
    # si no está presente
    def getentryid(self, table, field, value, createnew=True):
        cur = self.con.execute("select rowid from %s where %s = '%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute("insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    # Indexar una página
    def index_page(self, url, soup):
        if self.is_indexed(url):
            return
        print 'Indexing ' + url

        # Obtener las palabras individuales
        text = self.strip_html_tags(soup)
        words = self.separate_words(text)

        # Obtener la id de la URL
        urlid = self.getentryid('urllist', 'url', url)

        # Enlazar cada palabra con esta URL
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, location) values (%d,%d,%d)"
                             % (urlid, wordid, i))

    # Extraer el texto de una página de HTML (sin etiquetas)
    def strip_html_tags(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.strip_html_tags(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()

    # Separar las palabras por un caracter que no sea espacio en blanco
    def separate_words(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    # Regresar True si la url dada ya ha sido indexada
    def is_indexed(self, url):
        u = self.con.execute("select rowid from urllist where url='%s'" % url).fetchone()
        if u != None:
            # Revisa si la url ya fué indexada
            v = self.con.execute('select * from wordlocation where urlid = %d' % u[0]).fetchone()
            if v != None:
                return True
        return False

    # Agregar un enlace entre dos páginas
    def index_link(self, urlFrom, urlTo, linkText):
        fromid = self.getentryid('urllist', 'url', urlfrom)
        toid = self.getentryid('urllist', 'url', urlto)
        if fromid != toid: cur = self.con.execute('insert into link (fromid, toid) values (%d, %d)'% (fromid, toid))


    # Comenzando con una lista de páginas, hacer una búsqueda a lo ancho
    # a una profundidad dada, indexando las páginas en el proceso
    def crawl(self, pages, depth=2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open %s" % page
                    continue
                soup = BeautifulSoup(c.read().decode("ascii", "ignore"))
                self.index_page(page, soup)

                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0] # Remueve la parte de localización
                        if url[0:4] == 'http' and not self.is_indexed(url):
                            newpages.add(url)
                        linkText = self.strip_html_tags(link)
                        self.index_link(page, url, linkText)

                self.db_commit()

            pages = newpages

    # Crear las tablas de base de datos
    def db_create_tables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid, wordid, location)')
        self.con.execute('create table link(fromid integer, toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.db_commit()

class searcher:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

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
            wordrow = self.con.execute("select rowid from wordlist where word = '%s'" % word).fetchone()
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
            cur = self.con.execute(fullquery)
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
        return self.con.execute("select url from urllist where rowid = %d" % id).fetchone()[0]

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
        inboundcount = dict([(u, self.con.execute("select count(*) from link where toid=%d" % u).fetchone()[0])
                             for u in uniqueurls])
        return self.normalizescores(inboundcount)
