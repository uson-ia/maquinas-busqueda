# -*- coding: utf-8 -*-

import urllib2
from BeautifulSoup import *
from urlparse import urljoin
from pysqlite2 import dbapi2 as sqlite

ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])

class crawler:
    # Inicializa el crawler con el nombre de la base de datos
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
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
    def addtoindex(self, url, soup):
        if self.isindexed(url):
            return
        print 'Indexing ' + url

        # Obtener las palabras individuales
        text = self.gettextonly(soup)
        words = self.separatewords(text)

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
    def gettextonly(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()

    # Separar las palabras por un caracter que no sea espacio en blanco
    def separatewords(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    # Regresar True si la url dada ya ha sido indexada
    def isindexed(self, url):
        u = self.con.execute("select rowid from urllist where url='%s'" % url).fetchone()
        if u != None:
            # Revisa si la url ya fué indexada
            v = self.con.execute('select * from wordlocation where urlid = %d' % u[0]).fetchone()
            if v != None:
                return True
        return False

    # Agregar un enlace entre dos páginas
    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

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
                self.addtoindex(page, soup)

                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0] # Remueve la parte de localización
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)

                self.dbcommit()

            pages = newpages

    # Crear las tablas de base de datos
    def createindextables(self):
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
        self.dbcommit()

