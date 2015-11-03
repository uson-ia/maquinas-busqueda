__author__ = 'JuanManuel'

import urllib2
from BeautifulSoup import *
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite

# Se crea una lista de palabras a ignorar
ignorewords = set(['the','of','to','and','a','in','is','it'])

class crawler(object):
    # Se inicializa el crawler con el nombre de la base de datos
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    # Funcion auxiliar para obtener el id de la entrada y agregarlo
    # si no esta presente
    def getentryid(self, table, field, value, createnew=True):
        cur = self.con.execute(
        "select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
            "insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    # Indexar una pagina individual
    def addtoindex(self, url, soup):
        # print 'Indexando %s' % url
        if self.isindexed(url):
            return
        print 'Indexando ' + url

        # Obtener las palabras individuales
        text = self.gettextonly(soup)
        words = self.separatewords(text)

        # Obtener el id de la URL
        urlid = self.getentryid('urllist', 'url', url)

        # Linkear cada palabra con esta URL
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, location) \
                values (%d,%d,%d)" % (urlid, wordid, i))

    # Extrae el texto de una pagina HTML (sin tags)
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

    # Separa las palabras
    def separatewords(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    # Retorna true si esta url ya esta indexada
    def isindexed(self, url):
        u = self.con.execute \
        ("select rowid from urllist where url='%s'" % url).fetchone()
        if u != None:
        # Revisa si realmente se a crawleado
            v = self.con.execute(
            'select * from wordlocation where urlid=%d' % u[0]).fetchone( )
            if v != None: 
                return True
        return False

    # Agrega un link entre dos paginas
    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    # Comienza con una lista de paginas, hace una amplitud
    # primero busca a la profundidad dada , indexando paginas
    # como va
    def crawl(self, pages, depth = 2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "No se puede abrir %s" % page
                    continue
                soup = BeautifulSoup(c.read())
                self.addtoindex(page, soup)

                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)

                self.dbcommit()
            pages = newpages

    # Se crean las tablas para la base de datos
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

class searcher:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def getmatchrows(self, q):
        # Cadenas para construir el query
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        # Separar las palabras por espacios
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # Obtener el id de la palabra
            wordrow = self.con.execute(
                "select rowid from wordlist where word='%s'" % word).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' % (tablenumber - 1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1
                
        # Se crea el query a partir de las partes separadas
        fullquery = 'select %s from %s where %s' % (fieldlist, tablelist, clauselist)
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]

        return rows, wordids

    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])

        # Aqui van todas las funciones weights para probar cada metrica presentada
        #weights = []
        #weights = [(1.0, self.frequencyscore(rows))]
        #weights = [(1.0, self.locationscore(rows))]
        #weights = [(1.0,self.frequencyscore(rows)), (1.5,self.locationscore(rows))]
        #weights = [(1.0, self.distancescore(rows))]
        #weights = [(1.0, self.inboundlinkscore(rows))]
        weights = [(1.0, self.distancescore(rows))]

        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]

        return totalscores

    def geturlname(self, id):
        return self.con.execute(
        "select url from urllist where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse = 1)
        for (score, urlid) in rankedscores[0:10]:
            print '%f\t%s' % (score,self.geturlname(urlid))

    def normalizescores(self, scores, smallIsBetter = 0):
        vsmall = 0.00001 # Se evita la division por cero
        if smallIsBetter:
            minscore = min(scores.values())
            return dict([(u,float(minscore)/max(vsmall,l)) for (u,l) \
                in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore == 0:
                maxscore = vsmall
            return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])

    def frequencyscore(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows: 
            counts[row[0]] += 1
        return self.normalizescores(counts)

    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc
        return self.normalizescores(locations, smallIsBetter = 1)

    def distancescore(self, rows):
        # Si solo hay una palabra, todos ganamos!
        if len(rows[0]) <= 2:
            return dict([(row[0], 1.0) for row in rows])

        # Inicializa el diccionario con valores largos
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i]-row[i-1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]: 
                mindistance[row[0]] = dist

        return self.normalizescores(mindistance, smallIsBetter=1)

    def inboundlinkscore(self, rows):
        uniqueurls = set([row[0] for row in rows])
        inboundcount = dict([(u,self.con.execute( \
            'select count(*) from link where toid=%d' % u).fetchone()[0]) \
            for u in uniqueurls])
        return self.normalizescores(inboundcount)

def main():
    print "Ejemplos que aparecen en el proyecto principal"

    """
    print "Ejemplo 1"
    import urllib2
    c = urllib2.urlopen('https://en.wikipedia.org/wiki/Game_of_Thrones')
    contents = c.read()
    print contents[0:50]
    """

    """
    print "Ejemplo 2"
    import searchengine
    pagelist = ['https://en.wikipedia.org/wiki/Game_of_Thrones']
    crawler = searchengine.crawler('')
    crawler.crawl(pagelist)
    """

    """
    print "Ejemplo 3"
    import searchengine
    crawler = searchengine.crawler('searchindex.db')
    crawler.createindextables()
    """

    """
    print "Ejemplo 4"
    import searchengine
    crawler = searchengine.crawler('searchindex.db')
    crawler.createindextables()
    """

    """
    print "Ejemplo 5"
    import searchengine
    crawler = searchengine.crawler('searchindex.db')
    pages = ['https://en.wikipedia.org/wiki/Game_of_Thrones']
    crawler.crawl(pages)
    [row for row in crawler.con.execute('select rowid from wordlocation where wordid=1')]
    """

    """
    print "Ejemplo 6 :("
    import searchengine
    e = searchengine.searcher('searchindex.db')
    e.getmatchrows('John')
    """

    """
    print "Ejemplo 7 :("
    import searchengine
    e = searchengine.searcher('searchindex.db')
    e.query('John')
    """

    """
    print "Ejemplo 8 :("
    import searchengine
    e = searchengine.searcher('searchindex.db')
    e.query('John')
    """

    """
    print "Ejemplo 9 :("
    import searchengine
    e = searchengine.searcher('searchindex.db')
    e.query('John')
    """

if __name__ == "__main__":
    main()
