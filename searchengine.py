__author__ = 'JuanManuel'

import urllib2
from BeautifulSoup import *
from urlparse import urljoin

# Se crea una lista de palabras a ignorar
ignorewords = set(['the','of','to','and','a','in','is','it'])

class crawler(object):
    # Se inicializa el crawler con el nombre de la base de datos
    def __init__(self, dbname):
        pass

    def __del__(self):
        pass

    def dbcommit(self):
        pass

    # Funcion auxiliar para obtener el id de la entrada y agregarlo
    # si no esta presente
    def getentryid(self, table, field, value, createnew=True):
        return None

    # Indexar una pagina individual
    def addtoindex(self, url, soup):
        print 'Indexing ' + url

    # Extrae el texto de una pagina HTML (sin tags)
    def gettextonly(self, soup):
        return None

    # Separa las palabras
    def separatewords(self, text):
        return None

    # Retorna true si esta url ya esta indexada
    def isindexed(self, url):
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
        pass


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

if __name__ == "__main__":
    main()
