#Moises Gabriel Salas Aguilar Aguila

#librerias
import urllib2
from bs4 import *
from urlparse import urljoin
from pysqlite2 import dbapi2 as sqlite

#crear una lista de palabras que ignorar
ignorewords = set(['the','of','to','and','a','in','is','it'])


#clase que se encargara de buscar
class crawler:
	#Inicializamos la clase con el nombre de la base de datos
	def __init__(self,dbname):
		self.con = sqlite.connect(dbname)
		

	def __del__(self):
		self.con.close()

	def dbcommit(self):
		self.con.commit()

	#funcion auxiliar para agarrar una id de entrada y agregarla si no esta presente
	def getentryid(self,table,field,value,createnew = True):
		return None

	#funcion para indexar una pagina
	def addtoindex(self,url,soup):
		print 'Indexing %s' % url

	#funcion para extraer texto de una pagina html
	def gettextonly(self,soup):
		v = soup.string
		if v == None:
			c = soup.contents
			resulttext = ''
			for t in c:
				subtext = self.gettextonly(t)
				resulttext += subtext + "\n"
			return resulttext
		else:
			return v.strip()

	#funcion para separar las palabras
	def separatewords(self,text):
		return None

	#checa si el url ya se encuentra indexado
	def isindexed(self,url):
		return False

	# anade un link entre dos paginas
	def addlinkref(self,urlFrom,urlTo,linkText):
		pass

	# comenzando con una lista de paginas, se hace una busqueda primero a lo ancho hasta la profundidad dada, 
	# mientras vamos indexando paginas
	def crawl(self,pages,depth = 2):
		for i in range(depth):
			newpages=set()
			for page in pages:
				try:
					c=urllib2.urlopen(page)
				except:
					print "Could not open %s" %page
					continue
				soup = BeautifulSoup(c.read())
				self.addtoindex(page,soup)

				links = soup('a')
				for link in links:
					if('href' in dict(link.attrs)):
						url = urljoin(page,link['href'])
						if url.find("'") != -1: continue #
						url = url.split('#')[0] #
						if url[0:4] == 'http' and not self.isindexed(url):
							newpages.add(url)
						linkText = self.gettextonly(link)
						self.addlinkref(page,url,linkText)
					self.dbcommit()
				pages = newpages


	# crea las tablas de la base de datos
	def createindextables(self):
		self.con.execute('create table urllist(url)')
		self.con.execute('create table wordlist(word)')
		self.con.execute('create table wordlocation(urlid,wordid,location)')
		self.con.execute('create table link(fromid integer, toid integer')
		self.con.execute('create table linkwords(wordid,linkid)')
		self.con.execute('create index wordidx on wordlist(word)')
		self.con.execute('create index urlidx on urllist(url)')
		self.con.execute('create index wordurlidx on wordlocation(wordid)')
		self.con.execute('create index urltoidx on link(toid)')
 		self.con.execute('create index urlfromidx on link(fromid)')
 		self.dbcommit( )


def main():



	pagelist = ['https://en.wikipedia.org/wiki/Personal_computer']
	craler = crawler('searchindex.db')
	craler.createindextables()
	craler.crawl(pagelist)
if __name__ == "__main__":
    main()