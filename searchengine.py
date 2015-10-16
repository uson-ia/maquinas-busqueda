__author__ = 'JuanManuel'

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
	def getentryid(self, table, field, value, createnew = True):
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
		pass

	# Se crean las tablas para la base de datos
	def createindextables(self):
		pass