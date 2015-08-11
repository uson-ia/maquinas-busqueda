# -*- coding: utf-8 -*-

class crawler:
    # Inicializa el crawler con el nombre de la base de datos
    def __init__(self, dbname):
        pass

    def __del__(self):
        pass

    def dbcommit(self):
        pass

    # Función auxiliar para obtener la id de una entrada y añadirla
    # si no está presente
    def getentryid(self, table, field, value, createnew=True):
        return None

    # Indexar una página
    def addtoindex(self, url, soup):
        print 'Indexing %s' % url

    # Extraer el texto de una página de HTML (sin etiquetas)
    def gettextonly(self, soup):
        return None

    # Separar las palabras por un caracter que no sea espacio en blanco
    def separatewords(self, text):
        return None

    # Regresar True si la url dada ya ha sido indexada
    def isindexed(self, url):
        return False

    # Agregar un enlace entre dos páginas
    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    # Comenzando con una lista de páginas, hacer una búsqueda a lo ancho
    # a una profundidad dada, indexando las páginas en el proceso
    def crawl(self, pages, depth=2):
        pass

    # Crear las tablas de base de datos
    def createindextables(self):
        pass

