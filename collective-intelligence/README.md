# Searching and Ranking
## Programming Collective Intelligence capítulo 4

Este repositorio hará seguimiento de la implementación de un pequeño motor de búsqueda programado en Python sacado directamente del libro *Programming Collective Intelligence*.

El contenido de este capítulo está disponible en la sección de recursos en la página del curso *Tópicos avanzados en inteligencia artificial* del Piazza.

## Instrucciones para explorar la USON y CC

```
In [1]: import searchengine

In [2]: urls = ["http://www.uson.mx", "http://http://cc.uson.mx/"]

In [3]: db = "uson_lcc.db"

In [4]: crawler = searchengine.crawler(db)

In [5]: crawler.db_create_tables()

In [6]: crawler.crawl(urls, depth=500)

In [7]: crawler.calculate_pagerank()

In [8]: searcher = searchengine.searcher("uson.db")

In [9]: searcher.query("universidad")

```
