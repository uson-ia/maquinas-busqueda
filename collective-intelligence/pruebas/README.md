# Pruebas

Esta carpeta sirve para hacer pruebas con el código de `searchengine.py`, en el archivo `crawler.py` se encontrará la implementación de cada método relacionado con el indexado como función, es decir, no son miembros de una clase.

Para probar el código por primera vez, se sugiere escribir los siguientes comandos en el REPL de ipython:

```
In [1]: from crawler import *

In [2]: connection = db_connect(test_db)

In [3]: db_create_tables(connection) # Va a tardar un ratitito en crearlas

In [4]: crawl(test_urls, connection, depth=10) # Va a tardar un buen rato pero se despliega la info

In [5]: urllist, wordlist, wordlocation, link, linkwords = db_get_tables(connection)
```

Después de esto se puede explorar el log que vació en `[4]` la llamada a `crawl` o explorar la información almacenada en las tablas con las listas llamadas `urllist`, `wordlist`, `wordlocation`, `link`, `linkwords`.

Al finalizar la exploración se debe cerrar manualmente la conexión:

```
In [6]: db_close(connection)
```

Si se hace algún cambio en el código sugiero que eliminen el archivo de la base de datos `searchindex.db` para que el crawler comience desde cero.
