__author__ = 'JuanManuel'

from math import tanh
from pysqlite2 import dbapi2 as sqlite


"""
Funcion: dtanh(y)
Descripcion: Esta funcion calcula la pendiente para cualquier salida especifia.
Parametros:
y - Es un valor de salida.
Valor de retorno: Regresa la pendiente correspondiente al valor de salida dado.
"""
def dtanh(y):
    return 1.0 - y * y

"""
Clase: crawler
Descripcion: Los objetivos principales de esta clase son:

             * Crear una red neuronal

             Una red neuronal consiste de un conjunto de nodos a los cuales se les llama neuronas y conexiones entre ellas.
             La red neuronal que se presenta se le llama multilayer perceptron (MLP). Este tipo de red consiste en multiples capas
             de neuronas la primer capa consiste en las entradas que recibe la red neuronal en este caso serian las palabras a buscar 
             por un usuario osea una consulta. La ultima capa consiste en obtener una lista con todos los enlaces que contengan las palabras 
             a buscar. Tambien entre la capa de inicio y fin pueden haber multiples capas ha estas capas se les llaman capas ocultas ya que 
             no tienen una interaccion fuera de ellas mismas. Estas capas se encargan de combinar las entradas en este caso palabras. Todos
             los nodos en la capa inicial estan conectados a los nodos de la capa oculta y los nodos de dicha capa estan conectados a los nodos
             de la ultima capa en este caso solo se utiliza una capa oculta. Para obtener una lista con los mejores enlaces se necesita entrenar
             la red neuronal para entrenarla se utiliza un metodo el cual se llama backpropagation.  
"""
class searchnet:
	"""
    Funcion: __init__(self, dbname)
    Descripcion: Esta funcion crea la conexion a la base de datos con el nombre de dbname.
    Parametros:
    self   - Es una referencia a un objeto.
    dbname - Es el nombre de una base de datos.
    Valor de retorno: None
    """
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    """
    Funcion: __del__(self)
    Descripcion: Esta funcion cierra la conexion a la base de datos.
    Parametros:
    self - Es una referencia a un objeto.
    Valor de retorno: None
    """
    def __del__(self):
        self.con.close()

    """
    Funcion: maketables(self)
    Descripcion: Esta funcion crea el esquema para todas las tablas que se usan en la
                 base de datos las cuales dan soporte a la red neuronal.
    Parametros:
    self - Es una referencia a un objeto.
    Valor de retorno: None
    """
    def maketables(self):
        self.con.execute('create table hiddennode(create_key)')
        self.con.execute('create table wordhidden(fromid, toid, strength)')
        self.con.execute('create table hiddenurl(fromid, toid, strength)')
        self.con.commit()

    """
    Funcion: getstrength(self, fromid, toid, layer)
    Descripcion: Esta funcion accesa a la base de datos y determina que tan fuerte es la conexion actual entre dos 
    			 nodos de distintas capas de la red neuronal.
    Parametros:
    self   - Es una referencia a un objeto.
    fromid - Es un id de algun registro de una tabla que da soporte a la red neuronal el cual marca que 
    		 es el origen para determinar la fuerza de la conexion en la red neuronal.
    toid   - Es un id de algun registro de una tabla que da soporte a la red neuronal el cual marca que 
    		 es el destino para determinar la fuerza de la conexion en la red neuronal.
    layer  - Es una capa de la red neuronal.
    Valor de retorno: Regresa un valor por default si no hay conexiones tambien puede regrear un valor por default de 0.2 para 
    				  enlaces de palabras en la capa oculta o un valor por default de 0 para enlaces de la capa oculta a URLS.
    """
    def getstrength(self, fromid, toid, layer):
        if layer == 0: 
        	table = 'wordhidden'
        else: 
        	table = 'hiddenurl'

        res = self.con.execute('select strength from %s where \
                fromid = %d and toid = %d' % (table, fromid,
                    toid)).fetchone()

        if res == None:
            if layer == 0: 
            	return 0.2
            if layer == 1: 
            	return 0

        return res[0]

    """
    Funcion: setstrength(self, fromid, toid, layer, strength)
    Descripcion: Esta funcion accesa a la base de datos y determina si una conexion existe entre dos nodos despues la actualiza 
    			 o la crea con una nueva fuerza.
    Parametros:
    self     - Es una referencia a un objeto.
    fromid   - Es un id de algun registro de una tabla que da soporte a la red neuronal el cual marca que 
    		   es el origen para determinar la fuerza de la conexion en la red neuronal.
    toid   	 - Es un id de algun registro de una tabla que da soporte a la red neuronal el cual marca que 
    		   es el destino para determinar la fuerza de la conexion en la red neuronal.
    layer  	 - Es una capa de la red neuronal.
    strength - Es la fuerza con la cual se actualiza una conexion existente o se crea una conexion con esta fuerza.
    Valor de retorno: None
    """
    def setstrength(self, fromid, toid, layer, strength):
        if layer == 0: 
        	table = 'wordhidden'
        else: 
        	table = 'hiddenurl'

        res = self.con.execute('select rowid from %s where \
                fromid = %d and toid = %d' % (table, fromid,
                    toid)).fetchone()

        if res == None:
            self.con.execute('insert into %s (fromid, toid, strength) \
                    values(%d, %d, %f)' % (table, fromid, toid, strength))
        else:
            rowid = res[0]
            self.con.execute('update %s set strength = %f where \
                    rowid = %d' % (table, strength, rowid))

    """
    Funcion: generatehiddennode(self, wordids, urls)
    Descripcion: Esta funcion crea un nuevo nodo en la capa oculta cada vez que obtenga una combinacion de palabras
    			 que no este en dicha capa.
    Parametros:
    self    - Es una referencia a un objeto.
    wordids - Son los ids donde se encuentran las palabras de una consulta.
    urls  	- Es un conjunto de paginas.
    Valor de retorno: None
    """
    def generatehiddennode(self, wordids, urls):
        if len(wordids) > 3: 
        	return None
        # Comprobar si ya creamos un nodo para este conjunto de palabras
        createkey = '_'.join(sorted([str(wi) for wi in wordids]))
        res = self.con.execute("select rowid from hiddennode where create_key \
                = '%s'" % createkey).fetchone()

        # si no, lo crea
        if res == None:
            cur = self.con.execute("insert into hiddennode(create_key) \
                    values('%s')" % createkey)
            hiddenid = cur.lastrowid

            # poner en algunos pesos por default
            for wordid in wordids:
                self.setstrength(wordid, hiddenid, 0, 1.0 / len(wordids))
            for urlid in urls:
                self.setstrength(hiddenid, urlid, 1, 0.1)
            self.con.commit()

    """
    Funcion: getallhiddenids(self, wordids, urlids)
    Descripcion: Esta funcion encuentra todos los nodos relevantes de la capa oculta que contribuyen a una consulta
    			 en especifico.
    Parametros:
    self    - Es una referencia a un objeto.
    wordids - Son los ids donde se encuentran las palabras de una consulta.
    urlids  - Es un conjunto de ids de paginas.
    Valor de retorno: Regresa los nodos relevantes de la capa oculta que contribuyen a una consulta en especifico.
    """
    def getallhiddenids(self, wordids, urlids):
        l1 = {}
        for wordid in wordids:
            cur = self.con.execute('select toid from wordhidden where \
                    fromid = %d' % wordid)
            for row in cur: 
            	l1[row[0]] = 1
        for urlid in urlids:
            cur = self.con.execute('select fromid from hiddenurl where \
                    toid = %d' % urlid)
            for row in cur: 
            	l1[row[0]] = 1

        return l1.keys()

    """
    Funcion: setupnework(self, wordids, urlids)
    Descripcion: Esta funcion actualiza la red neuronal con los pesos actuales que se encuentran en la base de datos.
    Parametros:
    self    - Es una referencia a un objeto.
    wordids - Son los ids donde se encuentran las palabras de una consulta.
    urlids  - Es un conjunto de ids de paginas.
    Valor de retorno: None
    """
    def setupnework(self, wordids, urlids):
        # Listas de valores
        self.wordids = wordids
        self.hiddenids = self.getallhiddenids(wordids, urlids)
        self.urlids = urlids

        # Salidas del nodo
        self.ai = [1.0] * len(self.wordids)
        self.ah = [1.0] * len(self.hiddenids)
        self.ao = [1.0] * len(self.urlids)

        # Crear matriz de pesos
        self.wi = [[self.getstrength(wordid, hiddenid, 0)
            for hiddenid in self.hiddenids]
            for wordid in self.wordids]
        self.wo = [[self.getstrength(hiddenid, urlid, 1)
            for urlid in self.urlids]
            for hiddenid in self.hiddenids]

    """
    Funcion: feedforward(self)
    Descripcion: Esta funcion recibe una lista de entradas esta lista la mete a la red neuronal y regresa la salida 
    de todos los nodos en la ultima capa.
    Parametros:
    self    - Es una referencia a un objeto.
    Valor de retorno: Regresa la salida de todos los nodos de entrada en la ultima capa.
    """
    def feedforward(self):
        # Las unicas entradas son las palabras de la consulta
        for i in range(len(self.wordids)):
            self.ai[i] = 1.0

        # Activaciones ocultas
        for j in range(len(self.hiddenids)):
            sum = 0.0
            for i in range(len(self.wordids)):
                sum = sum + self.ai[i] * self.wi[i][j]
            self.ah[j] = tanh(sum)

        # Activaciones de salida
        for k in range(len(self.urlids)):
            sum = 0.0
            for j in range(len(self.hiddenids)):
                sum = sum + self.ah[j] * self.wo[j][k]
            self.ao[k] = tanh(sum)

        return self.ao[:]

    """
    Funcion: getresult(self, wordids, urlids)
    Descripcion: Esta funcion establece la red y usa feedforward para obtener las salidas de un conjunto de palabras
    			 y enlaces.
    Parametros:
    self    - Es una referencia a un objeto.
    wordids - Son los ids donde se encuentran las palabras de una consulta.
    urlids  - Es un conjunto de ids de paginas.
    Valor de retorno: Regresa la salidas de un conjunto de palabras y enlaces que se obtienen con el algoritmo de feedforward.
    """
    def getresult(self, wordids, urlids):
        self.setupnework(wordids, urlids)
        return self.feedforward()

    """
    Funcion: backPropagate(self, targets, N=0.5)
    Descripcion: Esta funcion realiza el algoritmo de backpropagation el cual sigue los siguientes pasos:
    			 Para cada nodo en la capa de salida:
    			 1. Calcula la diferencia entre la salida del nodo actual y la salida que deberia ser.
    			 2. Usa la funcion dtanh para determinar el numero de nodos de entrada que se deben cambiar.
    			 3. Cambia la fuerza de cada enlace entrante en proporcion con la fuerzas actuales de varios enlaces.
    			 Para cada nodo en la capa oculta:
    			 1. Cambia la salida de el nodo por la suma de fuerzas de cada enlace de salida multiplicandolo por la 
    			 	cantidad de su nodo destino al que tiene que cambiar.
    			 2. Usa la funcion dtanh para determinar el numero de nodos de entrada que se deben cambiar.
    			 3. Cambia la fuerza de cada enlace entrante en proporcion con la fuerzas actuales de varios enlaces.
    			 EN pocas palabras el algoritmo calcula todos los errores en su transcurso y ajusta los pesos.
    Parametros:
    self    - Es una referencia a un objeto.
    targets - Es una lista que contiene unos y ceros lo cual significa que si es uno un usuario hizo click en ese enlace y
    		  si es cero caso contrario el usuario no hizo click en el enlace.
    N   	- Es una constante la cual sirve para obtener los nuevos pesos.
    Valor de retorno: None
    """
    def backPropagate(self, targets, N=0.5):
        # Calcular los errores para la salida
        output_deltas = [0.0] * len(self.urlids)
        for k in range(len(self.urlids)):
            error = targets[k] - self.ao[k]
            output_deltas[k] = dtanh(self.ao[k]) * error

        # Calcular los errores de la capa oculta
        hidden_deltas = [0.0] * len(self.hiddenids)
        for j in range(len(self.hiddenids)):
            error = 0.0
            for k in range(len(self.urlids)):
                error = error + output_deltas[k] * self.wo[j][k]
            hidden_deltas[j] = dtanh(self.ah[j]) * error

        # Actualizar los pesos de salida
        for j in range(len(self.hiddenids)):
            for k in range(len(self.urlids)):
                change = output_deltas[k] * self.ah[j]
                self.wo[j][k] = self.wo[j][k] + N * change

        # Actualizar los pesos de entrada
        for i in range(len(self.wordids)):
            for j in range(len(self.hiddenids)):
                change = hidden_deltas[j] * self.ai[i]
                self.wi[i][j] = self.wi[i][j] + N * change

    """
    Funcion: trainquery(self, wordids, urlids, selectedurl)
    Descripcion: Esta funcion establece la red, ejecuta feedforward y backpropagation.
    Parametros:
    self    	- Es una referencia a un objeto.
    wordids 	- Son los ids donde se encuentran las palabras de una consulta.
    urlids 	 	- Es un conjunto de ids de paginas.
    selectedurl - Es el id de una pagina seleccionada.
    Valor de retorno: None
    """
    def trainquery(self, wordids, urlids, selectedurl):
        # Generar un nodo oculto si es necesario
        self. generatehiddennode(wordids, urlids)

        self.setupnework(wordids, urlids)
        self.feedforward()
        targets = [0.0] * len(urlids)
        targets[urlids.index(selectedurl)] = 1.0
        self.backPropagate(targets)
        self.updatedatabase()

    """
    Funcion: updatedatabase(self)
    Descripcion: Esta funcion actualiza la base de datos con los nuevos pesos obtenidos.
    Parametros:
    self - Es una referencia a un objeto.
    Valor de retorno: None
    """
    def updatedatabase(self):
        # Establecer los valores en la base de datos
        for i in range(len(self.wordids)):
            for j in range(len(self.hiddenids)):
                self.setstrength(self.wordids[i], self.hiddenids[j],
                        0, self.wi[i][j])

        for j in range(len(self.hiddenids)):
            for k in range(len(self.urlids)):
                self.setstrength(self.hiddenids[j], self.urlids[k], 1,
                        self.wo[j][k])

        self.con.commit()

def main():
    print "Ejemplos que aparecen en la red neuronal"

    """
    print "Ejemplo 1"
    import nn
    mynet = nn.searchnet('nn.db')
    mynet.maketables()
    wWorld, wRiver, wBank = 101, 102, 103
    uWorldBank, uRiver, uEarth = 201, 202, 203
    mynet.generatehiddennode([wWorld, wBank], [uWorldBank, uRiver, uEarth])
    for c in mynet.con.execute('select * from wordhidden'): print c
    for c in mynet.con.execute('select * from hiddenurl'): print c
    """

    """
    print "Ejemplo 2"
    import nn
    mynet = nn.searchnet('nn.db')
    wWorld, wRiver, wBank = 101, 102, 103
    uWorldBank, uRiver, uEarth = 201, 202, 203
    mynet.getresult([wWorld, wBank], [uWorldBank, uRiver, uEarth])
    """

    """
    print "Ejemplo 3"
    import nn
    mynet = nn.searchnet('nn.db')
    wWorld, wRiver, wBank = 101, 102, 103
    uWorldBank, uRiver, uEarth = 201, 202, 203
    mynet.trainquery([wWorld, wBank], [uWorldBank, uRiver, uEarth], uWorldBank)
    mynet.getresult([wWorld, wBank], [uWorldBank, uRiver, uEarth])
    """

    """
    print "Ejemplo 4"
    import nn
    mynet = nn.searchnet('nn.db')
    wWorld, wRiver, wBank = 101, 102, 103
    uWorldBank, uRiver, uEarth = 201, 202, 203
    allurls = [uWorldBank, uRiver, uEarth]
    for i in range(30):
    	mynet.trainquery([wWorld, wBank], allurls, uWorldBank)
    	mynet.trainquery([wRiver, wBank], allurls, uRiver)
    	mynet.trainquery([wWorld], allurls, uEarth)
    mynet.getresult([wWorld, wBank], allurls)
    mynet.getresult([wRiver, wBank], allurls)
    mynet.getresult([wBank], allurls)
    """

if __name__ == "__main__":
    main()