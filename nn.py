__author__ = 'JuanManuel'

from math import tanh
from pysqlite2 import dbapi2 as sqlite

class searchnet:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def maketables(self):
        self.con.execute('create table hiddennode(create_key)')
        self.con.execute('create table wordhidden(fromid, toid, strength)')
        self.con.execute('create table hiddenurl(fromid, toid, strength)')
        self.con.commit()

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

if __name__ == "__main__":
    main()