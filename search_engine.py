import urllib2
from bs4 import BeautifulSoup as BF
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite
from re import compile


ignore_words = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])


class crawler:

    def __init__(self, db_name):
        self.con = sqlite.connect(db_name)

    def __del__(self):
        self.con.close()

    def db_commit(self):
        self.con.commit()

    def get_entry_id(self, table, field, value, create_new=True):
        cur = self.con.execute(
            "select rowid from %s where %s = '%s'" % (table, field, value))
        res = cur.fetchone()
        if res is None:
            cur = self.con.execute(
                "insert into %s(%s) values('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    def add_to_index(self, url, soup):
        if self.is_indexed(url):
            return
        print "Indexing", url

        # Obtenemos las palabras de manera individual
        text = self.get_text_only(soup)
        words = self.separate_words(text)

        # Obtener el id de la url
        url_id = self.get_entry_id('urllist', 'url', url)

        # linkear cada palabra a la url
        for i in range(len(words)):
            word = words[i]
            if word in ignore_words:
                continue
            word_id = self.get_entry_id('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, \
            location) values(%d, %d, %d)" % (url_id, word_id, i))

    def get_text_only(self, soup):
        v = soup.string
        if v is None:
            c = soup.contents
            result_text = ''
            for t in c:
                sub_text = self.get_text_only(t)
                result_text += sub_text + '\n'
            return result_text
        else:
            return v.strip()

    def separate_words(self, text):
        # return text.split()
        splitter = compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    def is_indexed(self, url):
        u = self.con.execute("select rowid from \
            urllist where url = '%s'" % url).fetchone()
        if u is not None:
            # Checa si ya ha sido "aranado"
            v = self.con.execute("select * from \
                wordlocation where urlid = %d" % u[0])\
                .fetchone()
            if v is not None:
                return True
        return False

    def add_link_ref(self, url_From, url_To, link_Text):
        fromid = self.getentryid('urllist', 'url', urlfrom)
        toid = self.getentryid('urllist', 'url', urlto)
        if fromid != toid:
            cur = self.con.execute('insert into link (fromid, toid) values (%d, %d)'% (fromid, toid))

    def crawl(self, pages, depth=2):

        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open page >> ", page
                    continue
                soup = BF(c.read())
                self.add_to_index(page, soup)

                links = soup('a')
                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split("#")[0]
                        if url[0:4] == 'http' and not self.is_indexed(url):
                            newpages.add(url)
                        link_text = self.get_text_only(link)
                        self.add_link_ref(page, url, link_text)
                self.db_commit()
            pages = newpages

    def create_index_tables(self):
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
        self.db_commit()


class searcher(object):
    """docstring for saercher"""
    def __init__(self, db_name):
        self.con = sqlite.connect(db_name)

    def __del__(self):
        self.con.close()

    def get_match_rows(self, q):
        # Cadenas para construir la consulta
        field_list = 'w0.urlid'
        table_list = ''
        clause_list = ''
        word_ids = []

        # Parte las palabras por espacios
        words = q.split(' ')
        table_number = 0

        print words
        for word in words:
            # Obtener el ID de la palabra
            word_row = self.con.execute(
                "select rowid from wordlist where word = '%s'" % word
            ).fetchone()
            if word_row is not None:
                word_id = word_row[0]
                word_ids.append(word_id)
                if table_number > 0:
                    table_list += ','
                    clause_list += ' and '
                    clause_list += 'w%d.urlid = w%d.urlid and ' % \
                        (tablenumber - 1, tablenumber)
                field_list += ',w%d.location' % table_number
                table_list += 'wordlocation w%d' % table_number
                clause_list += 'w%d.wordid = %d' % (table_number, word_id)
                table_number += 1
        try:
            # Crear la consulta a partir de las partes separadas
            full_query = 'select %s from %s where %s' % \
                (field_list, table_list, clause_list)
            cur = self.con.execute(full_query)
            rows = [row for row in cur]
            return rows, wordids
        except:
            print "Error en la base de datos o frase no encontrada"

    def get_scored_list(self, rows, word_ids):
        total_score = dict([(row[0], 0) for row in rows])

        # Aqui es donde despues pondras los pesos de la funcion score
        weights = [(1.0, self.frequency_scores(rows))]

        for (weight, scores) in weights:
            for url in total_scores:
                total_scores[url] += weight*scores[url]

        return total_scores

    def get_url_name(self, id):
        return self.con.execute("select url from urllist \
                                where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, word_ids = self.get_match_rows(q)
        print rows, word_ids
        scores = self.get_scored_list(rows, word_ids)
        ranked_scores = \
            sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, url_id) in ranked_scores[0:10]:
            print '%f\t%s' % (score, self.get_url_name(urlid))

    def normalize_scores(self, scores, small_is_better=0):
        v_small = 0.000001  # Con esto evitamos la division entre 0
        if small_is_better:
            min_score = min(scores.values())
            return dict([(u, float(min_score) / max(v_small)) for (u, l) in scores.items()])
        else:
            max_score = max(scores.values())
            if max_score == 0:
                max_score = v_small
            return dict([(u, float(c) / max_score) for (u, c) in scores.items()])

    def frequency_scores(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows:
            counts[row[0]] += 1
        return self.normalize_scores(counts)

    def location_score(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc

        return self.normalize_scores(locations, small_is_better=1)

    def distances_score(self, rows):
        # Si hay una sola palabra, todos ganan
        if len(rows[0]) <= 2:
            return dict([(row[0], 1.0) for row in rows])

        # Inicializa el diccionario con valores grandes
        min_distance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs([i] - row[i - 1]) for i in range(2, len(row))])
            if dist < min_distance[row[0]]:
                min_distance[row[0]] = dist

        return self.normalize_scores(min_distance, small_is_better=1)
