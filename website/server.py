# -*- coding: utf-8 -*-
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, StaticFileHandler
import os.path
import sys

sys.path.insert(0, os.path.abspath("../collective-intelligence"))

import searchengine as se
searcher = se.searcher("index.db")

foofle_data = {"query" : "",
               "results" : []}

def update_data(query):
    foofle_data["query"] = query
    foofle_data["results"] = searcher.query(query)

class MainHandler(RequestHandler):
    def initialize(self, data):
        self.data = data

    def get(self):
        self.render("index.html", query = self.data["query"], results = self.data["results"])

    def post(self):
        query = self.get_argument("input-query")
        print "La busqueda que se realizara utilizara la cadena '%s' como consulta" % query
        update_data(query)
        self.get()


def make_app():
    settings = {
                # el flag de debug proveé comportamientos útiles para el desarrollo, ejemplo:
                # - autoreload, para reiniciar el servidor automáticamente cuando sea necesario
                # - no se cachean los templates compilados (i.e. no se tiene que reiniciar el servidor para ver cambios)
                # - si sucede una excepciones y no se atrapa, se sirve al cliente en una página de error
                'debug':True,
    }

    return Application(
        [
            url(r"/", MainHandler, {"data" : foofle_data}),
            url(r"/(.*)", StaticFileHandler, {"path":"./ui"})
        ],
        template_path = os.path.join(os.path.dirname(__file__), "ui"), **settings)

def main():
    app = make_app()
    port = 3456
    app.listen(port)
    print "Ahoy pirate! bring your ship to the port " + str(port) + " [url is localhost:" + str(port) + "]"
    IOLoop.current().start()

if __name__ == "__main__":
    main()