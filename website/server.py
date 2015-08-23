# -*- coding: utf-8 -*-
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, StaticFileHandler
import os.path

foofle_data = {"query" : "",
               "results" : []}

def update_data(query):
    foofle_data["query"] = query
    foofle_data["results"] = [[10, "http://" + query]]

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
    return Application(
        [
            url(r"/", MainHandler, {"data" : foofle_data}),
            url(r"/(.*)", StaticFileHandler, {"path":"./ui"})
        ],
        template_path = os.path.join(os.path.dirname(__file__), "ui"))

def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()
