from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, StaticFileHandler
import os.path

foofle_query = ""
foofle_results = [[10, "http://www.uson.mx"],
                  [9, "https://www.wikipedia.org"],
                  [8, "https://www.google.com.mx"]]
#foofle_results = []

class MainHandler(RequestHandler):
    def initialize(self, database):
        self.database = database
        self.foofle2_query = "Hola"
        self.foofle2_results = []

    def get(self):
        #self.render("index.html", query = foofle_query, results = foofle_results)
        self.render("index.html", query = self.foofle2_query, results = self.foofle2_results)

    def post(self):
        print "In POST method"
        print self.get_argument("input-query")


def make_app():
    return Application(
        [
            url(r"/", MainHandler, dict(database={})),
            url(r"/(.*)", StaticFileHandler, {"path":"./ui"})
        ],
        template_path = os.path.join(os.path.dirname(__file__), "ui"))

def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()
