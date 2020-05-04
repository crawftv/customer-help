import falcon
import sqlite3
import json

from falcon_jinja2 import FalconTemplate

falcon_template = FalconTemplate()

class AssignLabel(object):
    def on_post(self, req, resp):
        resp.status = falcon.HTTP_200
        #with sqlite3.connect("hand-labeled-data.db"):

        resp.body = json.dumps(req)

class Home(object):
    @falcon_template.render('index.html')
    def on_get(self, req, resp):
        resp.context = {'framework': 'Falcon'}

app = falcon.API()

home = Home()
app.add_route('/', home)
