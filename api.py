import falcon
import sqlite3
import json

from falcon_jinja2 import FalconTemplate


falcon_template = FalconTemplate()
app = falcon.API()


class Home(object):
    @falcon_template.render("index.html")
    def on_get(self, req, resp):
        resp.context = {"framework": "Falcon"}


home = Home()
app.add_route("/", home)


class Search(object):
    @falcon_template.render("search_result.html")
    def on_post(self, req, resp):
        resp.context = {}
        search_term = req.stream.read().decode("UTF-8").split("=")[1]
        resp.context["search_term"] = search_term
        resp.status = falcon.HTTP_200
        with sqlite3.connect("hand-labeled-data.db") as conn:
            dict_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            conn.row_factory = dict_factory
            query = (
                "SELECT text,rowid,rating FROM data WHERE data MATCH 'terrible' LIMIT 1"
            )
            search_result = conn.execute(query).fetchone()
            resp.context["search_result"] = search_result


search = Search()
app.add_route("/search", search)


class SubmitRating(object):
    def on_post(self, req, resp, rowid):
        resp.status = falcon.HTTP_200
        req_dict = byte_stream_to_dict(req)
        resp.body = json.dumps(req_dict)


submit = SubmitRating()
app.add_route("/submit_rating/{rowid:int}", submit)


def byte_stream_to_dict(req):
    stream_string = req.stream.read().decode("UTF-8")
    pair_split = stream_string.split("&")
    list_of_pairs = [ii.split("=") for ii in pair_split]
    req_dict = {ii[0]: ii[1] for ii in list_of_pairs}
    return req_dict
