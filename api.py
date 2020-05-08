import falcon
import sqlite3
import json

from falcon_jinja2 import FalconTemplate

labels = ["positive", "negative", "undecidable"]
falcon_template = FalconTemplate()
app = falcon.API()


class Home(object):
    @falcon_template.render("index.html")
    def on_get(self, req, resp):
        with sqlite3.connect("hand-labeled-data.db") as conn:
            dict_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            conn.row_factory = dict_factory
            query = (
                "SELECT rating, COUNT(rating) as num FROM labeled_data group by rating"
            )
            result = conn.execute(query).fetchall()
        resp.status = falcon.HTTP_200
        resp.context = {"labeled_data": list(result)}


home = Home()
app.add_route("/", home)


class Search(object):
    @falcon_template.render("search_result.html")
    def on_post(self, req, resp, search_term=None):
        if search_term is None:
            search_term = req.stream.read().decode("UTF-8").split("=")[1]
        resp.context["search_term"] = search_term
        resp.status = falcon.HTTP_200
        with sqlite3.connect("hand-labeled-data.db") as conn:
            dict_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            conn.row_factory = dict_factory
            query = (
<<<<<<< HEAD
                "SELECT text,rowid,rating FROM data WHERE data MATCH (?) LIMIT 1"
=======
                "SELECT text,rowid,rating FROM data WHERE data MATCH (?) ORDER BY RANDOM() LIMIT 1"
>>>>>>> push-large-data
            )
            search_result = conn.execute(query,(search_term,)).fetchone()
        resp.context["search_result"] = search_result
        resp.context["labels"] = labels
    
    @falcon_template.render("search_result.html")
    def on_get(self,req,resp,search_term=None):
        with sqlite3.connect("hand-labeled-data.db") as conn:
            dict_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            conn.row_factory = dict_factory
            query = (
<<<<<<< HEAD
                "SELECT text,rowid,rating FROM data WHERE data MATCH (?) LIMIT 1"
=======
                "SELECT text,rowid,rating FROM data WHERE data MATCH (?) ORDER BY RANDOM() LIMIT 1"
>>>>>>> push-large-data
            )
            search_result = conn.execute(query,(search_term,)).fetchone()
        resp.context["search_term"] = search_term
        resp.context["search_result"] = search_result
        resp.context["labels"] = labels
        



search = Search()
app.add_route("/search", search)
app.add_route("/search/{search_term}", search)


class SubmitRating(object):
    def on_post(self, req, resp, rowid, search_term):
        resp.status = falcon.HTTP_302
        req_dict = byte_stream_to_dict(req)
        resp.body = json.dumps(req_dict)
        with sqlite3.connect("hand-labeled-data.db") as conn:
            dict_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            conn.row_factory = dict_factory
            extract = "SELECT text FROM data WHERE rowid = (?)"
            data = conn.execute(extract, (int(rowid),)).fetchone()
            insert = "INSERT INTO labeled_data VALUES (?,?,?)"
            conn.execute(insert, (rowid, data["text"], req_dict["label"]))
            delete = "DELETE FROM data WHERE rowid = (?)"
            conn.execute(delete, (int(rowid),))
        resp.location = f"/search/{search_term}"


submit = SubmitRating()
app.add_route("/submit_rating/{rowid:int}/{search_term}", submit)


def byte_stream_to_dict(req):
    stream_string = req.stream.read().decode("UTF-8")
    pair_split = stream_string.split("&")
    list_of_pairs = [ii.split("=") for ii in pair_split]
    req_dict = {ii[0]: ii[1] for ii in list_of_pairs}
    return req_dict
