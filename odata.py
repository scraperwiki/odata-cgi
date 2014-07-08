#! /usr/bin/env python3.3

from __future__ import unicode_literals


import logging
import os
import sys

from datetime import datetime, date
from gzip import GzipFile
from logging import FileHandler, getLogger
from os.path import join
from sys import stdout
from time import time


from flask import Flask, Response, render_template, request
from wsgiref.handlers import CGIHandler

from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select

log = getLogger('odata')
HOME = os.environ.get("HOME", "/home")

# Get the "root" url path, because
# Flask isn't running at the domain root
request_path = os.environ.get('PATH_INFO', '/toolid/token/cgi-bin/odata')
api_path = '/'.join(request_path.split('/')[0:5])
api_server = os.environ.get('HTTP_HOST', 'server.scraperwiki.com')


TEMPLATE_START = """\
<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<feed xml:base="https://{api_server}{api_path}"\
 xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"\
 xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"\
 xmlns="http://www.w3.org/2005/Atom">
  <title type="text">{collection}</title>
  <id>https://{api_server}{api_path}/{collection}</id>
  <updated>{update_time}</updated>
  <m:count>{total_count}</m:count>
  <link rel="self" title="{collection}"\
 href="https://{api_server}{api_path}/{collection}" />
"""

TEMPLATE_END = """\
</feed>
"""

NEXT = """<link rel="next" href="https://{api_server}{api_path}/\
{collection}{next_query_string}" />"""

ENTRY_START = """\
  <entry>
    <id>https://{api_server}{api_path}/{collection}({rowid})</id>
    <title type="text"></title>
    <updated>{update_time}</updated>
    <author><name /></author>
    <category term="scraperwiki.com.sql"\
 scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
    <content type="application/xml">
      <m:properties>
      """

ENTRY_END = """\
      </m:properties>
    </content>
  </entry>"""

CELL_NULL = """<d:{safe_name} m:type="{type}" m:null="true" />"""

CELL_VALUE = """<d:{safe_name} m:type="{type}">{value}</d:{safe_name}>"""


TYPEMAP = {
    type(None): "Edm.Null",
    bool: "Edm.Boolean",
    float: "Edm.Double",
    int: "Edm.Int64",
    # long: "Edm.Int64",
    datetime: "Edm.DateTimeOffset",
    date: "Edm.Date",
    str: "Edm.String",
    # unicode: "Edm.String",
}


from xml.sax.saxutils import escape

# Tableau only takes date/times in OData which have milliseconds and
# a Z at the end (no other timezone).
# XXX not clear SQLAlchemy is returning the correctly timezoned data.
# This works for Twitter data as in UTC, needs testing more on other
# timezone data. For now, better than whole thing being broken.
def format_date_for_tableau(d):
    return d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# I think lowercase "true" and "false" may be OData standard anyway,
# certainly it is all Tableau accepts.
def format_bool_for_tableau(value):
    if value:
        return "true"
    else:
        return "false"

def make_cells(cells):
    result = []
    for key, value in cells.items():
        if key == "rowid":
            continue

        odata_type = TYPEMAP[type(value)]

        form = CELL_VALUE
        if value is None:
            form = CELL_NULL
        else:
            form = CELL_VALUE

        if isinstance(value, str):
            value = escape(value)

        if type(value) == datetime:
            value = format_date_for_tableau(value)

        if type(value) == bool:
            value = format_bool_for_tableau(value)

        yield form.format(
            safe_name=key,
            type=odata_type,
            value=value,
        )


def render(api_server, api_path, collection, entries,
           total_count, offset, limit, skip_token):
    tmpl = dict(
        api_server=api_server,
        api_path=api_path,
        collection=collection,
        update_time=format_date_for_tableau(datetime.now()),
        total_count=total_count,
    )

    yield TEMPLATE_START.format(**tmpl)

    start = time()

    i = offset
    next_index = i + 1
    for i, entry in enumerate(entries, start=offset):

        yield ENTRY_START.format(
            rowid=entry['rowid'],
            **tmpl
        )
        yield from make_cells(entry)
        yield ENTRY_END

        next_index = i + 1

        if next_index % 1000 == 0:
            elapsed = time() - start
            if elapsed > 4:
                break

    if next_index < total_count:
        # There are more records
        next_query_string = '?$top={}&amp;$skip={}'.format(limit, next_index)

        if skip_token:
            FMT = '?$top={}&amp;$skiptoken={}'
            next_query_string = FMT.format(limit, next_index)

        yield NEXT.format(next_query_string=next_query_string, **tmpl)

    yield TEMPLATE_END


def build_odata(table, collection, offset=0, limit=100000, skip_token=None):
    records = select(["rowid as rowid", table])
    record_count_total = records.count().scalar()

    records = records.offset(offset).limit(limit)
    records = records.execute()

    yield from render(api_server, api_path, collection, records,
                      record_count_total, offset, limit, skip_token)


def main():
    engine = create_engine('sqlite:///scraperwiki.sqlite')
    m = MetaData(engine)
    m.reflect()

    collection = "tweets"
    table = m.tables[collection]

    result = build_odata(table, collection)

    compress = "gzip" in os.environ.get("HTTP_ACCEPT_ENCODING", "")

    print("Status: 200 OK")
    print("Content-Type: application/xml;charset=utf-8")
    if compress:
        print("Content-Encoding: gzip")
    print("")

    if compress:
        with GzipFile(fileobj=stdout.buffer, mode="w", compresslevel=1) as fd:
            w = fd.write
            for s in result:
                w(s.encode("utf8"))
    else:
        w = stdout.write
        for s in result:
            w(s)

app = Flask(__name__)
# app.debug = True
app.url_map.strict_slashes = False


@app.route(api_path + "/<collection>/")
def show_collection(collection):
    log.info("show_collection(collection) req args {}".format(request.args))

    engine = create_engine('sqlite:////home/scraperwiki.sqlite')
    m = MetaData(engine)
    m.reflect()

    table = m.tables[collection]

    limit = int(request.args.get('$top', 100000))
    offset = int(request.args.get('$skip', 0))
    if request.args.get('$skiptoken'):
        limit = 100000
        offset = int(request.args.get('$skiptoken'))

    log.info("offset, limit = {} {}".format(offset, limit))
    result = build_odata(table, collection, offset, limit,
                         request.args.get('$skiptoken'))

    return Response(result, mimetype='application/xml;charset=utf-8')

if __name__ == "__main__":
    # Are we running as CGI, or shell script?
    IS_CGI = os.environ.get("GATEWAY_INTERFACE") == "CGI/1.1"
    if IS_CGI:
        h = FileHandler(join(HOME, "http", "log.txt"))
        log.addHandler(h)
        app.logger.addHandler(h)
        log.setLevel(logging.INFO)
        CGIHandler().run(app)
    else:
        main()
