#! /usr/bin/env python3.3

import datetime

from nose.tools import assert_in

import odata


def test_make_cells_empty_arg():
    fixture = {}
    cells = odata.make_cells(fixture)
    assert len(list(cells)) == 0


def test_make_cells_n():
    fixture = {
        "foo1": "bar",
        "foo2": "bar",
        "foo3": "bar",
    }
    cells = odata.make_cells(fixture)
    assert len(list(cells)) == 3


def test_make_cells_datetime_present():
    fixture = {
        "foo3": datetime.datetime.now(),
    }
    cells = odata.make_cells(fixture)
    (result,) = cells
    assert_in('m:type="Edm.DateTimeOffset"', result)

def test_make_cells_datetime_natural():
    fixture = {
        "humbug": datetime.datetime(2008, 11, 24, 15, 11, 49),
    }
    cells = odata.make_cells(fixture)
    (result,) = cells
    # This exact format - with milliseconds, and Z as timezone - is only one
    # Tableau 8.2 accepts.
    assert_in('<d:humbug m:type="Edm.DateTimeOffset">2008-11-24T15:11:49.000000Z</d:humbug>', result)

