#! /usr/bin/env python3.3

from datetime import datetime

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


def test_make_cells_datetime():
    fixture = {
        "foo3": datetime.now(),
    }
    cells = odata.make_cells(fixture)
    (result,) = cells
    assert_in('m:type="Edm.DateTime"', result)
