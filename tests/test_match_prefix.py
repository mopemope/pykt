# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException


def clear():
    db = KyotoTycoon()
    db = db.open()
    db.clear()
    db.close()

@raises(IOError)
def test_err_prefix():
    db = KyotoTycoon()
    ret = db.match_prefix("A")

@raises(KTException)
@with_setup(setup=clear)
def test_match_prefix_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.set("ABC", "B")
    db.set("BC", "B")
    db.set("C", "B")
    ret = db.match_prefix("A")
    ok_(ret == ["ABC"])
    db.close()

@with_setup(setup=clear)
def test_match_prefix1():
    db = KyotoTycoon()
    db = db.open()
    db.set("ABC", "B")
    db.set("BC", "B")
    db.set("C", "B")
    ret = db.match_prefix("A")
    ok_(ret == ["ABC"])
    db.close()

@with_setup(setup=clear)
def test_match_prefix2():
    db = KyotoTycoon()
    db = db.open()
    db.set("ABC", "B")
    db.set("AAA", "B")
    db.set("C", "B")
    ret = db.match_prefix("A")
    ok_(ret == ["AAA", "ABC"])
    db.close()

@with_setup(setup=clear)
def test_prefix_notfound():
    db = KyotoTycoon()
    db = db.open()
    db.set("BC", "B")
    db.set("C", "B")
    ret = db.match_prefix("A")
    ok_(ret == [])
    db.close()

@with_setup(setup=clear)
def test_match_prefix_utf8():
    db = KyotoTycoon()
    db = db.open()
    db.set("あいうえお", "B")
    db.set("BC", "B")
    db.set("C", "B")
    ret = db.match_prefix("あいう")
    ok_(ret == ["あいうえお"])
    db.close()


