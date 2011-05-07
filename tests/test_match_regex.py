# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException


def clear():
    db = KyotoTycoon()
    db = db.open()
    db.clear()
    db.close()

@raises(IOError)
def test_err_regex():
    db = KyotoTycoon()
    ret = db.match_prefix("A")

@with_setup(setup=clear)
def test_match_regex1():
    db = KyotoTycoon()
    db = db.open()
    db.set("ABC", "B")
    db.set("BC", "B")
    db.set("C", "B")
    ret = db.match_regex("A")
    ok_(ret == ["ABC"])
    db.close()

@raises(KTException)
@with_setup(setup=clear)
def test_match_regex_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.set("ABC", "B")
    db.set("BC", "B")
    db.set("C", "B")
    db.match_regex("A")
    ok_(false)

@with_setup(setup=clear)
def test_match_regex2():
    db = KyotoTycoon()
    db = db.open()
    db.set("ABC", "B")
    db.set("AAA", "B")
    db.set("C", "B")
    ret = db.match_regex("A")
    ok_(ret == ["AAA", "ABC"])
    db.close()

@with_setup(setup=clear)
def test_match_regex3():
    db = KyotoTycoon()
    db = db.open()
    db.set("ABC", "B")
    db.set("AAA", "B")
    db.set("C", "B")
    ret = db.match_regex("A$")
    ok_(ret == ["AAA"])
    db.close()

@with_setup(setup=clear)
def test_notfound():
    db = KyotoTycoon()
    db = db.open()
    db.set("BC", "B")
    db.set("C", "B")
    ret = db.match_regex("A")
    ok_(ret == [])
    db.close()

@with_setup(setup=clear)
def test_match_regex_utf8():
    db = KyotoTycoon()
    db = db.open()
    db.set("あいうえお", "B")
    db.set("BC", "B")
    db.set("C", "B")
    ret = db.match_regex("あいう")
    ok_(ret == ["あいうえお"])
    db.close()


