# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException


def clear():
    db = KyotoTycoon()
    db = db.open()
    db.clear()
    db.close()

@raises(IOError)
def test_err_get():
    db = KyotoTycoon()
    ret = db.get("A")

@with_setup(setup=clear)
def test_get_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.set("A", "B")
    ret = db.get("A")
    ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_get():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.get("A")
    ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_get_notfound():
    db = KyotoTycoon()
    db = db.open()
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_get_utf8():
    db = KyotoTycoon()
    db = db.open()
    db.set("あいうえお", "かきくけこ")
    ret = db.get("あいうえお")
    ok_(ret == "かきくけこ")
    db.close()

@with_setup(setup=clear)
def test_get_mapping_protocol():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db["A"]
    ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_get_loop():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    for i in xrange(100):
        ret = db.get("A")
        ok_(ret == "B")
    db.close()


