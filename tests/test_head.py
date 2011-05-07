# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException


def clear():
    db = KyotoTycoon()
    db = db.open()
    db.clear()
    db.close()

@raises(IOError)
def test_err_head():
    db = KyotoTycoon()
    ret = db.head("A")

@with_setup(setup=clear)
def test_head():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.head("A")
    ok_(ret  == True)
    db.close()

@with_setup(setup=clear)
def test_head_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.set("A", "B")
    ret = db.head("A")
    ok_(ret  == True)
    db.close()

@with_setup(setup=clear)
def test_head_utf8():
    db = KyotoTycoon()
    db = db.open()
    db.set("あいうえお", "かきくけこ")
    ret = db.head("あいうえお")
    ok_(ret == True)
    db.close()

@with_setup(setup=clear)
def test_head_notfound():
    db = KyotoTycoon()
    db = db.open()
    ret = db.head("A"* 10)
    ok_(ret == False)
    db.close()

@with_setup(setup=clear)
def test_head_loop():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    for i in xrange(100):
        ret = db.head("A")
        ok_(ret == True)
    db.close()

