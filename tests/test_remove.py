# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException

def clear():
    db = KyotoTycoon()
    db = db.open()
    db.clear()
    db.close()

@raises(IOError)
def test_err_remove():
    db = KyotoTycoon()
    ret = db.remove("A")

@with_setup(setup=clear)
def test_remove():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.get("A")
    ok_(ret == "B")
    ret = db.remove("A")
    ok_(ret == True)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_remove_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.set("A", "B")
    ret = db.get("A")
    ok_(ret == "B")
    ret = db.remove("A")
    ok_(ret == True)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_remove_utf8():
    db = KyotoTycoon()
    db = db.open()
    db.set("あいうえお", "かきくけこ")
    ret = db.get("あいうえお")
    ok_(ret == "かきくけこ") 
    ret = db.remove("あいうえお")
    ok_(ret == True) 
    ret = db.get("あいうえお")
    ok_(ret == None) 
    db.close()

@with_setup(setup=clear)
def test_remove_notfound():
    db = KyotoTycoon()
    db = db.open()
    ret = db.remove("A"* 10)
    ok_(ret == False)
    db.close()

@with_setup(setup=clear)
def test_remove_loop():
    db = KyotoTycoon()
    db = db.open()

    for i in xrange(100):
        db.set("A", "B")
        ret = db.get("A")
        ok_(ret == "B")
        ret = db.remove("A")
        ok_(ret == True)
        ret = db.get("A")
        ok_(ret == None)
    db.close()


