# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException
import time

def clear():
    db = KyotoTycoon()
    db = db.open()
    db.clear()
    db.close()

@raises(IOError)
def test_err_set():
    db = KyotoTycoon()
    db.set("A", "B")

@with_setup(setup=clear)
def test_set():
    db = KyotoTycoon()
    db = db.open()
    ret = db.set("A", "B")
    ok_(ret)
    ret = db.get("A")
    ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_set_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    ret = db.set("A", "B")
    ok_(ret)
    ret = db.get("A")
    ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_set_utf8():
    db = KyotoTycoon()
    db = db.open()
    ret = db.set("あいうえお", "かきくけこ")
    ok_(ret) 
    ret = db.get("あいうえお")
    ok_(ret == "かきくけこ")
    db.close()

@with_setup(setup=clear)
def test_set_large_key():
    db = KyotoTycoon()
    db = db.open()
    ret = db.set("L" * 1024 * 4, "L")
    ok_(ret) 
    ret = db.get("L" * 1024 * 4)
    ok_(ret == "L")
    db.close()

@with_setup(setup=clear)
def test_set_large():
    db = KyotoTycoon()
    db = db.open()
    ret = db.set("L", "L" * 1024 * 1024 * 1)
    ok_(ret) 
    ret = db.get("L")
    ok_(ret == "L" * 1024 * 1024 * 1)
    db.close()

@with_setup(setup=clear)
def test_mapping_protocol():
    db = KyotoTycoon()
    db = db.open()
    ret = db["M"]  = "MAP"
    ok_(ret) 
    ret = db.get("M")
    ok_(ret == "MAP")
    db.close()

@with_setup(setup=clear)
def test_set_loop():
    db = KyotoTycoon()
    db = db.open()
    for i in xrange(100):
        ret = db.set("A", "B")
        ok_(ret)
        ret = db.get("A")
        ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_set_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.set("A", "B", expire=2)
    ok_(ret)
    time.sleep(3)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_set_expire_not_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.set("A", "B", expire=3)
    ok_(ret)
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == "B")
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == None)
    db.close()
