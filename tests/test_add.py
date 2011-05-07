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
def test_err_add():
    db = KyotoTycoon()
    db.add("A", "B")

@with_setup(setup=clear)
def test_add():
    db = KyotoTycoon()
    db = db.open()
    ret = db.add("A", "B")
    ok_(ret)
    ret = db.get("A")
    ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_add_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    ret = db.add("A", "B")
    ok_(ret)
    ret = db.get("A")
    ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_add_utf8():
    db = KyotoTycoon()
    db = db.open()
    ret = db.add("あいうえお", "かきくけこ")
    ok_(ret) 
    ret = db.get("あいうえお")
    ok_(ret == "かきくけこ")
    db.close()

@with_setup(setup=clear)
def test_add_large_key():
    db = KyotoTycoon()
    db = db.open()
    ret = db.add("L" * 1024 * 4, "L")
    ok_(ret) 
    ret = db.get("L" * 1024 * 4)
    ok_(ret == "L")
    db.close()

@with_setup(setup=clear)
def test_add_large():
    db = KyotoTycoon()
    db = db.open()
    ret = db.add("L", "L" * 1024 * 1024 * 1)
    ok_(ret) 
    ret = db.get("L")
    ok_(ret == "L" * 1024 * 1024 * 1)
    db.close()

@raises(KTException)
@with_setup(setup=clear)
def test_dup():
    db = KyotoTycoon()
    db = db.open()
    ret = db.add("A", "B")
    ok_(ret == True)
    ok_(db.get("A") == "B")
    ret = db.add("A", "B")


@with_setup(setup=clear)
def test_loop():
    db = KyotoTycoon()
    db = db.open()
    for i in xrange(100):
        k = str(i)
        ret = db.add(k, "B")
        ok_(ret)
        ret = db.get(k)
        ok_(ret == "B")
    db.close()

@with_setup(setup=clear)
def test_add_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.add("A", "B", expire=2)
    ok_(ret)
    time.sleep(3)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_add_expire_not_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.add("A", "B", expire=3)
    ok_(ret)
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == "B")
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == None)
    db.close()
