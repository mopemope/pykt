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
def test_err_append():
    db = KyotoTycoon()
    db.append("A", "B")

@with_setup(setup=clear)
def test_append():
    db = KyotoTycoon()
    db = db.open()
    ret = db.append("A", "B")
    ok_(ret)
    ret = db.get("A")
    ok_(ret == "B")
    db.close()

@raises(KTException)
@with_setup(setup=clear)
def test_append_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    ret = db.append("A", "B")
    ok_(False)

@with_setup(setup=clear)
def test_append_utf8():
    db = KyotoTycoon()
    db = db.open()
    ret = db.append("あいうえお", "かきくけこ")
    ok_(ret) 
    ret = db.get("あいうえお")
    ok_(ret == "かきくけこ")
    db.close()

@with_setup(setup=clear)
def test_append_large_key():
    db = KyotoTycoon()
    db = db.open()
    ret = db.append("L" * 1024 * 4, "L")
    ok_(ret) 
    ret = db.get("L" * 1024 * 4)
    ok_(ret == "L")
    db.close()

@with_setup(setup=clear)
def test_append_large():
    db = KyotoTycoon()
    db = db.open()
    ret = db.append("L", "L" * 1024 * 1024 * 1)
    ok_(ret) 
    ret = db.get("L")
    ok_(ret == "L" * 1024 * 1024 * 1)
    db.close()

@with_setup(setup=clear)
def test_duble():
    db = KyotoTycoon()
    db = db.open()
    ret = db.append("A", "B")
    ok_(ret == True)
    ok_(db.get("A") == "B")
    ret = db.append("A", "B")
    ok_(ret == True)
    ok_(db.get("A") == "BB")


@with_setup(setup=clear)
def test_loop():
    db = KyotoTycoon()
    db = db.open()
    for i in xrange(100):
        ret = db.append("A", "B")
        ok_(ret)
        ret = db.get("A")
        ok_(ret == "B" * (i+1))
    db.close()

@with_setup(setup=clear)
def test_append_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.append("A", "B", expire=2)
    ok_(ret)
    time.sleep(3)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_append_expire_not_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.append("A", "B", expire=3)
    ok_(ret)
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == "B")
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == None)
    db.close()
