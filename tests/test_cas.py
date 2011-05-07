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
def test_err_cas():
    db = KyotoTycoon()
    db.cas("A")

@with_setup(setup=clear)
def test_cas():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.cas("A", oval="B", nval="C")
    ok_(ret == True)
    ret = db.get("A")
    ok_(ret == "C")
    db.close()

@raises(KTException)
@with_setup(setup=clear)
def test_cas_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.set("A", "B")
    db.cas("A", oval="B", nval="C")
    ok_(False)

@with_setup(setup=clear)
@raises(KTException)
def test_cas_fail():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.cas("A", oval="C", nval="C")

@with_setup(setup=clear)
@raises(KTException)
def test_cas_few_param1():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    db.cas("A", nval="C")

@with_setup(setup=clear)
def test_cas_few_param2():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.cas("A", oval="B")
    ok_(ret == True)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_cas_utf8():
    db = KyotoTycoon()
    db = db.open()
    db.set("あいうえお", "かきくけこ")
    ret = db.cas("あいうえお", oval="かきくけこ", nval="さしすせそ")
    ok_(ret == True)
    ret = db.get("あいうえお")
    ok_(ret == "さしすせそ")
    db.close()

@with_setup(setup=clear)
def test_cas_loop():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "0")
    for i in xrange(100):
        a = str(i)
        b = str(i+1)
        ret = db.cas("A", oval=a, nval=b)
        ok_(ret == True)
        ret = db.get("A")
        ok_(ret == b)
    db.close()

@with_setup(setup=clear)
def test_cas_expire():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.cas("A", oval="B", nval="C", expire=2)
    ok_(ret)
    time.sleep(3)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

@with_setup(setup=clear)
def test_cas_expire_not_expire():
    db = KyotoTycoon()
    db = db.open()
    db.set("A", "B")
    ret = db.cas("A", oval="B", nval="C", expire=2)
    ok_(ret)
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == "C")
    time.sleep(2)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

