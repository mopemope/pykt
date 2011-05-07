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
def test_err_increment():
    db = KyotoTycoon()
    ret = db.increment("I")

@with_setup(setup=clear)
def test_increment():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment("I")
    ok_(ret == 1)
    ret = db.increment("I")
    ok_(ret == 2)
    db.close()

@raises(KTException)
@with_setup(setup=clear)
def test_increment_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.increment("I")
    ok_(False)

@with_setup(setup=clear)
def test_increment_utf8():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment("インクリメント")
    ok_(ret == 1)
    db.close()

@with_setup(setup=clear)
def test_increment_arg():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment("I")
    ok_(ret == 1)
    ret = db.increment("I", 100)
    ok_(ret == 101)
    db.close()

@with_setup(setup=clear)
def test_increment_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment("A", expire=2)
    ok_(ret)
    time.sleep(3)
    ret = db.get("A")
    ok_(ret == None)
    db.close()

