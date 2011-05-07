
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
def test_err_increment_double():
    db = KyotoTycoon()
    ret = db.increment_double("ID")

@with_setup(setup=clear)
def test_increment_double():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment_double("ID")
    ok_(ret == 1.0)
    ret = db.increment_double("ID")
    ok_(ret == 2.0)
    db.close()

@raises(KTException)
@with_setup(setup=clear)
def test_increment_double_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    db.increment_double("ID")
    ok_(False)

@with_setup(setup=clear)
def test_increment_double_utf8():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment_double("インクリメントd")
    ok_(ret == 1.0)
    db.close()

@with_setup(setup=clear)
def test_increment_double_arg():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment_double("ID")
    ok_(ret == 1.0)
    ret = db.increment_double("ID", 100.1)
    ok_(ret == 101.1)

@with_setup(setup=clear)
def test_increment_expire():
    db = KyotoTycoon()
    db = db.open()
    ret = db.increment_double("A", expire=2)
    ok_(ret)
    time.sleep(3)
    ret = db.get("A")
    ok_(ret == None)
    db.close()
