# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException

def test_init():
    db = KyotoTycoon()
    ok_(db != None)

def test_open():
    db = KyotoTycoon()
    db = db.open()
    ok_(db != None)

@raises(IOError)
def test_open_err():
    db = KyotoTycoon()
    db.open("A", 7777)
    db.close()

def test_close():
    db = KyotoTycoon()
    db = db.open()
    ret = db.close()
    ok_(ret == True)

def test_non_connect_close():
    db = KyotoTycoon()
    ret = db.close()
    ok_(ret == False)

def test_double_close():
    db = KyotoTycoon()
    ret = db.close()
    ret = db.close()
    ok_(ret == False)

def test_set_db():
    db = KyotoTycoon()
    db.db = "test"
    ok_(db.db == "test")

def test_get_db1():
    db = KyotoTycoon()
    ok_(db.db == None)

def test_get_db2():
    db = KyotoTycoon("test")
    ok_(db.db == "test")
