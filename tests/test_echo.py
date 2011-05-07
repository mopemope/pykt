# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException

@raises(IOError)
def test_err_echo():
    db = KyotoTycoon()
    db.echo()

def test_echo():
    db = KyotoTycoon()
    db = db.open()
    ret = db.echo()
    ok_(ret == True) 
    db.close()

def test_echo_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    ret = db.echo()
    ok_(ret == True) 
    db.close()

def test_echo_loop():
    db = KyotoTycoon()
    db = db.open()
    for i in xrange(100):
        ret = db.echo()
        ok_(ret == True) 
    db.close()

