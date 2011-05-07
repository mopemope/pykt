# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException

@raises(IOError)
def test_err_report():
    db = KyotoTycoon()
    db.report()

def test_report():
    db = KyotoTycoon()
    db = db.open()
    ret = db.report()
    ok_(ret)
    ok_(isinstance(ret, dict))
    db.close()

def test_report_with_db():
    db = KyotoTycoon("test")
    db = db.open()
    ret = db.report()
    ok_(ret)
    ok_(isinstance(ret, dict))
    db.close()

def test_report_loop():
    db = KyotoTycoon()
    db = db.open()
    for i in xrange(100):
        ret = db.report()
        ok_(ret)
        ok_(isinstance(ret, dict))
    db.close()

