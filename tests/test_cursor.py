# -*- coding: utf-8 -*-
from nose.tools import *
from pykt import KyotoTycoon, KTException, Cursor
import time

d = dict(A="B", C="D", E="F", G="H")
d2 = {
    "あいうえお": "ABC", 
    "かきくけこ": "てすと2", 
    "さしすせそ": "てすと2", 
    }

def clear():
    db = KyotoTycoon()
    db = db.open()
    db.clear()
    db.close()

@nottest
@raises(IOError)
def test_cursor_err():
    db = KyotoTycoon()
    db.cursor()


@with_setup(setup=clear)
def test_cursor():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    ok_(isinstance(c, Cursor))

@with_setup(setup=clear)
def test_cursor_jump():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    ret = c.jump()
    ok_(ret == True)

@raises(KTException)
@with_setup(setup=clear)
def test_cursor_jump_nodata():
    db = KyotoTycoon()
    db = db.open()
    c = db.cursor()
    c.jump()
    ok_(False)

"""
@nottest
@with_setup(setup=clear)
def test_cursor_jump_back():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    ret = c.jump()
    k1, v1 = c.get(True)
    c.jump_back()
    k3, v3 = c.get(True)
    ok_(k1 == k3)
    ok_(v1 == v3)
"""

@with_setup(setup=clear)
def test_cursor_step():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    ret = c.jump()
    k, v = c.get()
    c.step()
    k, v = c.get()
    ok_(k == "E")
    ok_(v == "F")

@with_setup(setup=clear)
def test_cursor_get():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "B")
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "B")

@with_setup(setup=clear)
def test_cursor_get_step():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k, v = c.get(True)
    ok_(k == "A")
    ok_(v == "B")
    k, v = c.get(True)
    ok_(k == "E")
    ok_(v == "F")
    k, v = c.get(True)
    ok_(k == "C")
    ok_(v == "D")


@with_setup(setup=clear)
def test_cursor_get_key():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k = c.get_key()
    ok_(k == "A")
    k = c.get_key()
    ok_(k == "A")

@with_setup(setup=clear)
def test_cursor_get_key_step():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k = c.get_key(True)
    ok_(k == "A")
    k = c.get_key(True)
    ok_(k == "E")
    k = c.get_key(True)
    ok_(k == "C")

@with_setup(setup=clear)
def test_cursor_get_value():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    v = c.get_value()
    ok_(v == "B")
    v = c.get_value()
    ok_(v == "B")

@with_setup(setup=clear)
def test_cursor_get_value_step():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    v = c.get_value(True)
    ok_(v == "B")
    v = c.get_value(True)
    ok_(v == "F")
    v = c.get_value(True)
    ok_(v == "D")

@with_setup(setup=clear)
def test_cursor_set_value():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "B")
    ret = c.set_value("C")
    ok_(ret == True)
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "C")
    
@with_setup(setup=clear)
def test_cursor_set_value_utf8():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "B")
    ret = c.set_value("あいうえお")
    ok_(ret == True)
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "あいうえお")

@with_setup(setup=clear)
def test_cursor_set_value_step():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "B")
    ret = c.set_value("C", True)
    ok_(ret == True)
    k, v = c.get()
    ok_(k == "E")
    ok_(v == "F")
    ok_(db.get("A") == "C")

@with_setup(setup=clear)
def test_cursor_set_value_expire():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    k, v = c.get()
    ok_(k == "A")
    ok_(v == "B")
    ret = c.set_value("C", expire=2)
    ok_(ret == True)
    time.sleep(3)
    k, v = c.get()
    ok_(k == "E")
    ok_(v == "F")

@with_setup(setup=clear)
def test_cursor_remove():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.jump()
    ret = c.remove()
    ok_(ret == True)
    k, v = c.get()
    ok_(k == "E")
    ok_(v == "F")
    ok_(db.get("A") == None)

@raises(KTException)
@with_setup(setup=clear)
def test_cursor_remove_fail():
    db = KyotoTycoon()
    db = db.open()
    db.set_bulk(d)
    c = db.cursor()
    c.remove()
    ok_(False)


