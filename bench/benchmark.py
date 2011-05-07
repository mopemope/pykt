# -*- coding: utf-8 -*-
from pykt import KyotoTycoon as kt1
from kyototycoon import KyotoTycoon as kt2
import timeit

key = "A" * 12
val = "B" * 1024  

def pykt_set():
    db = kt1()
    db.open()
    ret = db.set(key, val)
    assert ret == True
    db.close()

def pykt_replace():
    db = kt1()
    db.open()
    ret = db.replace(key, val)
    assert ret == True
    db.close()

def pykt_append():
    db = kt1()
    db.open()
    ret = db.append(key, val)
    assert ret == True
    db.close()

def kyoto_set():
    db = kt2()
    db.open()
    ret = db.set(key, val)
    assert ret == True
    db.close()

def kyoto_append():
    db = kt2()
    db.open()
    ret = db.append(key, val)
    assert ret == True
    db.close()

def pykt_get():
    db = kt1()
    db.open()
    ret = db.get(key)
    assert ret == val
    db.close()

def kyoto_get():
    db = kt2()
    db.open()
    ret = db.get(key)
    assert ret == val
    db.close()

def pykt_gets():
    db = kt1()
    db.open()
    
    for i in xrange(10):
        ret = db.get(key)
        assert ret == val
    db.close()

def kyoto_gets():
    db = kt2()
    db.open()
    
    for i in xrange(10):
        ret = db.get(key)
        assert ret == val
    db.close()

def pykt_increment():
    db = kt1()
    db.open()
    ret = db.increment("N", 1)
    db.close()

def kyoto_increment():
    db = kt2()
    db.open()
    ret = db.increment("N", 1)
    db.close()

if __name__ == "__main__":
    res = timeit.timeit("pykt_set()", "from __main__ import pykt_set", number=1000)
    print "pykt_set %f" % res
    #res = timeit.timeit("pykt_replace()", "from __main__ import pykt_replace", number=1000)
    #print "pykt_replace %f" % res
    res = timeit.timeit("kyoto_set()", "from __main__ import kyoto_set", number=1000)
    print "kt_set %f" % res
    
    #res = timeit.timeit("pykt_append()", "from __main__ import pykt_append", number=1000)
    #print "pykt_append %f" % res
    #res = timeit.timeit("kyoto_append()", "from __main__ import kyoto_append", number=1000)
    #print "kt_append %f" % res
    
    res = timeit.timeit("pykt_get()", "from __main__ import pykt_get", number=1000)
    print "pykt_get %f" % res
    res = timeit.timeit("kyoto_get()", "from __main__ import kyoto_get", number=1000)
    print "kt_get %f" % res

    res = timeit.timeit("pykt_gets()", "from __main__ import pykt_gets", number=100)
    print "pykt_gets %f" % res
    res = timeit.timeit("kyoto_gets()", "from __main__ import kyoto_gets", number=100)
    print "kt_gets %f" % res

    res = timeit.timeit("pykt_increment()", "from __main__ import pykt_increment", number=1000)
    print "pykt_increment %f" % res
    res = timeit.timeit("kyoto_increment()", "from __main__ import kyoto_increment", number=1000)
    print "kt_increment %f" % res

