from pykt import KyotoTycoon, set_serializer, set_deserializer
from cPickle import dumps, loads

set_serializer(dumps)
set_deserializer(loads)

key = "A" * 12
val = "B" * 1024  

d = dict(name="John", no=1)

db = KyotoTycoon()
db.open()
print db.set(key, d)
ret = db.get(key)
assert(d == ret)
db.close()


