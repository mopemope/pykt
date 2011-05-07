from pykt import KyotoTycoon 

key = "A" * 12
val = "B" * 1024  

db = KyotoTycoon()
db.open()
print db.set(key, val)
print db.get(key)
db.close()

