import leveldb
import sys
import re

''' easy interface to leveldb '''

dbNames = ["alice", "bob", "charlie"]
dbs = [leveldb.LevelDB("/tmp/raftdb/%s" % x) for x in dbNames]

try:
	opt = sys.argv[1]
except:
	print "Invalid Option"
	sys.exit(1)

if opt == "-h":
	print "kc = key count\nlk = list keys\ndel = clear db\nget = get value\ngetr = get keys matching regex"

# list key count
if opt == "kc":
	for d in dbs:
		print len(list(d.RangeIter()))

# list keys
if opt == "lk":
	for db in dbs:
		for k, v in db.RangeIter():
			print k, v
		print "=== OK ==="

# delete all keys
if opt == "del":
	for db in dbs:
		for k, v in db.RangeIter():
			db.Delete(k)
		print "=== OK ==="

# get value for key
if opt == "get":
	key = sys.argv[2]
	for db in dbs:
		print db.Get(key)
		print "=== OK ==="
