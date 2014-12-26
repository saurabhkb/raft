package storage

/*
=======================
DATABASE INTERFACE
=======================
*/

import "github.com/syndtr/goleveldb/leveldb"

var db *leveldb.DB

func Init(dbpath string) {
	db, _ = leveldb.OpenFile(dbpath, nil)
}

func Get(key string) (string, error) {
	v, e := db.Get([]byte(key), nil)
	return string(v), e
}

func Put(key, val string) error {
	return db.Put([]byte(key), []byte(val), nil)
}

func Close() error {
	return db.Close()
}
