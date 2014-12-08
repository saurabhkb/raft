package storage

/*
=======================
DATABASE INTERFACE
=======================
*/

import (
	"fmt"
)

import "github.com/syndtr/goleveldb/leveldb"

var db *leveldb.DB

func Init(dbpath string) {
	var e error
	db, e = leveldb.OpenFile(dbpath, nil)
	fmt.Println("DB ERROR", e)
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
