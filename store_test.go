package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
)

func setupDB(t *testing.T) *DataStorage {
	// Remove old db if any
	os.RemoveAll("test")
	ds := NewDataStorage("test")
	t.Cleanup(func() {
		ds.db.Close()
		os.RemoveAll("test")
	})
	return ds
}

func TestIterationRange(t *testing.T) {
	ds := setupDB(t)
	fm := FileMetadata{
		Bucket:     "amd",
		Key:        "meta.csv",
		Identifier: "a3f16799-d85c-4930-be23-e55f08ffedff",
	}
	ds.InsertIdentifierAsync(fm)
	fm.Identifier = "2a53989b-7b7c-4800-ba23-c7db3569b3df"
	ds.InsertIdentifierAsync(fm)
	fm.Identifier = "0447faf2-0bda-42be-8738-6a7628eb05f9"
	ds.InsertIdentifierAsync(fm)
	fm.Key = "metaa.csv"
	ds.InsertIdentifierAsync(fm)
	fm.Key = "met.csv"
	ds.InsertIdentifierAsync(fm)
	ds.ListAll()
	iter, _ := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(fmt.Sprintf("%s:%s:%s", BPrefix, "amd", "meta.csv")),
	})
	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Println(string(iter.Key()))
	}
}
