package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// ErrKeyNotFound is returned when a key does not exist in the store.
var ErrKeyNotFound = errors.New("key not found")

var BPrefix string = "bucket"
var BPrefixByte []byte = []byte(BPrefix) // bucket:<bucket_name>:<key>

var BListPrefix string = "blist"
var BListPrefixByte []byte = []byte(BListPrefix) // blist:<bucket_name>
type Bucket []byte

func (buck Bucket) Bucket() []byte {
	if !bytes.HasPrefix(buck, BPrefixByte) {
		return nil
	}

	rest := buck[len(BPrefix)+1:]

	idx := bytes.IndexByte(rest, ':')
	if idx == -1 {
		return nil
	}

	return rest[:idx]
}

func (buck Bucket) Key() []byte {
	if !bytes.HasPrefix(buck, BPrefixByte) {
		return nil
	}

	rest := buck[len(BPrefix)+1:]

	idx := bytes.IndexByte(rest, ':')
	if idx == -1 || idx+1 >= len(rest) {
		return nil
	}

	return rest[idx+1:]
}

var IPrefix string = "identifier"
var IPrefixByte []byte = []byte(IPrefix) // identifier:<bucket_name>:<key>:<id>
type Identifier []byte

func (i Identifier) Bucket() []byte {
	if !bytes.HasPrefix(i, IPrefixByte) {
		return nil
	}

	rest := i[len(IPrefix)+1:]

	idx := bytes.IndexByte(rest, ':')
	if idx == -1 {
		return nil
	}

	return rest[:idx]
}

func (i Identifier) Key() []byte {
	if !bytes.HasPrefix(i, IPrefixByte) {
		return nil
	}

	rest := i[len(IPrefix)+1:]

	idx1 := bytes.IndexByte(rest, ':')
	if idx1 == -1 {
		return nil
	}

	rest = rest[idx1+1:]

	idx2 := bytes.IndexByte(rest, ':')
	if idx2 == -1 {
		return nil
	}

	return rest[:idx2]
}

func (i Identifier) ID() []byte {
	if !bytes.HasPrefix(i, IPrefixByte) {
		return nil
	}

	rest := i[len(IPrefix)+1:] // FIX

	// skip bucket
	idx1 := bytes.IndexByte(rest, ':')
	if idx1 == -1 {
		return nil
	}
	rest = rest[idx1+1:]

	// skip key
	idx2 := bytes.IndexByte(rest, ':')
	if idx2 == -1 || idx2+1 >= len(rest) {
		return nil
	}

	return rest[idx2+1:]
}

type DataStorage struct {
	db   *pebble.DB
	flag int32
}

func (ds *DataStorage) TryLock() bool {
	return atomic.CompareAndSwapInt32(&ds.flag, 0, 1)
}

func (ds *DataStorage) Unlock() {
	atomic.StoreInt32(&ds.flag, 0)
}

func NewDataStorage(folder string) *DataStorage {
	db, err := pebble.Open(folder, &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	ds := &DataStorage{
		db: db,
	}
	return ds
}

func (ds *DataStorage) ListAll() {
	fmt.Println("\nPrinting DB:")
	it, _ := ds.db.NewIter(nil)
	for it.First(); it.Valid(); it.Next() {
		val := it.Value()
		if len(val) > 20 {
			fmt.Printf("Key: %s, Value: %s...\n", it.Key(), val[:20])
		} else {
			fmt.Printf("Key: %s, Value: %s\n", it.Key(), val)
		}
	}
}

// func (ds *DataStorage) GetBuckets() ([]string, error) {
// 	arr := make([]string, 0)
// 	it, err := ds.db.NewIter(&pebble.IterOptions{
// 		LowerBound: []byte(fmt.Sprintf("%s:0")),
// 		UpperBound: []byte(fmt.Sprintf("%s:z")),
// 	})
// 	if err != nil {
// 		return arr, err
// 	}
// 	defer it.Close()
// 	for it.First(); it.Valid(); it.Next() {
// 		arr = append(arr, string(it.Value()))
// 	}
// 	return arr, nil
// }

// func (ds *DataStorage) CreateBucket(name string) error {
// 	key := fmt.Sprintf("%s:%s", BPrefix, name)
// 	return ds.db.Set([]byte(key), []byte(name), pebble.Sync)
// }

// func (ds *DataStorage) DeleteQueue(name string) error {
// 	key := fmt.Sprintf("%s:%s", BPrefix, name)
// 	return ds.db.Delete([]byte(key), pebble.Sync)
// }

func (ds *DataStorage) DB() *pebble.DB {
	return ds.db
}

func (ds *DataStorage) UpsertKeyAsync(meta FileMetadata) error {
	data, err := meta.Encode()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s:%s:%s", BPrefix, meta.Bucket, meta.Key)
	return ds.db.Set([]byte(key), data, pebble.NoSync)
}

func (ds *DataStorage) InsertIdentifierAsync(meta FileMetadata) error {
	data, err := meta.Encode()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s:%s:%s:%s", IPrefix, meta.Bucket, meta.Key, meta.Identifier)
	return ds.db.Set([]byte(key), data, pebble.NoSync)
}

func (ds *DataStorage) UpsertKeySync(meta FileMetadata) error {
	data, err := meta.Encode()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s:%s:%s", BPrefix, meta.Bucket, meta.Key)
	return ds.db.Set([]byte(key), data, pebble.Sync)
}

func (ds *DataStorage) DeleteOldIdentifiers(meta FileMetadata) ([]FileMetadata, error) {
	var deleted []FileMetadata
	iter, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(fmt.Sprintf("%s:%s:%s", IPrefix, meta.Bucket, meta.Key)),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		val := Identifier(iter.Key())
		if !bytes.Equal(val.Bucket(), []byte(meta.Bucket)) {
			break
		}
		if !bytes.Equal(val.Key(), []byte(meta.Key)) {
			break
		}
		if !bytes.Equal(val.ID(), []byte(meta.Identifier)) {
			var fm FileMetadata
			if decErr := fm.Decode(iter.Value()); decErr != nil {
				continue
			}
			deleted = append(deleted, fm)
			if er := ds.db.Delete(iter.Key(), pebble.Sync); er != nil {
				return deleted, er
			}
		}
	}
	return deleted, nil
}

func (ds *DataStorage) DeleteKey(bucket, key string) error {
	startK := fmt.Sprintf("%s:%s:%s", BPrefix, bucket, key)
	return ds.db.Delete([]byte(startK), pebble.Sync)
}

func (ds *DataStorage) CreateBucket(bucket string) error {
	key := fmt.Sprintf("%s:%s", BListPrefix, bucket)
	return ds.db.Set([]byte(key), []byte("1"), pebble.Sync)
}

func (ds *DataStorage) DeleteIdentifier(bucket, key, id string) error {
	idk := fmt.Sprintf("%s:%s:%s:%s", IPrefix, bucket, key, id)
	return ds.db.Delete([]byte(idk), pebble.Sync)
}

func (ds *DataStorage) GetKey(bucket, key string) (FileMetadata, error) {
	dbkey := fmt.Sprintf("%s:%s:%s", BPrefix, bucket, key)
	value, closer, err := ds.db.Get([]byte(dbkey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return FileMetadata{}, ErrKeyNotFound
		}
		return FileMetadata{}, err
	}
	defer closer.Close()
	var fm FileMetadata
	if err := fm.Decode(value); err != nil {
		return FileMetadata{}, err
	}
	return fm, nil
}

// ListKeys returns object metadata for keys in the bucket with the given prefix, up to maxKeys.
// Keys are returned in lexicographical order.
func (ds *DataStorage) ListKeys(bucket, prefix string, maxKeys int) ([]FileMetadata, error) {
	lowerBound := fmt.Sprintf("%s:%s:%s", BPrefix, bucket, prefix)
	// Exclusive upper bound: first string > "bucket:bucket:*" is "bucket:bucket;" (':'+1)
	upperBound := fmt.Sprintf("%s:%s;", BPrefix, bucket)
	iter, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	keyPrefix := fmt.Sprintf("%s:%s:", BPrefix, bucket)
	var result []FileMetadata
	for iter.First(); iter.Valid() && len(result) < maxKeys; iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, []byte(keyPrefix)) {
			break
		}
		var fm FileMetadata
		if err := fm.Decode(iter.Value()); err != nil {
			continue
		}
		if prefix != "" && !bytes.HasPrefix([]byte(fm.Key), []byte(prefix)) {
			continue
		}
		result = append(result, fm)
	}
	return result, nil
}

// ListBuckets returns all bucket names: those with at least one object plus those explicitly created (CreateBucket).
func (ds *DataStorage) ListBuckets() ([]string, error) {
	seen := make(map[string]struct{})

	// Buckets that have at least one object (keys bucket:bucketname:key)
	lowerBound := fmt.Sprintf("%s:", BPrefix)
	iter, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
	})
	if err != nil {
		return nil, err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, BPrefixByte) {
			break
		}
		b := Bucket(key)
		name := string(b.Bucket())
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	iter.Close()

	// Explicitly created buckets (blist:bucketname)
	blistLower := fmt.Sprintf("%s:", BListPrefix)
	blistIter, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(blistLower),
	})
	if err != nil {
		return nil, err
	}
	for blistIter.First(); blistIter.Valid(); blistIter.Next() {
		key := blistIter.Key()
		if !bytes.HasPrefix(key, BListPrefixByte) {
			break
		}
		name := string(key[len(BListPrefix)+1:])
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	blistIter.Close()

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	return names, nil
}
