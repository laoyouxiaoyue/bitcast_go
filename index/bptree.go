package index

import (
	"bitcast_go/data"
	"fmt"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBuckerName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {

	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites

	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	fmt.Printf(filepath.Join(dirPath, bptreeIndexFileName))
	fmt.Printf(os.Getwd())
	if err != nil {
		panic("fail to open bptree")
	}

	// 创建对应的bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBuckerName)
		return err
	}); err != nil {
		panic("fail to create bucket")
	}
	return &BPlusTree{
		tree: bptree,
	}
}
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBuckerName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("fail to put key in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBuckerName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("fail to get key in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBuckerName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("fail to delete key in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBuckerName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("fail to get bucket size")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func (b *bptreeIterator) Rewind() {
	if b.reverse {
		b.currKey, b.currValue = b.cursor.Last()
	} else {
		b.currKey, b.currValue = b.cursor.First()
	}
}

func (b *bptreeIterator) Seek(key []byte) {
	b.currKey, b.currValue = b.cursor.Seek(key)
}

func (b *bptreeIterator) Next() {
	if b.reverse {
		b.currKey, b.currValue = b.cursor.Prev()
	} else {
		b.currKey, b.currValue = b.cursor.Next()
	}
}

func (b *bptreeIterator) Valid() bool {
	return len(b.currKey) != 0
}

func (b *bptreeIterator) Key() []byte {
	return b.currKey
}

func (b *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(b.currValue)
}

func (b *bptreeIterator) Close() {
	_ = b.tx.Rollback()
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin transaction")
	}
	return &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBuckerName).Cursor(),
		reverse: reverse,
	}
}
