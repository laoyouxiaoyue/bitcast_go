package bitcast_go

import (
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcast-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer db.destroyDB()

	assert.NoError(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcast-go-iterator")
	defer func(path string) {
		err := os.RemoveAll(path)
		assert.NoError(t, err)
	}(dir)
	opts.DirPath = dir
	db, err := Open(opts)
	defer db.destroyDB()

	assert.NoError(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.NoError(t, err)

	iterator = db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.NoError(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDB_Iterator_All_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcast-go-iterator3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer db.destroyDB()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("a"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("b"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("c"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("d"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("e"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ef"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("efz"), utils.RandomValue(10))
	assert.Nil(t, err)

	iter := db.NewIterator(DefaultIteratorOptions)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log("key = ", string(iter.Key()))
	}

	t.Log("---------------------------------------------\n")
	iter.Rewind()
	for iter.Seek([]byte("c")); iter.Valid(); iter.Next() {
		t.Log("key = ", string(iter.Key()))
	}
	t.Log("---------------------------------------------\n")
	// 反向
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		t.Log("key = ", string(iter2.Key()))
	}
	t.Log("---------------------------------------------\n")
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Reverse = true
	pre := []byte("e")
	iterOpts2.Prefix = pre
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.True(t, strings.HasPrefix(string(iter3.Key()), string(pre)))
		t.Log("key = ", string(iter3.Key()))
	}
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcast-go-iterator-ListKeys")

	t.Log(dir)
	opts.DirPath = dir
	db, err := Open(opts)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	err = db.Put([]byte("a"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("b"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("c"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("d"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("e"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ef"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("efz"), utils.RandomValue(10))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
	err = db.Close()
	db, err = Open(opts)
	defer func(db *DB) {
		err := db.Close()
		assert.NoError(t, err)
	}(db)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.NoError(t, err)
	keys = db.ListKeys()
	assert.Equal(t, 7, len(keys))
	for _, key := range keys {
		t.Log(key)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcast-go-iterator-ListKeys")

	opts.DirPath = dir
	db, err := Open(opts)
	defer db.destroyDB()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("a"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("b"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("c"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("d"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("e"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ef"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("efz"), utils.RandomValue(10))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		return true
	})
	assert.Nil(t, err)
}
