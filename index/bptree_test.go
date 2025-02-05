package index

import (
	"bitcast_go/data"
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join("./tmp")
	if err := os.MkdirAll("./tmp", 0755); err != nil {
		log.Fatal("Failed to create directory:", err)
	}
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put(utils.GetTestKey(10), &data.LogRecordPos{Fid: 10, Offset: 5})
	tree.Put(utils.GetTestKey(12), &data.LogRecordPos{Fid: 11, Offset: 6})
	tree.Put(utils.GetTestKey(13), &data.LogRecordPos{Fid: 12, Offset: 7})
	assert.Equal(t, 3, tree.Size())

	value := tree.Get(utils.GetTestKey(10))
	assert.Equal(t, &data.LogRecordPos{Fid: 10, Offset: 5}, value)

	value = tree.Get(utils.GetTestKey(12))
	assert.Equal(t, &data.LogRecordPos{Fid: 11, Offset: 6}, value)

	value = tree.Get(utils.GetTestKey(13))
	assert.Equal(t, &data.LogRecordPos{Fid: 12, Offset: 7}, value)

	value = tree.Get(utils.GetTestKey(14))
	assert.Nil(t, value)
	res := tree.Delete(utils.GetTestKey(13))
	assert.True(t, res)

}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join("./tmp")
	if err := os.MkdirAll("./tmp", 0755); err != nil {
		log.Fatal("Failed to create directory:", err)
	}
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put(utils.GetTestKey(10), &data.LogRecordPos{Fid: 10, Offset: 5})
	tree.Put(utils.GetTestKey(12), &data.LogRecordPos{Fid: 11, Offset: 6})
	tree.Put(utils.GetTestKey(13), &data.LogRecordPos{Fid: 12, Offset: 7})
	assert.Equal(t, 3, tree.Size())

	value := tree.Get(utils.GetTestKey(10))
	assert.Equal(t, &data.LogRecordPos{Fid: 10, Offset: 5}, value)

	value = tree.Get(utils.GetTestKey(12))
	assert.Equal(t, &data.LogRecordPos{Fid: 11, Offset: 6}, value)

	value = tree.Get(utils.GetTestKey(13))
	assert.Equal(t, &data.LogRecordPos{Fid: 12, Offset: 7}, value)

	value = tree.Get(utils.GetTestKey(14))
	assert.Nil(t, value)
	res := tree.Delete(utils.GetTestKey(13))
	assert.True(t, res)

	iter := tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(iter.Value())
	}
}
