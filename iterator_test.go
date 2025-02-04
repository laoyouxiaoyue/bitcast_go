package bitcast_go

import (
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"os"
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
