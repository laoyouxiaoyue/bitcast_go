package index

import (
	"bitcast_go/data"
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_adaptiveRadixTree_Put(t *testing.T) {
	art := NewART()

	// Put
	art.Put(utils.GetTestKey(5), &data.LogRecordPos{Fid: 1, Offset: 5})
	art.Put(utils.GetTestKey(6), &data.LogRecordPos{Fid: 1, Offset: 6})
	art.Put(utils.GetTestKey(7), &data.LogRecordPos{Fid: 1, Offset: 7})
	art.Put(utils.GetTestKey(7), &data.LogRecordPos{Fid: 1, Offset: 8})
	// Get
	res := art.Get(utils.GetTestKey(5))
	assert.Equal(t, res, &data.LogRecordPos{Fid: 1, Offset: 5})
	res = art.Get(utils.GetTestKey(8))
	assert.Nil(t, res)

	//Deleted
	res1 := art.Delete(utils.GetTestKey(5))
	assert.True(t, res1)

	res = art.Get(utils.GetTestKey(5))
	assert.Nil(t, res)

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Value())
	}

	iter = art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Value())
	}
}
