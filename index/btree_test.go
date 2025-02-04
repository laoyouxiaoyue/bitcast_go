package index

import (
	"bitcast_go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 2, Offset: 103})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 单条简单
	bt1.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iter2 := bt1.Iterator(true)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// rewind测试
	iter2.Rewind()
	assert.Equal(t, true, iter2.Valid())

	// 多数据
	bt1.Put([]byte("b"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt1.Put([]byte("c"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt1.Put([]byte("d"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt1.Put([]byte("e"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt1.Put([]byte("f"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt1.Put([]byte("g"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iter3 := bt1.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	//seek测试

	iter5 := bt1.Iterator(true)
	for iter5.Seek([]byte("b")); iter5.Valid(); iter5.Next() {
		assert.GreaterOrEqual(t, []byte("b"), iter5.Key())
	}

	iter6 := bt1.Iterator(false)
	for iter6.Seek([]byte("b")); iter6.Valid(); iter6.Next() {
		assert.LessOrEqual(t, []byte("b"), iter6.Key())
	}
}
