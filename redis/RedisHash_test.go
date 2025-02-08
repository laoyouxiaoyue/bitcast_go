package redis

import (
	bitcask "bitcast_go"
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDataTypeService_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	dts, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(100)
	ok2, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := dts.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := dts.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := dts.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = dts.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestDataTypeService_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hdel")
	opts.DirPath = dir
	dts, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	del1, err := dts.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(100)
	ok2, err := dts.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := dts.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := dts.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
}
