package redis

import (
	bitcask "bitcast_go"
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestDataTypeService_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	//t.Log(dir)
	opts.DirPath = dir
	dts, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = dts.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = dts.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := dts.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := dts.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = dts.Get(utils.GetTestKey(33))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestDataTypeService_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	//t.Log(dir)
	opts.DirPath = dir
	dts, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	// del
	err = dts.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = dts.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	// type
	typ, err := dts.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = dts.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = dts.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}
