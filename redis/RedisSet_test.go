package redis

import (
	bitcask "bitcast_go"
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDataTypeService_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-sismember")
	opts.DirPath = dir
	dts, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dts.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestDataTypeService_SRem(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-srem")
	opts.DirPath = dir
	dts, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dts.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = dts.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = dts.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}
