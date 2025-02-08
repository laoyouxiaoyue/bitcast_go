package redis

import (
	bitcask "bitcast_go"
	"errors"
)

var (
	ErrWrongTypeOperation = errors.New("wrong type operation")
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	List
	Hash
	Set
)

// RedisDataStructure Redis 数据结构服务
type RedisDataStructure struct {
	db *bitcask.DB
}

// NewRedisDataStructure 初始化Redis 数据结构服务
func NewRedisDataStructure(options bitcask.Options) (*RedisDataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}
