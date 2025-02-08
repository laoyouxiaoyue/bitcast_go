package redis

import (
	bitcask "bitcast_go"
	"errors"
)

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 查找元数据

	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造Hash数据部分的key

	hk := &hashInternalKy{
		key:     key,
		filed:   field,
		version: meta.version,
	}

	encKey := hk.encode()

	// 先查找是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKy{
		key:     key,
		filed:   field,
		version: meta.version,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKy{
		key:     key,
		filed:   field,
		version: meta.version,
	}

	encKey := hk.encode()
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}
