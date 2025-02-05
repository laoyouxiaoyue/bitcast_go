package index

import (
	"bitcast_go/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	Size() int
	Iterator(reverse bool) Iterator
	Close() error
}
type IndexType = int8

const (
	// Btree 索引模型
	Btree IndexType = iota + 1

	// ART ART自适应基数树
	ART

	BPTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		return nil
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	// Rewind 从头开始
	Rewind()
	// Seek 找到第一个大于等于或小于等于的目标key
	Seek(key []byte)
	// Next 跳转到下一个key
	Next()
	// Valid 检查是否有效
	Valid() bool
	// Key 获取当前key值
	Key() []byte
	// Value 获取当前key值的logrecordpos值
	Value() *data.LogRecordPos
	// Close 关闭迭代器 释放资源
	Close()
}
