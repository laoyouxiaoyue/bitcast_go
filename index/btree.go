package index

import (
	"bitcast_go/data"
	"bytes"
	"fmt"
	"github.com/google/btree"
	"sort"
	"strconv"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func (bt *BTree) Close() error {
	return nil
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}
func (bt *BTree) Size() int {
	return bt.tree.Len()
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

type btreeIterator struct {
	currIndex int     // 目前下标
	reverse   bool    // 是否反向
	values    []*Item // key+位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		values:    values,
		reverse:   reverse,
		currIndex: 0,
	}
}

// Rewind 回到迭代器起点
func (b *btreeIterator) Rewind() {
	b.currIndex = 0
}

func (b *btreeIterator) Seek(key []byte) {
	if b.reverse {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) <= 0
		})
	} else {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) >= 0
		})
	}

}

func (b *btreeIterator) Next() {
	b.currIndex++
	fmt.Printf(strconv.Itoa(b.currIndex))
}

func (b *btreeIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

func (b *btreeIterator) Key() []byte {
	return b.values[b.currIndex].key
}

func (b *btreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIndex].pos
}

func (b *btreeIterator) Close() {
	b.values = nil
}
