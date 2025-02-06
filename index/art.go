package index

import (
	"bitcast_go/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func (a *AdaptiveRadixTree) Close() error {
	return nil
}

// NewART 初始化索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: &sync.RWMutex{},
	}
}

func (a *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	a.lock.Lock()
	defer a.lock.Unlock()
	oldValue, _ := a.tree.Insert(key, pos)
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (a *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	a.lock.RLock()
	defer a.lock.RUnlock()
	value, found := a.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (a *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	a.lock.Lock()
	defer a.lock.Unlock()
	oldValue, deleted := a.tree.Delete(key)
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (a *AdaptiveRadixTree) Size() int {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.tree.Size()
}

func (a *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return newartIterator(a.tree, reverse)
}

type artIterator struct {
	currIndex int     // 目前下标
	reverse   bool    // 是否反向
	values    []*Item // key+位置索引信息
}

func newartIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	values := make([]*Item, tree.Size())

	if reverse {
		idx = tree.Size() - 1
	}

	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		values:    values,
		reverse:   reverse,
		currIndex: 0,
	}
}

// Rewind 回到迭代器起点
func (b *artIterator) Rewind() {
	b.currIndex = 0
}

func (b *artIterator) Seek(key []byte) {
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

func (b *artIterator) Next() {
	b.currIndex++
}

func (b *artIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

func (b *artIterator) Key() []byte {
	return b.values[b.currIndex].key
}

func (b *artIterator) Value() *data.LogRecordPos {
	return b.values[b.currIndex].pos
}

func (b *artIterator) Close() {
	b.values = nil
}
