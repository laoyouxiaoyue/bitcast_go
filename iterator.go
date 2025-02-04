package bitcast_go

import (
	"bitcast_go/index"
	"strings"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

func (i *Iterator) Rewind() {
	i.indexIter.Rewind()
}

func (i *Iterator) Seek(key []byte) {
	i.indexIter.Seek(key)
	i.skipToNext()
}

func (i *Iterator) Next() {
	i.indexIter.Next()
	i.skipToNext()
}

func (i *Iterator) Valid() bool {
	return i.indexIter.Valid()
}

func (i *Iterator) Key() []byte {
	return i.indexIter.Key()
}

func (i *Iterator) Value() ([]byte, error) {
	logRecordPos := i.indexIter.Value()

	i.db.mu.RLock()
	defer i.db.mu.RUnlock()

	return i.db.getValueByPostion(logRecordPos)
}

func (i *Iterator) Close() {
	i.indexIter.Close()
}

func (i *Iterator) skipToNext() {
	prefixlen := len(i.options.Prefix)
	if prefixlen == 0 {
		return
	}
	for ; i.indexIter.Valid(); i.indexIter.Next() {
		if strings.HasPrefix(string(i.indexIter.Key()), string(i.options.Prefix)) {
			break
		}
	}
}
