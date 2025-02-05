package bitcast_go

import (
	"bitcast_go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp(os.TempDir(), "bitcast")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//写数据之后未提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	t.Log(val)

	//提交
	err = wb.Commit()
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)

	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	err = wb2.Commit()
	assert.Nil(t, err)

	val, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
}
