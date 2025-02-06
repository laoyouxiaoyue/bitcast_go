package bitcast_go

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destoryDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			if err := db.Close(); err != nil {
				panic(err)
			}

			err := os.RemoveAll(db.options.DirPath)
			if err != nil {
				panic(err)
			}
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcast-go-iterator")
	opts.DirPath = dir

	fmt.Printf(dir)
	db, err := Open(opts)
	defer db.destroyDB()

	assert.NoError(t, err)
	assert.NotNil(t, db)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcast-go-iterator")
	opts.DirPath = dir

	fmt.Printf(dir)
	db, err := Open(opts)
	defer db.destroyDB()

	assert.NoError(t, err)
	assert.NotNil(t, db)

	db2, err := Open(opts)
	t.Log(db2)
	t.Log(err)
}
