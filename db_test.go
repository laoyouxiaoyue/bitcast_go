package bitcast_go

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

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
