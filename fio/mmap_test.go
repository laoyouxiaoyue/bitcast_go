package fio

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewMMapIOManger(t *testing.T) {
	path := filepath.Join("./tmp", "mmap-a.data")

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aaaaa"))

	mmapIo, err := NewMMapIOManger(path)
	assert.Nil(t, err)
	size, err := mmapIo.Size()
	t.Log(size)
}
