package fio

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewFileIOManager(t *testing.T) {

	fio, err := NewFileIOManager(filepath.Join("tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("hello"))
	t.Log(n, err)
	n, err = fio.Write([]byte("hello22"))
	t.Log(n, err)

}

func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("read_test"))
	assert.Nil(t, err)

	tmp := make([]byte, 9)
	n, err := fio.Read(tmp, 0)

	assert.Equal(t, 9, n)

	assert.Equal(t, []byte("read_test"), tmp)
}
