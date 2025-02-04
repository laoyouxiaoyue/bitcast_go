package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dirPath := filepath.Join(os.TempDir())
	dataFile1, err := OpenDataFile(dirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, os.ModePerm)
		assert.Nil(t, err)
	}
	t.Log(dirPath)
}

func TestDataFile_Write(t *testing.T) {
	dirPath := filepath.Join(os.TempDir())
	dataFile1, err := OpenDataFile(dirPath, 0)
	defer func(dataFile1 *DataFile) {
		err := dataFile1.Close()
		assert.Nil(t, err)
	}(dataFile1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("hello world1"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("hello world2"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dirPath := filepath.Join(os.TempDir())
	dataFile1, err := OpenDataFile(dirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dirPath := filepath.Join(os.TempDir())
	dataFile1, err := OpenDataFile(dirPath, 0)
	defer func(dataFile1 *DataFile) {
		err := dataFile1.Close()
		assert.Nil(t, err)
	}(dataFile1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Sync()
	assert.Nil(t, err)
}
