package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap 用来读取索取数据
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManger(filename string) (*MMap, error) {
	_, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (mp *MMap) Read(b []byte, offset int64) (int, error) {
	return mp.readerAt.ReadAt(b, offset)
}

func (mp *MMap) Write(bytes []byte) (int, error) {
	panic("not implement")
}

func (mp *MMap) Sync() error {
	panic("not implement")
}

func (mp *MMap) Close() error {
	return mp.readerAt.Close()
}

func (mp *MMap) Size() (int64, error) {
	return int64(mp.readerAt.Len()), nil
}
