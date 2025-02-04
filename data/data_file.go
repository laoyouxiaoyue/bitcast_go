package data

import "bitcast_go/fio"

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

const DataFileNameSuffix = ".data"

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}
func (df *DataFile) Write(data []byte) error {
	return nil
}
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
