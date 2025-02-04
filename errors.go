package bitcast_go

import "errors"

var (
	ErrKeyIsEmpty = errors.New("key is empty")

	ErrIndexUpdataFailed = errors.New("index updata failed")

	ErrKeyNotFound = errors.New("key not found")

	ErrDataFileNotFound = errors.New("data file not found")

	ErrDirPathIsEmpty = errors.New("database dir path is empty")

	ErrDataFileSizeIllegal = errors.New("data file size is illegal")

	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
)
