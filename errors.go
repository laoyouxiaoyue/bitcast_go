package bitcast_go

import "errors"

var (
	ErrKeyIsEmpty = errors.New("key is empty")

	ErrIndexUpdataFailed = errors.New("index updata failed")

	ErrKeyNotFound = errors.New("key not found")

	ErrDataFileNotFound = errors.New("data file not found")

	ErrDirPathIsEmpty = errors.New("database dir path is empty")

	ErrDataFileSizeIllegal = errors.New("data file size is illegal")

	ErrDataDirCorrupted = errors.New("data directory is corrupted")

	ErrExceedMaxBatchNum = errors.New("exceed max batch num")

	ErrMergeIsProgress = errors.New("merge is progress")

	ErrDatabaseIsUsing = errors.New("database is using database")

	ErrInvalidMergeRatio = errors.New("invalid merge ratio,must between 0 and 1")

	ErrMergeRationUnreached = errors.New("merge ratio is unreached")

	ErrNoEnoughSpaceForMerge = errors.New("no enough space for merge ratio")
)
