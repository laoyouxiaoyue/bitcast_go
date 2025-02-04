package fio

const DataFilePerm = 0644

type IOManager interface {
	// Read 读指定文件
	Read([]byte, int64) (int error)
	Write([]byte) (int, error)

	// Sync 刷盘
	Sync() error

	// Close 关闭文件
	Close() error
}
