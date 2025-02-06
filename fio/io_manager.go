package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

type IOManager interface {
	// Read 读指定文件
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)

	// Sync 刷盘
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManger(fileName)
	default:
		panic("unsupported io type")
	}
}
