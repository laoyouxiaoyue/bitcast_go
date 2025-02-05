package bitcast_go

type Options struct {
	DirPath string //数据库目录

	DataFileSize int64 //单个文件最大大小

	SyncWrites bool //每次操作是否持久化

	IndexType IndexerType
}

type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 提交时是否sync持久化
	SyncWrites bool
}

type IteratorOptions struct {
	Prefix []byte

	Reverse bool
}

type IndexerType = int8

const (
	// BTree
	BTree IndexerType = iota + 1

	// ART自适应基数树索引
	ART
)

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultOptions = Options{
	DirPath:      ".",
	DataFileSize: 64 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    BTree,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
