package bitcast_go

type Options struct {
	DirPath string //数据库目录

	DataFileSize int64 //单个文件最大大小

	SyncWrites bool //每次操作是否持久化

	IndexType IndexerType
}

type IndexerType = int8

const (
	// BTree
	BTree IndexerType = iota + 1

	// ART自适应基数树索引
	ART
)
