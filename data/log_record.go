package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecordPos struct {
	Fid    uint32 // 文件 id ,数据所在位置
	Offset int64  // 偏移，在这个文件块的哪个位置
}

// 编码Logrecord , 返回数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 每一行数据的格式头
type logRecordHeader struct {
	crc        uint32 // crc 校验码
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(l *LogRecord, header []byte) uint32 {
	return 0
}
