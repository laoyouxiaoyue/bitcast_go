package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// 编码Logrecord , 返回数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type
	var index = 5

	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

// EncodeLogRecordPos 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}
func DecodeLogRecordPos(buf []byte) *LogRecordPos {

	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
	}
}

// 每一行数据的格式头
type logRecordHeader struct {
	crc        uint32 // crc 校验码
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if buf == nil || len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:])

	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(l *LogRecord, header []byte) uint32 {

	if l == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, l.Key)
	crc = crc32.Update(crc, crc32.IEEETable, l.Value)

	return crc

}
