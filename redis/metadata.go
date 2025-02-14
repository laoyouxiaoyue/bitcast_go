package redis

import (
	"encoding/binary"
	"math"
)

type metadata struct {
	dataType byte   // 数据类型
	expire   int64  //过期时间
	version  int64  //版本号
	size     uint32 //数据量
	head     uint64 // List 数据结构专用
	tail     uint64 // List数据结构专业
}

const (
	maxMetadataSize  = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetSize
	}

	buf := make([]byte, size)
	buf[0] = md.dataType

	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n
	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, n = binary.Uvarint(buf[index:])
		index += n
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		tail:     tail,
		head:     head,
	}
}

type hashInternalKy struct {
	key     []byte
	version int64
	filed   []byte
}

func (hk *hashInternalKy) encode() []byte {
	buf := make([]byte, len(hk.key)+8+len(hk.filed))
	// key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	//field
	copy(buf[index:], hk.filed)

	return buf
}
