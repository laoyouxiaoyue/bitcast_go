package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	header := &LogRecord{
		Key:   []byte("test"),
		Value: []byte("test"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(header)
	t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	header2 := &LogRecord{
		Key:  []byte("test"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(header2)
	t.Log(res2)
	assert.NotNil(t, n2)
	assert.Greater(t, n2, int64(5))
}
