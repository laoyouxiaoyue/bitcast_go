package benchmark

import (
	bitcask "bitcast_go"
	"bitcast_go/utils"
	"errors"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

var db *bitcask.DB

func init() {
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp(os.TempDir(), "bitcast-go-benchmark")
	options.DirPath = dir

	var err error
	db, err = bitcask.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int() % b.N))
		if err != nil && errors.Is(err, bitcask.ErrKeyNotFound) {
			panic(err)
		}
	}
}
func Benchmark_Delete(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int() % b.N))
		assert.Nil(b, err)
	}
}
