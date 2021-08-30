package bytepool

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const retry = 10

func TestMemUsage(t *testing.T) {
	time.Sleep(2 * time.Second)
	b := GetBytesWithLen(1234)
	putAndWait(b)

	assert.Equal(t, int64(2048), memUsage)
}

func TestReusable(t *testing.T) {
	time.Sleep(2 * time.Second)
	b := GetBytesWithLen(10)
	b[0] = 1
	putAndWait(b)
	c := GetBytesWithLen(10)
	assert.Equal(t, c[0], b[0])
}

func TestLimitReached(t *testing.T) {
	time.Sleep(2 * time.Second)
	prevLimit := MemoryLimit
	SetMemoryLimit(20)
	defer SetMemoryLimit(prevLimit)
	b := GetBytesWithLen(10)
	c := GetBytesWithLen(10)
	putAndWait(b)

	assert.Equal(t, false, withinLimitWhenAdd(c))
	PutBytes(c)
	assertEqualWithRetry(t, int64(16), memUsage)
}

func TestSelfCleanUp(t *testing.T) {
	time.Sleep(2 * time.Second)
	b := GetBytesWithLen(10)
	putAndWait(b)
	time.Sleep(2 * time.Second)
	assertEqualWithRetry(t, int64(0), memUsage)
}

func TestExpand(t *testing.T) {
	time.Sleep(2 * time.Second)
	size := 3
	for i := 0; i < initialChannelSize+6; i++ {
		PutBytes(make([]byte, size))
	}
	time.Sleep(200 * time.Millisecond)
	idx := getMaxIdx(size)
	assert.Equal(t, initialChannelSize+expandChannelDelta, cap(exp2Pools[idx].bytesStream))
}

func putAndWait(b []byte) {
	PutBytes(b)
	time.Sleep(500 * time.Millisecond)
}

func assertEqualWithRetry(t *testing.T, expected interface{}, given interface{}) {
	for i := 0; i < retry; i++ {
		if !reflect.DeepEqual(expected, given) {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	assert.Equal(t, expected, given)
}
