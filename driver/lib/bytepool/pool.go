package bytepool

import (
	"sync/atomic"
	"time"
)

const (
	initialChannelSize = 10
	expandChannelDelta = 10
	monitorTime        = time.Second
	carryOverTimeout   = time.Second
)

var (
	MemoryLimit int64 = 1024 * 1024 * 1024
	memUsage    int64

	exp2Pools [32]*nPool
)

type nPool struct {
	bytesStream   chan []byte
	writeLock     chan struct{}
	lastMonitored time.Time
}

func init() {
	for i := range exp2Pools {
		exp2Pools[i] = &nPool{
			bytesStream:   make(chan []byte, initialChannelSize),
			writeLock:     make(chan struct{}, 1),
			lastMonitored: time.Now(),
		}
	}
}

func SetMemoryLimit(n int64) {
	MemoryLimit = n
}

func getExp2Pool(idx int) *nPool {
	return exp2Pools[idx]
}

func (p *nPool) optionalLockAcquire() (bool, func()) {
	select {
	case p.writeLock <- struct{}{}:
		return true, func() {
			<-p.writeLock
		}
	default:
		return false, nil
	}
}

func (p *nPool) monitor() {
	reported := time.Now()
	p.lastMonitored = reported
	time.Sleep(monitorTime)
	if p.lastMonitored == reported { // no usage since the last second
		p.executeWriteOptional(func() {
			p.resetChannel(reported)
		})
	}
}

func (p *nPool) resetChannel(reported time.Time) {
	oldStream := p.bytesStream
	newStream := make(chan []byte, initialChannelSize)
	p.bytesStream = newStream
	go carryOver(newStream, oldStream)
	p.flushWithUsageCheck(reported)
}

func (p *nPool) flushWithUsageCheck(t time.Time) {
	for b := range p.bytesStream {
		decrementMemUsage(cap(b))
		if p.lastMonitored != t {
			go PutBytes(b)
			return
		}
	}
}

func (p *nPool) expand() {
	oldStream := p.bytesStream
	newChCap := cap(oldStream) + expandChannelDelta
	newStream := make(chan []byte, newChCap)
	p.bytesStream = newStream
	go carryOver(newStream, oldStream)
}

func (p *nPool) executeWriteOptional(f func()) bool {
	ok, finish := p.optionalLockAcquire()
	if !ok {
		return false
	}
	defer finish()

	f()

	return true
}

func (p *nPool) latePut(buf []byte) {
	for withinLimitWhenAdd(buf) {
		select {
		case p.bytesStream <- buf:
			return
		default:
			if !p.executeWriteOptional(p.expand) {
				time.Sleep(time.Second)
			}
		}
	}
}

func carryOver(dst, src chan []byte) {
	for {
		select {
		case b := <-src:
			dst <- b
		case <-time.After(carryOverTimeout):
			return
		}
	}
}

func decrementMemUsage(n int) {
	atomic.StoreInt64(&memUsage, atomic.AddInt64(&memUsage, int64(-n)))
}

func incrementMemUsage(n int) {
	atomic.StoreInt64(&memUsage, atomic.AddInt64(&memUsage, int64(n)))
}

func withinLimitWhenAdd(buf []byte) bool {
	return memUsage+int64(cap(buf)) < MemoryLimit
}
