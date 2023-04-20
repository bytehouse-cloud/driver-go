package conn

import (
	"errors"
	"sync"
	"time"
)

//go:generate mockgen -source=./refresh_reader.go -destination=mocks/refresh_reader.go
type SetReadDeadlineReader interface {
	Read([]byte) (int, error)
	SetReadDeadline(time.Time) error
}

// RefreshReader is a Reader that extends the read deadline
// by a fixed duration each time a read operation was carried out.
type RefreshReader struct {
	closed bool
	lock   sync.Mutex
	signal chan struct{}
	reader SetReadDeadlineReader
}

func NewRefreshReader(r SetReadDeadlineReader, reset time.Duration) *RefreshReader {
	newRefreshReader := &RefreshReader{
		signal: make(chan struct{}, 1),
		reader: r,
	}

	go newRefreshReader.asyncRefreshTimeout(reset)
	return newRefreshReader
}

func (r *RefreshReader) Read(p []byte) (int, error) {
	if r.isClose() {
		return 0, errors.New(readOnCloseRefreshReader)
	}

	n, err := r.reader.Read(p)
	r.refresh(err)
	return n, err
}

func (r *RefreshReader) refresh(err error) {
	if err != nil {
		r.Close()
		return
	}

	if r.isClose() { // ignore if the refreshReader is closed alr;
		return
	}

	// optionally send refresh signal
	select {
	case r.signal <- struct{}{}:
	default:
	}

	return
}

func (r *RefreshReader) asyncRefreshTimeout(reset time.Duration) {
	// minimun refresh rate
	interval := reset / 2

	for {
		r.reader.SetReadDeadline(time.Now().Add(reset))
		time.Sleep(interval)
		<-r.signal
		if r.isClose() {
			return
		}
	}
}

func (r *RefreshReader) Close() error {
	if err := r.tryToClose(); err != nil {
		return nil // just to make sure that when we are closing on alr closed RefreshReader -> not return error
	}

	// optionally send refresh signal if signal channel is not full
	select {
	case r.signal <- struct{}{}:
	default:
	}

	return nil
}

func (r *RefreshReader) isClose() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.closed
}

func (r *RefreshReader) tryToClose() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closed {
		return errors.New(closeOnCloseRefreshReader)
	}
	r.closed = true
	return nil
}
