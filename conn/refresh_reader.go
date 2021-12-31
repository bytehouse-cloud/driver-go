package conn

import (
	"time"
)

type SetReadDeadlineReader interface {
	Read([]byte) (int, error)
	SetReadDeadline(time.Time) error
}

// RefreshReader is a Reader that extends the read deadline
// by a fixed duration each time a read operation was carried out.
type RefreshReader struct {
	closed bool
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
	n, err := r.reader.Read(p)
	r.refresh(err)
	return n, err
}

func (r *RefreshReader) refresh(err error) {
	if err != nil {
		r.closed = true
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
		if r.closed {
			return
		}
	}
}
