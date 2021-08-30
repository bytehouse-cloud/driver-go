package response

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type tableStatusPacket struct{}

func (t *tableStatusPacket) Close() error {
	return nil
}

func (t *tableStatusPacket) String() string {
	return "!tableStatusPacket: NotSupported"
}

func (t *tableStatusPacket) packet() {
}

func readTableStatusPacket(decoder *ch_encoding.Decoder) (*tableStatusPacket, error) {
	return nil, ErrTableStatusNotSupported
}

func writeTableStatusPacket(tsPacket *tableStatusPacket, encoder *ch_encoding.Encoder) error {
	return ErrTableStatusNotSupported
}

var ErrTableStatusNotSupported = errors.ErrorfWithCaller("table status packet unsupported")
