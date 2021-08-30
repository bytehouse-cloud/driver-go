package response

import (
	"fmt"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
)

type ProgressPacket struct {
	Rows      uint64
	Bytes     uint64
	TotalRows uint64

	DiskCacheBytes uint64
}

func (s *ProgressPacket) Close() error {
	return nil
}

func (s *ProgressPacket) String() string {
	return fmt.Sprint(*s)
}

func readProgressPacket(decoder *ch_encoding.Decoder, revision uint64) (*ProgressPacket, error) {
	var (
		p   ProgressPacket
		err error
	)
	if p.Rows, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.Bytes, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.TotalRows, err = decoder.Uvarint(); err != nil {
		return nil, err
	}

	if revision >= protocol.DBMS_MIN_REVISION_WITH_DISK_CACHE_HIT_RATIO {
		if p.DiskCacheBytes, err = decoder.Uvarint(); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func writeProgressPacket(progress *ProgressPacket, encoder *ch_encoding.Encoder, revision uint64) (err error) {
	if err = encoder.Uvarint(progress.Rows); err != nil {
		return err
	}
	if err = encoder.Uvarint(progress.Bytes); err != nil {
		return err
	}
	if err = encoder.Uvarint(progress.TotalRows); err != nil {
		return err
	}

	if revision >= protocol.DBMS_MIN_REVISION_WITH_DISK_CACHE_HIT_RATIO {
		if err = encoder.Uvarint(progress.DiskCacheBytes); err != nil {
			return err
		}
	}

	return nil
}

func (s *ProgressPacket) packet() {}
