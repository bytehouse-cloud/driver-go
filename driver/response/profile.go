package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type ProfilePacket struct {
	Rows                      uint64
	Blocks                    uint64
	Bytes                     uint64
	AppliedLimit              bool
	RowsBeforeLimit           uint64
	CalculatedRowsBeforeLimit bool
}

func (s *ProfilePacket) Close() error {
	return nil
}

func (s *ProfilePacket) String() string {
	var buf strings.Builder
	buf.WriteString("Profile Info: [")
	buf.WriteString("Rows: ")
	buf.WriteString(formatUint64(s.Rows))
	buf.WriteString(commaSep)
	buf.WriteString("Bytes: ")
	buf.WriteString(formatUint64(s.Bytes))
	buf.WriteString(commaSep)
	buf.WriteString("Blocks: ")
	buf.WriteString(formatUint64(s.Blocks))
	buf.WriteString(commaSep)
	buf.WriteString("Applied Limit: ")
	buf.WriteString(formatBool(s.AppliedLimit))
	buf.WriteString(commaSep)
	buf.WriteString("Rows Before Limit: ")
	buf.WriteString(formatUint64(s.RowsBeforeLimit))
	buf.WriteString(commaSep)
	buf.WriteString("Calculated Rows Before Limit: ")
	buf.WriteString(formatBool(s.CalculatedRowsBeforeLimit))
	buf.WriteByte(squareCloseBracket)
	return buf.String()
}

func readProfilePacket(decoder *ch_encoding.Decoder) (*ProfilePacket, error) {
	var (
		err error
		p   ProfilePacket
	)
	if p.Rows, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.Blocks, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.Bytes, err = decoder.Uvarint(); err != nil {
		return nil, err
	}

	if p.AppliedLimit, err = decoder.Bool(); err != nil {
		return nil, err
	}
	if p.RowsBeforeLimit, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if p.CalculatedRowsBeforeLimit, err = decoder.Bool(); err != nil {
		return nil, err
	}

	return &p, nil
}

func writeProfilePacket(profile *ProfilePacket, encoder *ch_encoding.Encoder) (err error) {
	if err = encoder.Uvarint(profile.Rows); err != nil {
		return err
	}
	if err = encoder.Uvarint(profile.Blocks); err != nil {
		return err
	}
	if err = encoder.Uvarint(profile.Bytes); err != nil {
		return err
	}

	if err = encoder.Bool(profile.AppliedLimit); err != nil {
		return err
	}
	if err = encoder.Uvarint(profile.RowsBeforeLimit); err != nil {
		return err
	}
	if err = encoder.Bool(profile.CalculatedRowsBeforeLimit); err != nil {
		return err
	}

	return nil
}

func (s *ProfilePacket) packet() {}
