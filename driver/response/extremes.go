package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

type ExtremesPacket struct {
	Table string
	Block *data.Block
}

func (s *ExtremesPacket) Close() error {
	return s.Block.Close()
}

func (s *ExtremesPacket) String() string {
	var buf strings.Builder
	buf.WriteString("ExtremesPacket:\n")
	s.Block.PrettyFmtBuild(&buf)
	return buf.String()
}

func (s *ExtremesPacket) packet() {}

func readExtremesPacket(decoder *ch_encoding.Decoder, compress bool) (*ExtremesPacket, error) {
	var (
		extremes ExtremesPacket
		err      error
	)
	extremes.Table, extremes.Block, err = readBlock(decoder, compress)
	if err != nil {
		return nil, err
	}
	return &extremes, nil
}

func writeExtremesPacket(extremes *ExtremesPacket, encoder *ch_encoding.Encoder, compress bool) error {
	return writeBlock(extremes.Table, extremes.Block, encoder, compress)
}
