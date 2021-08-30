package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

type TotalsPacket struct {
	Table string
	Block *data.Block
}

func (s *TotalsPacket) Close() error {
	return s.Block.Close()
}

func (s *TotalsPacket) String() string {
	buf := strings.Builder{}
	buf.WriteString("Totals: \n")
	s.Block.PrettyFmtBuild(&buf)
	return buf.String()
}

func (s TotalsPacket) packet() {}

func readTotalsPacket(decoder *ch_encoding.Decoder, compress bool) (*TotalsPacket, error) {
	var (
		totals TotalsPacket
		err    error
	)
	totals.Table, totals.Block, err = readBlock(decoder, compress)
	if err != nil {
		return nil, err
	}
	return &totals, nil
}

func writeTotalsPacket(totals *TotalsPacket, encoder *ch_encoding.Encoder, compress bool) (err error) {
	return writeBlock(totals.Table, totals.Block, encoder, compress)
}
