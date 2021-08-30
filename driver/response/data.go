package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

type DataPacket struct {
	Table string
	Block *data.Block
}

func (s *DataPacket) Close() error {
	return s.Block.Close()
}

func readDataPacket(decoder *ch_encoding.Decoder, compress bool) (*DataPacket, error) {
	var (
		serverData DataPacket
		err        error
	)
	serverData.Table, serverData.Block, err = readBlock(decoder, compress)
	if err != nil {
		return nil, err
	}
	return &serverData, nil
}

func writeDataPacket(data *DataPacket, encoder *ch_encoding.Encoder, compress bool) error {
	return writeBlock(data.Table, data.Block, encoder, compress)
}

func (s *DataPacket) String() string {
	var buf strings.Builder
	s.Block.PrettyFmtBuild(&buf)
	return buf.String()
}

func (s *DataPacket) packet() {}
