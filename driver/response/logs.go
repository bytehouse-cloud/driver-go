package response

import (
	"strings"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

type LogPacket struct {
	Table string
	Block *data.Block
}

func (s *LogPacket) Close() error {
	return s.Block.Close()
}

func (s *LogPacket) String() string {
	if s == nil || s.Block == nil {
		return emptyString
	}
	var buf strings.Builder
	for i := 0; i < s.Block.NumRows; i++ {
		if i > 0 {
			buf.WriteByte(newline)
		}
		buf.WriteString("Log: [")
		for j := 0; j < len(s.Block.Columns); j++ {
			if j > 0 {
				buf.WriteString(commaSep)
			}
			buf.WriteString(s.Block.Columns[j].Name)
			buf.WriteString(mapSep)
			buf.WriteString(s.Block.Columns[j].Data.GetString(i))
		}
		buf.WriteString("]")
		buf.WriteByte(newline)
	}
	return buf.String()
}

func (s *LogPacket) packet() {}

func readLogPacket(decoder *ch_encoding.Decoder, location *time.Location) (*LogPacket, error) {
	var (
		serverLog LogPacket
		err       error
	)
	serverLog.Table, serverLog.Block, err = readBlockWithLocation(decoder, false, location)
	if err != nil {
		return nil, err
	}
	return &serverLog, nil
}

func writeLogPacket(log *LogPacket, encoder *ch_encoding.Encoder) error {
	return writeBlock(log.Table, log.Block, encoder, false)
}
