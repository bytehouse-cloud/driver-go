package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type TableColumnsPacket struct {
	Table       string
	Description string
}

func (s *TableColumnsPacket) Close() error {
	return nil
}

func (s *TableColumnsPacket) String() string {
	var buf strings.Builder
	buf.WriteString("Table Columns: ")
	buf.WriteString(s.Description)
	return buf.String()
}

func (s *TableColumnsPacket) packet() {}

func readTableColumnsPacket(decoder *ch_encoding.Decoder) (*TableColumnsPacket, error) {
	var (
		err                error
		tableColumnsPacket TableColumnsPacket
	)
	if tableColumnsPacket.Table, err = decoder.String(); err != nil {
		return nil, err
	}
	if tableColumnsPacket.Description, err = decoder.String(); err != nil {
		return nil, err
	}
	return &tableColumnsPacket, nil
}

func writeTableColumnsPacket(tableColumns *TableColumnsPacket, encoder *ch_encoding.Encoder) (err error) {
	if err = encoder.String(tableColumns.Table); err != nil {
		return err
	}
	return encoder.String(tableColumns.Description)
}
