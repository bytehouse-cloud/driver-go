package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type QueryMetadataPacket struct {
	QueryID string
}

func (s *QueryMetadataPacket) Close() error {
	return nil
}

func (s *QueryMetadataPacket) String() string {
	var buf strings.Builder
	buf.WriteString("Query Metadata: [")
	buf.WriteString("QueryID: ")
	buf.WriteString(s.QueryID)
	buf.WriteByte(squareCloseBracket)
	return buf.String()
}

func (s *QueryMetadataPacket) packet() {}

func readQueryMetadataPacket(decoder *ch_encoding.Decoder) (*QueryMetadataPacket, error) {
	var (
		queryMeta QueryMetadataPacket
		err       error
	)
	queryMeta.QueryID, err = decoder.String()
	if err != nil {
		return nil, err
	}
	return &queryMeta, nil
}

func writeQueryMetadataPacket(queryMeta *QueryMetadataPacket, encoder *ch_encoding.Encoder) (err error) {
	err = encoder.String(queryMeta.QueryID)
	if err != nil {
		return err
	}
	return nil
}
