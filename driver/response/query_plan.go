package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type QueryPlanPacket struct {
	Plans []string
}

func (s *QueryPlanPacket) Close() error {
	return nil
}

func (s *QueryPlanPacket) String() string {
	var buf strings.Builder
	buf.WriteString("Query Plan: [")
	for i := range s.Plans {
		buf.WriteString(s.Plans[i])
	}
	buf.WriteByte(squareCloseBracket)
	return buf.String()
}

func (s *QueryPlanPacket) packet() {}

func readQueryPlanPacket(decoder *ch_encoding.Decoder) (*QueryPlanPacket, error) {
	var (
		queryPlan QueryPlanPacket
		err       error
	)
	queryPlan.Plans, err = readStringSlice(decoder)
	if err != nil {
		return nil, err
	}
	return &queryPlan, nil
}

func writeQueryPlanPacket(qp *QueryPlanPacket, encoder *ch_encoding.Encoder) error {
	return writeStringSlice(qp.Plans, encoder)
}
