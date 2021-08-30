package response

import (
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type AggregateQueryPlanPacket struct {
	Plans []string
}

func (s *AggregateQueryPlanPacket) Close() error {
	return nil
}

func (s *AggregateQueryPlanPacket) String() string {
	var buf strings.Builder
	buf.WriteString("Aggregate Query Plan: [")
	for i := range s.Plans {
		buf.WriteString(s.Plans[i])
	}
	buf.WriteByte(squareCloseBracket)
	return buf.String()
}

func (s *AggregateQueryPlanPacket) packet() {}

func readAggQueryPlanPacket(decoder *ch_encoding.Decoder) (*AggregateQueryPlanPacket, error) {
	var (
		aggQueryPlan AggregateQueryPlanPacket
		err          error
	)
	aggQueryPlan.Plans, err = readStringSlice(decoder)
	if err != nil {
		return nil, err
	}
	return &aggQueryPlan, err
}

func writeAggQueryPlanPacket(aggPlan *AggregateQueryPlanPacket, encoder *ch_encoding.Encoder) (err error) {
	return writeStringSlice(aggPlan.Plans, encoder)
}
