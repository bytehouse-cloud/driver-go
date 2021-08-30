package response

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAggregateQueryPlanPacket_String(t *testing.T) {
	type fields struct {
		Plan []string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Can parse string",
			fields: fields{
				Plan: []string{"few", "few"},
			},
			want: "Aggregate Query Plan: [fewfew]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &AggregateQueryPlanPacket{
				tt.fields.Plan,
			}
			require.Equal(t, s.String(), tt.want)
		})
	}
}
