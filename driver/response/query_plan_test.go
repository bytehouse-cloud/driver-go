package response

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryPlanPacket_String(t *testing.T) {
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
			want: "Query Plan: [fewfew]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &QueryPlanPacket{
				tt.fields.Plan,
			}
			require.Equal(t, s.String(), tt.want)
		})
	}
}
