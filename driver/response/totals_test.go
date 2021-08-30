package response

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestTotalsPacket_String(t *testing.T) {
	type fields struct {
		Table string
		Block *data.Block
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Can parse string",
			fields: fields{
				Table: "Cool table",
				Block: func() *data.Block {
					b, _ := data.NewBlock([]string{"hi"}, []column.CHColumnType{column.UINT32}, 2)
					return b
				}(),
			},
			want: "Totals: \n┌─\u001B[1mhi\u001B[0m─┐\n│ 0  \u001B[0m│\n│\u001B[100m 0  \u001B[0m│\n└────┘\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &TotalsPacket{
				Table: tt.fields.Table,
				Block: tt.fields.Block,
			}
			require.Equal(t, s.String(), tt.want)
		})
	}
}
