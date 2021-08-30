package response

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestLogPacket_String(t *testing.T) {
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
			want: "Log: [hi: 0]\n\nLog: [hi: 0]\n",
		},
		{
			name: "Can parse string if 2 cols",
			fields: fields{
				Table: "Cool table",
				Block: func() *data.Block {
					b, _ := data.NewBlock([]string{"hi", "hi2"}, []column.CHColumnType{column.UINT32, column.UINT32}, 2)
					return b
				}(),
			},
			want: "Log: [hi: 0, hi2: 0]\n\nLog: [hi: 0, hi2: 0]\n",
		},
		{
			name: "Can return empty string if no block",
			fields: fields{
				Table: "Cool table",
				Block: nil,
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &LogPacket{
				Table: tt.fields.Table,
				Block: tt.fields.Block,
			}
			require.Equal(t, s.String(), tt.want)
		})
	}
}
