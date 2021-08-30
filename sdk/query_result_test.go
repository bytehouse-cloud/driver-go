package sdk

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/driver/response"
)

func TestRows(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Can read data from channel",
			test: func(t *testing.T) {
				ch := make(chan response.Packet, 1)
				b, _ := data.NewBlock([]string{"dog"}, []column.CHColumnType{column.UINT32}, 2)
				dp := &response.DataPacket{
					Table: "cool_table",
					Block: b,
				}
				ch <- dp
				close(ch)
				qr := NewQueryResult(ch, func() {})

				reader := qr.ExportToReader("CSV")
				bs, err := ioutil.ReadAll(reader)
				require.Nil(t, err)

				expected := `0
0`
				require.Equal(t, expected, string(bs))
				require.NoError(t, qr.Exception())
				require.NoError(t, qr.Close())
			},
		},
		{
			name: "Can create insert query result",
			test: func(t *testing.T) {
				ch := make(chan response.Packet, 1)
				b, _ := data.NewBlock([]string{"dog"}, []column.CHColumnType{column.UINT32}, 2)
				dp := &response.LogPacket{
					Table: "cool_table",
					Block: b,
				}
				qr := NewInsertQueryResult(ch)
				ch <- dp
				close(ch)

				// Sleep to wait for goroutine in insert result to finish
				time.Sleep(100 * time.Millisecond)

				logs := qr.GetAllLogs()
				require.Equal(t, 1, len(logs))

				meta := qr.GetAllMeta()
				require.Equal(t, 1, len(meta))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
