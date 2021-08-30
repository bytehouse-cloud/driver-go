package sql

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/sdk"
)

func TestRows(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Can check columns",
			test: func(t *testing.T) {
				ch := make(chan response.Packet, 1)
				b, _ := data.NewBlock([]string{"dog"}, []column.CHColumnType{column.UINT32}, 2)
				dp := &response.DataPacket{
					Table: "cool_table",
					Block: b,
				}
				ch <- dp
				r := &rows{
					columnNames: nil,
					queryResult: sdk.NewQueryResult(ch, func() {}),
				}
				close(ch)

				require.Equal(t, r.Columns(), []string{"dog"})
				require.Equal(t, r.ColumnTypeDatabaseTypeName(0), string(column.UINT32))
				require.Equal(t, r.ColumnTypeScanType(0).Kind(), reflect.Uint32)
			},
		},
		{
			name: "Empty rows can execute methods without panic or error",
			test: func(t *testing.T) {
				r := &emptyR{}
				require.NotNil(t, r.Columns())
				require.NoError(t, r.Close())
				require.NoError(t, r.Next([]driver.Value{}))
			},
		},
		{
			name: "Can get and close query result",
			test: func(t *testing.T) {
				value := uint32(100)
				ch := make(chan response.Packet, 1)
				b, _ := data.NewBlock([]string{"dog"}, []column.CHColumnType{column.UINT32}, 2)
				for _, col := range b.Columns {
					_, _ = col.Data.ReadFromValues([]interface{}{value, value})
				}

				dp := &response.DataPacket{
					Table: "cool_table",
					Block: b,
				}
				ch <- dp

				r := &rows{
					columnNames: nil,
					queryResult: sdk.NewQueryResult(ch, func() {}),
				}
				close(ch)

				rValues := make([]driver.Value, 1)
				require.NoError(t, r.Next(rValues))
				require.Equal(t, rValues[0], value)
				require.NoError(t, r.Next(rValues))
				require.Equal(t, rValues[0], value)
				require.Equal(t, r.Next(rValues), io.EOF)
				require.NoError(t, r.Close())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
