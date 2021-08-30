package response

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
)

func TestReadPacket(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Read ServerHello packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerHello)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.Equal(t, &HelloPacket{}, p)
				require.Equal(t, "Hello", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerData packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerData)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &DataPacket{}, p)
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerException packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerException)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &ExceptionPacket{}, p)
				require.Equal(t, "[code: 0, name: , , stack trace: ]", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerProgress packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerProgress)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &ProgressPacket{}, p)
				require.Equal(t, "{0 0 0 0}", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerPong packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerPong)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &PongPacket{}, p)
				require.Equal(t, "Pong", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerEndOfStream packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerEndOfStream)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &EndOfStreamPacket{}, p)
				require.Equal(t, "End of Stream", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerProfileInfo packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerProfileInfo)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &ProfilePacket{}, p)
				require.Equal(t, "Profile Info: [Rows: 0, Bytes: 0, Blocks: 0, Applied Limit: false, Rows Before Limit: 0, Calculated Rows Before Limit: false]", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerTotals packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerTotals)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &TotalsPacket{}, p)
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerExtremes packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerExtremes)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &ExtremesPacket{}, p)
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerTablesStatus packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerTablesStatus)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.Equal(t, err, ErrTableStatusNotSupported)
				require.IsType(t, &tableStatusPacket{}, p)
				require.Equal(t, "!tableStatusPacket: NotSupported", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerLog packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerLog)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &LogPacket{}, p)
				require.Equal(t, "", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerTableColumns packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerTableColumns)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &TableColumnsPacket{}, p)
				require.Equal(t, "Table Columns: ", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerQueryPlan packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerQueryPlan)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &QueryPlanPacket{}, p)
				require.Equal(t, "Query Plan: []", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerAggQueryPlan packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerAggQueryPlan)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &AggregateQueryPlanPacket{}, p)
				require.Equal(t, "Aggregate Query Plan: []", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Read ServerQueryMetadata packet",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, protocol.ServerQueryMetadata)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.NoError(t, err)
				require.IsType(t, &QueryMetadataPacket{}, p)
				require.Equal(t, "Query Metadata: [QueryID: ]", p.String())
				require.NoError(t, p.Close())
			},
		},
		{
			name: "Throws exception if unknown packet type",
			test: func(t *testing.T) {
				b := make([]byte, 1000)
				binary.LittleEndian.PutUint64(b, 100)
				decoder := ch_encoding.NewDecoder(bytepool.NewZReader(bytes.NewReader(b), 100, 100))
				p, err := ReadPacket(decoder, false, 0)
				require.Error(t, err)
				require.Equal(t, "driver-go(response.ReadPacket): unknown packet type: 100", err.Error())
				require.Nil(t, p)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

type invalidPacket struct {
}

func (i *invalidPacket) packet() {
}

func (i *invalidPacket) String() string {
	return ""
}

func (i *invalidPacket) Close() error {
	return nil
}

func TestWritePacket(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Write ServerHello packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&HelloPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerData packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&DataPacket{
					Table: "",
					Block: &data.Block{},
				}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerException packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&ExceptionPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerProgress packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&ProgressPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerPong packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&PongPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerEndOfStream packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&EndOfStreamPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerProfileInfo packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&ProfilePacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerTotals packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&TotalsPacket{
					Table: "",
					Block: &data.Block{},
				}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerExtremes packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&ExtremesPacket{
					Table: "",
					Block: &data.Block{},
				}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerTablesStatus packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&tableStatusPacket{}, encoder, false, 0)
				require.Equal(t, err, ErrTableStatusNotSupported)
			},
		},
		{
			name: "Write ServerLog packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&LogPacket{
					Table: "",
					Block: &data.Block{},
				}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerTableColumns packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&TableColumnsPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerQueryPlan packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&QueryPlanPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerAggQueryPlan packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&AggregateQueryPlanPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Write ServerQueryMetadata packet",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&QueryMetadataPacket{}, encoder, false, 0)
				require.NoError(t, err)
			},
		},
		{
			name: "Throws exception if unknown packet type",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				err := WritePacket(&invalidPacket{}, encoder, false, 0)
				require.Error(t, err)
				require.Equal(t, "driver-go(response.WritePacket): unknown packet type: *response.invalidPacket", err.Error())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
