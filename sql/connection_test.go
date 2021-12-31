package sql

import (
	"context"
	"database/sql/driver"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/sdk"
)

type sampleValuer struct{}

func (s *sampleValuer) Value() (driver.Value, error) {
	return driver.Value(1), nil
}

func TestCHConn_CheckNamedValue(t *testing.T) {
	type fields struct {
		Gateway *sdk.Gateway
	}
	type args struct {
		nv *driver.NamedValue
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "Int can pass",
			fields: fields{},
			args: args{
				nv: &driver.NamedValue{
					Name:    "jack",
					Ordinal: 0,
					Value:   323,
				},
			},
			wantErr: false,
		},
		{
			name:   "int8 can pass",
			fields: fields{},
			args: args{
				nv: &driver.NamedValue{
					Name:    "jack",
					Ordinal: 0,
					Value:   driver.Value(int8(3)),
				},
			},
			wantErr: false,
		},
		{
			name:   "uuid can pass",
			fields: fields{},
			args: args{
				nv: &driver.NamedValue{
					Name:    "jack",
					Ordinal: 0,
					Value:   driver.Value(uuid.New()),
				},
			},
			wantErr: false,
		},
		{
			name:   "array can pass",
			fields: fields{},
			args: args{
				nv: &driver.NamedValue{
					Name:    "jack",
					Ordinal: 0,
					Value:   driver.Value([]int{1}),
				},
			},
			wantErr: false,
		},
		{
			name:   "ip can pass",
			fields: fields{},
			args: args{
				nv: &driver.NamedValue{
					Name:    "jack",
					Ordinal: 0,
					Value:   driver.Value(net.IPv6zero),
				},
			},
			wantErr: false,
		},
		{
			name:   "driver valuer can pass",
			fields: fields{},
			args: args{
				nv: &driver.NamedValue{
					Name:    "jack",
					Ordinal: 0,
					Value:   driver.Value(sampleValuer{}),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CHConn{
				Gateway: tt.fields.Gateway,
			}
			if err := c.CheckNamedValue(tt.args.nv); (err != nil) != tt.wantErr {
				t.Errorf("CheckNamedValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConnection(t *testing.T) {
	ch := &CHConn{
		Gateway: &sdk.Gateway{Conn: conn.MockConn()},
	}

	err := ch.Ping(context.Background())
	require.NoError(t, err)

	require.NoError(t, ch.ResetSession(context.Background()))

	_, err = ch.ExecContext(context.Background(), "select 1", nil)
	require.Error(t, err)

	_, err = ch.QueryContext(context.Background(), "select 1", nil)
	require.Error(t, err)

	st, err := ch.PrepareContext(context.Background(), "select ?")
	require.NoError(t, err)
	stCtx, ok := st.(driver.StmtExecContext)
	require.True(t, ok)
	_, err = stCtx.ExecContext(context.Background(), []driver.NamedValue{
		{
			Name:    "hello",
			Ordinal: 0,
			Value:   123},
	})
	require.Error(t, err)
	require.Equal(t, "driver-go: named params not supported", err.Error())

	_, err = stCtx.ExecContext(context.Background(), []driver.NamedValue{
		{
			Ordinal: 0,
			Value:   123},
	})
	require.Error(t, err)
	require.NotEqual(t, "driver-go: named params not supported", err.Error())

	stqCtx, ok := st.(driver.StmtQueryContext)
	require.True(t, ok)
	_, err = stqCtx.QueryContext(context.Background(), []driver.NamedValue{
		{
			Name:    "hello",
			Ordinal: 0,
			Value:   123},
	})
	require.Error(t, err)
	require.Equal(t, "driver-go: named params not supported", err.Error())

	_, err = stqCtx.QueryContext(context.Background(), []driver.NamedValue{
		{
			Ordinal: 0,
			Value:   123},
	})
	require.Error(t, err)
	require.NotEqual(t, "driver-go: named params not supported", err.Error())

	require.NoError(t, st.Close())
	require.NoError(t, ch.Close())
	ch.Gateway.Conn.SendQuery("select 1")
	require.Equal(t, driver.ErrBadConn, ch.ResetSession(context.Background()))
	require.Equal(t, driver.ErrBadConn, ch.Ping(context.Background()))
}

func TestGetConn(t *testing.T) {
	chConn := &CHConn{
		Gateway: &sdk.Gateway{Conn: conn.MockConn()},
	}

	cn, err := GetConn(chConn)
	require.NoError(t, err)
	require.NotNil(t, cn)

	var x interface{}
	cn, err = GetConn(x)
	require.Error(t, err)
	require.Nil(t, cn)
}
