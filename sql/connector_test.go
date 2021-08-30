package sql

import (
	"database/sql/driver"
	"testing"

	bytehouse "github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/utils"
)

func Test_connector_Connect(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		dsn         string
		connContext *bytehouse.ConnectionContext
	}
	tests := []struct {
		name    string
		args    args
		want    driver.Conn
		wantErr bool
	}{
		{
			name: "Valid dsn returns valid db",
			args: args{
				dsn:         "tcp://localhost:9000?user=default&compress=true",
				connContext: bytehouse.EmptyConnectionContext,
			},
			wantErr: false,
		},
		{
			name: "Invalid dsn returns errChan",
			args: args{
				dsn:         "tcp://localhost:9000poo?user=default&compress=true",
				connContext: bytehouse.EmptyConnectionContext,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := GatewayDriver{}
			got, err := d.Open(tt.args.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantExecErr %v", err, tt.wantErr)
				return
			}
			if _, ok := got.(driver.Conn); !tt.wantErr && !ok {
				t.Errorf("Open() got = %v, return type is not driver.Conn", got)
			}
		})
	}
}
