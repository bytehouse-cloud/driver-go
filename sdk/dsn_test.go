package sdk

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"

	"github.com/bytehouse-cloud/driver-go/conn"
)

func TestParseDSN(t *testing.T) {
	defaultLog := func(string2 string, args ...interface{}) {}
	defaultOpts := []conn.OptionConfig{
		conn.OptionReceiveTimeout(settings.DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC),
		conn.OptionSendTimeout(settings.DBMS_DEFAULT_SEND_TIMEOUT_SEC),
		conn.OptionConnTimeout(settings.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC),
		conn.OptionLogf(defaultLog),
	}

	type args struct {
		dsn          string
		hostOverride func() (host string, err error)
		logger       func(s string, i ...interface{})
	}
	tests := []struct {
		name              string
		args              args
		want              *Config
		wantOpts          []conn.OptionConfig
		wantQuerySettings map[string]interface{}
		wantErr           bool
	}{
		{
			name: "Can parse simple dsn",
			args: args{
				dsn: "user:password@protocol(address)/dbname",
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewPasswordAuthentication("default", ""),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName(":"),
			},
		},
		{
			name: "Can accept region with no volcano flag false and map accordingly",
			args: args{
				dsn: "?region=" + conn.RegionCnNorth1 + "&volcano=false",
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewPasswordAuthentication("default", ""),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName("gateway.aws-cn-north-1.bytehouse.cn:19000"),
				conn.OptionSecure(true),
			},
		},
		{
			name: "Can accept region and map accordingly",
			args: args{
				dsn: "?region=" + conn.RegionCnNorth1,
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewPasswordAuthentication("default", ""),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName("gateway.aws-cn-north-1.bytehouse.cn:19000"),
				conn.OptionSecure(true),
			},
		},
		{
			name: "Can accept volcano region and map accordingly",
			args: args{
				dsn: "?region=" + conn.RegionBoe + "&volcano=true",
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewPasswordAuthentication("default", ""),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName("gateway.volc-boe.offline.bytehouse.cn:19000"),
				conn.OptionSecure(true),
			},
		},
		{
			name: "Can reject volcano region if no volcano flag is given",
			args: args{
				dsn: "?region=" + conn.RegionBoe,
			},
			wantErr: true,
		},
		{
			name: "Can override host",
			args: args{
				dsn: "user:password@protocol(address)/dbname",
				hostOverride: func() (host string, err error) {
					return "goodmorning", nil
				},
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewPasswordAuthentication("default", ""),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionLogf(defaultLog),
				conn.OptionHostName("goodmorning"),
			},
		},
		{
			name: "Can parse simple dsn with params",
			args: args{
				dsn: "user:password@protocol(address)/dbname?secure=true&send_timeout=100&pool_size=2&target_account=10&replication_alter_columns_timeout=1&skip_history=true",
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewPasswordAuthentication("default", ""),
				compress:       false,
				querySettings: map[string]interface{}{
					"replication_alter_columns_timeout": uint64(1),
					"skip_history":                      true,
				},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName(":"),
				conn.OptionSecure(true),
				conn.OptionSendTimeout(100),
			},
		},
		{
			name: "Can throw ioErr if invalid dsn",
			args: args{
				dsn: "://usernafewfweijoofjewo/few?few***",
			},
			wantErr: true,
		},
		{
			name: "can accept user and password",
			args: args{
				dsn: "?user=mary&password=mary_password",
			},
			want: &Config{
				authentication: conn.NewPasswordAuthentication("mary", "mary_password"),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName(":"),
			},
		},
		{
			name: "If access key without region then err",
			args: args{
				dsn: "?access_key=abc&secret_key=def",
			},
			wantErr: true,
		},
		{
			name: "If access key without secret_key then err",
			args: args{
				dsn: "?access_key=abc&region=cn-north-1",
			},
			wantErr: true,
		},
		{
			name: "Can accept signature authentication",
			args: args{
				dsn: "?access_key=AK1899200289&secret_key=SK90189ASHUSHU17823&region=cn-north-1",
			},
			want: &Config{
				authentication: conn.NewSignatureAuthentication("AK1899200289", "SK90189ASHUSHU17823", "cn-north-1"),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionRegion(conn.RegionCnNorth1),
			},
		},
		{
			name: "Can accept system Authentication",
			args: args{
				dsn: "?token=abc123&is_system=true",
			},
			want: &Config{
				authentication: conn.NewSystemAuthentication("abc123"),
				querySettings:  map[string]interface{}{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName(":"),
			},
		},
		{
			name: "Can throw ioErr if invalid compress",
			args: args{
				dsn: "user:password@protocol(address)/dbname?compress=hi",
			},
			wantErr: true,
		},
		{
			name: "Can throw ioErr if invalid duration",
			args: args{
				dsn: "user:password@protocol(address)/dbname?secure=true&send_timeout=2w2&pool_size=2",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.args.dsn, func(t *testing.T) {
			if tt.args.logger == nil {
				tt.args.logger = defaultLog
			}
			got, err := ParseDSN(tt.args.dsn, tt.args.hostOverride, tt.args.logger)
			if err != nil {
				if tt.wantErr {
					return
				}
				require.NoError(t, err)
			}
			require.Equal(t, tt.want.databaseName, got.databaseName)

			// Conn options check
			opts := append(defaultOpts, tt.wantOpts...)
			wantConnConfig, err := conn.NewConnConfig(opts...)
			require.Equal(t, fmt.Sprint(wantConnConfig), fmt.Sprint(got.connConfig))

			require.Equal(t, tt.want.authentication, got.authentication)
			require.Equal(t, tt.want.compress, got.compress)
			require.Equal(t, tt.want.querySettings, got.querySettings)
		})
	}
}
