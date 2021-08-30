package sdk

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/conn"
)

func TestParseDSN(t *testing.T) {
	defaultLog := func(string2 string, args ...interface{}) {}
	defaultOpts := []conn.OptionConfig{
		conn.OptionReadTimeout(time.Minute),
		conn.OptionWriteTimeout(time.Minute),
		conn.OptionConnTimeout(3 * time.Second),
		conn.OptionLogf(defaultLog),
	}

	type args struct {
		dsn          string
		hostOverride func() (host string, err error)
		logger       func(s string, i ...interface{})
	}
	tests := []struct {
		name     string
		args     args
		want     *Config
		wantOpts []conn.OptionConfig
		wantErr  error
	}{
		{
			name: "Can parse simple dsn",
			args: args{
				dsn: "user:password@protocol(address)/dbname",
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewAuthentication("", "default", ""),
				querySettings:  map[string]string{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName(":"),
			},
		},
		{
			name: "Can accept region and map accordingly",
			args: args{
				dsn: "?region=" + conn.CnNorth1,
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewAuthentication("", "default", ""),
				querySettings:  map[string]string{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName("gateway.aws-cn-north-1.bytehouse.cn:19000"),
			},
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
				authentication: conn.NewAuthentication("", "default", ""),
				querySettings:  map[string]string{},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionLogf(defaultLog),
				conn.OptionHostName("goodmorning"),
			},
		},
		{
			name: "Can parse simple dsn with params",
			args: args{
				dsn: "user:password@protocol(address)/dbname?secure=true&write_timeout=100s&pool_size=2&target_account=10&replication_alter_columns_timeout=1",
			},
			want: &Config{
				databaseName:   "",
				authentication: conn.NewAuthentication("", "default", ""),
				compress:       false,
				querySettings: map[string]string{
					"replication_alter_columns_timeout": "1",
				},
			},
			wantOpts: []conn.OptionConfig{
				conn.OptionHostName(":"),
				conn.OptionWriteTimeout(100 * time.Second),
				conn.OptionSecure(true),
			},
		},
		{
			name: "Can throw ioErr if invalid dsn",
			args: args{
				dsn: "://usernafewfweijoofjewo/few?few***",
			},
			wantErr: errors.New("driver-go(sdk.ParseDSN): host port resolution error = parse \"://usernafewfweijoofjewo/few?few***\": missing protocol scheme"),
		},
		{
			name: "Can throw ioErr if invalid compress",
			args: args{
				dsn: "user:password@protocol(address)/dbname?compress=hi",
			},
			wantErr: errors.New("driver-go(sdk.ParseDSN): error parsing compress parameter as bool = strconv.ParseBool: parsing \"hi\": invalid syntax"),
		},
		{
			name: "Can throw ioErr if invalid duration",
			args: args{
				dsn: "user:password@protocol(address)/dbname?secure=true&write_timeout=100&pool_size=2",
			},
			wantErr: errors.New("driver-go(sdk.ParseDSN): makeConnConfigs error = driver-go(sdk.makeConnConfigs): error parsing time.Duration for 100, parameter = time: missing unit in duration \"100\""),
		},
	}
	for _, tt := range tests {
		t.Run(tt.args.dsn, func(t *testing.T) {
			if tt.args.logger == nil {
				tt.args.logger = defaultLog
			}
			got, err := ParseDSN(tt.args.dsn, tt.args.hostOverride, tt.args.logger)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tt.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)

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
