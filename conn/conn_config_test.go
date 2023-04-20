package conn

import (
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
)

func TestNewConnConfig(t *testing.T) {
	tests := []struct {
		name    string
		opts    []OptionConfig
		want    *ConnConfig
		wantErr bool
	}{
		{
			name: "given no config then no error",
			want: &ConnConfig{
				tlsConfig:             tlsConfDefault,
				connTimeoutSeconds:    settings.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC,
				receiveTimeoutSeconds: settings.DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC,
				sendTimeoutSeconds:    settings.DBMS_DEFAULT_SEND_TIMEOUT_SEC,
				dialStrategy:          DialRandom,
				logf:                  noLog,
			},
		},
		{
			name:    "given invalid region then error",
			opts:    []OptionConfig{OptionRegion("eaoeaoa")},
			wantErr: true,
		},
		{
			name:    "given invalid region then error",
			opts:    []OptionConfig{OptionRegion("eaoeaoa")},
			wantErr: true,
		},
		{
			name:    "given invalid tls config key then error",
			opts:    []OptionConfig{OptionTlsConfigFromRegistry("aoeaoe")},
			wantErr: true,
		},
		{
			name: "given full config then match same",
			opts: []OptionConfig{
				OptionRegion(RegionApSouthEast1),
				OptionConnTimeout(1),
				OptionReceiveTimeout(1),
				OptionSendTimeout(1),
				OptionDialStrategy(DialRandom),
				OptionHostName("some_host"),
				OptionSecure(true),
				OptionSkipVerification(true),
				OptionNoDelay(true),
				OptionTlsConfig(tlsConfDefault),
			},
			want: &ConnConfig{
				secure:                true,
				skipVerify:            true,
				noDelay:               true,
				tlsConfig:             tlsConfDefault,
				hosts:                 []string{"gateway.aws-ap-southeast-1.bytehouse.cloud:19000", "some_host"},
				connTimeoutSeconds:    1,
				receiveTimeoutSeconds: 1,
				sendTimeoutSeconds:    1,
				dialStrategy:          DialRandom,
				logf:                  noLog,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConnConfig(tt.opts...)
			if tt.wantErr && err != nil {
				return
			}

			assert.Equal(t, fmt.Sprint(got), fmt.Sprint(tt.want))
		})
	}
}

func TestRegisterTlsConfig(t *testing.T) {
	key := "some_key"
	RegisterTlsConfig(key, &tls.Config{})
	_, err := getTLSConfigClone(key)
	assert.Nil(t, err)
}

func TestOptionLogf(t *testing.T) {
	var tgt string
	var specialLog logf = func(s string, i ...interface{}) {
		tgt = s
	}
	conf, err := NewConnConfig(OptionLogf(specialLog))
	require.NoError(t, err)
	conf.logf("some_log")
	assert.Equal(t, tgt, "some_log")
}
