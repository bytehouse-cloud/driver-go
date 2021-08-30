package conn

import (
	"crypto/tls"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnConfig(t *testing.T) {
	tlsConfDefault := &tls.Config{}

	tests := []struct {
		name    string
		opts    []OptionConfig
		want    *ConnConfig
		wantErr bool
	}{
		{
			name: "given no config then no error",
			want: &ConnConfig{logf: noLog},
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
				OptionRegion(ApSouthEast1),
				OptionConnTimeout(time.Second),
				OptionReadTimeout(time.Second),
				OptionWriteTimeout(time.Second),
				OptionDialStrategy(DialRandom),
				OptionHostName("some_host"),
				OptionSecure(true),
				OptionSkipVerification(true),
				OptionNoDelay(true),
				OptionTlsConfig(tlsConfDefault),
			},
			want: &ConnConfig{
				secure:       true,
				skipVerify:   true,
				noDelay:      true,
				tlsConfig:    tlsConfDefault,
				hosts:        []string{"gateway.aws-ap-southeast-1.bytehouse.cloud:19000", "some_host"},
				connTimeout:  time.Second,
				readTimeout:  time.Second,
				writeTimeout: time.Second,
				dialStrategy: DialRandom,
				logf:         noLog,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConnConfig(tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(fmt.Sprint(got), fmt.Sprint(tt.want)) {
				t.Errorf("NewConnConfig() got = %v, want %v", got, tt.want)
			}
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
