package conn

import (
	"crypto/tls"
	"time"
)

type logf func(string, ...interface{})

var (
	noLog          logf = func(s string, i ...interface{}) {}
	tlsConfDefault      = &tls.Config{}
)

func NewConnConfig(opts ...OptionConfig) (*ConnConfig, error) {
	newConnConfigs := &ConnConfig{
		logf:         noLog,
		tlsConfig:    tlsConfDefault,
		connTimeout:  time.Second,
		dialStrategy: DialRandom,
		readTimeout:  time.Minute,
		writeTimeout: time.Minute,
	}

	for _, opt := range opts {
		if err := opt(newConnConfigs); err != nil {
			return nil, err
		}
	}

	return newConnConfigs, nil
}

type ConnConfig struct {
	secure, skipVerify, noDelay            bool
	tlsConfig                              *tls.Config
	hosts                                  []string
	connTimeout, readTimeout, writeTimeout time.Duration
	dialStrategy                           DialStrategy
	logf                                   func(string, ...interface{})
}
