package conn

import (
	"crypto/tls"

	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
)

type logf func(string, ...interface{})

var (
	noLog          logf = func(s string, i ...interface{}) {}
	tlsConfDefault      = &tls.Config{}
)

func NewConnConfig(opts ...OptionConfig) (*ConnConfig, error) {
	newConnConfigs := &ConnConfig{
		logf:                  noLog,
		tlsConfig:             tlsConfDefault,
		connTimeoutSeconds:    settings.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC,
		dialStrategy:          DialRandom,
		receiveTimeoutSeconds: settings.DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC,
		sendTimeoutSeconds:    settings.DBMS_DEFAULT_SEND_TIMEOUT_SEC,
	}

	for _, opt := range opts {
		if err := opt(newConnConfigs); err != nil {
			return nil, err
		}
	}

	return newConnConfigs, nil
}

type ConnConfig struct {
	secure, skipVerify, noDelay                                   bool
	tlsConfig                                                     *tls.Config
	hosts                                                         []string
	connTimeoutSeconds, receiveTimeoutSeconds, sendTimeoutSeconds uint64 //in seconds
	dialStrategy                                                  DialStrategy
	logf                                                          func(string, ...interface{})
}
