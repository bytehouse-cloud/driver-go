package conn

import (
	"crypto/tls"
	"fmt"
	"sync"
)

var (
	tlsConfigLock     sync.RWMutex
	tlsConfigRegistry map[string]*tls.Config
)

func init() {
	tlsConfigRegistry = make(map[string]*tls.Config)
}

func RegisterTlsConfig(key string, config *tls.Config) {
	tlsConfigRegistry[key] = config
}

func getTLSConfigClone(key string) (*tls.Config, error) {
	tlsConfigLock.RLock()
	defer tlsConfigLock.RUnlock()

	v, ok := tlsConfigRegistry[key]
	if !ok {
		return nil, fmt.Errorf("[makeConnConfigs] invalid tls_config - no config registered under name: %s, no tls_config will be used", key)
	}
	return v.Clone(), nil
}
