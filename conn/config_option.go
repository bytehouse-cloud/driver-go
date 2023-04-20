package conn

import (
	"crypto/tls"
)

type OptionConfig func(connConfigs *ConnConfig) error

func OptionVolcano(region string) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		hp, err := resolveVolcanoRegion(region)
		if err != nil {
			return err
		}
		connConfigs.hosts = append(connConfigs.hosts, hp)
		connConfigs.secure = true
		return nil
	}
}

func OptionRegion(region string) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		hp, err := resolveRegion(region)
		if err != nil {
			return err
		}
		connConfigs.hosts = append(connConfigs.hosts, hp)
		connConfigs.secure = true
		return nil
	}
}

func OptionHostName(host string) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.hosts = append(connConfigs.hosts, host)
		return nil
	}
}

func OptionSecure(secure bool) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.secure = secure
		return nil
	}
}

func OptionSkipVerification(skip bool) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.skipVerify = skip
		return nil
	}
}

func OptionNoDelay(noDelay bool) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.noDelay = noDelay
		return nil
	}
}

type DialStrategy string

const (
	DialRandom     DialStrategy = "random"
	DialInOrder    DialStrategy = "in_order"
	DialTimeRandom DialStrategy = "time_random"
)

func OptionDialStrategy(strategy DialStrategy) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.dialStrategy = strategy
		return nil
	}
}

func OptionTlsConfig(tlsConfig *tls.Config) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.tlsConfig = tlsConfig
		return nil
	}
}

func OptionTlsConfigFromRegistry(key string) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		var err error
		connConfigs.tlsConfig, err = getTLSConfigClone(key)
		return err
	}
}

func OptionConnTimeout(d uint64) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.connTimeoutSeconds = d
		return nil
	}
}

func OptionSendTimeout(d uint64) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.sendTimeoutSeconds = d
		return nil
	}
}

func OptionReceiveTimeout(d uint64) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.receiveTimeoutSeconds = d
		return nil
	}
}

func OptionLogf(logf logf) OptionConfig {
	return func(connConfigs *ConnConfig) error {
		connConfigs.logf = logf
		return nil
	}
}
