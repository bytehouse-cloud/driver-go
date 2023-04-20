package sdk

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	bytehouse "github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
	"github.com/bytehouse-cloud/driver-go/sdk/param"
)

var (
	ErrParseParamFmt         = "dsn parse error, name: %v, type: %T, given: %v, error: %s"
	ErrDsnMissingSecretKey   = errors.New("missing secret key in dsn")
	ErrDsnMissingRegion      = errors.New("missing region in dsn")
	ErrTokenAuthNotSupported = errors.New("token authentication not supported")
)

// Config is a configuration parsed from a DSN string.
type Config struct {
	connConfig     *conn.ConnConfig
	databaseName   string
	authentication conn.Authentication
	compress       bool
	querySettings  map[string]interface{}
}

type (
	HostOverride func() (host string, err error)
	Logf         func(s string, args ...interface{})
)

// ParseDSN returns a new config used to connect to database
func ParseDSN(dsn string, hostOverride HostOverride, logf Logf) (*Config, error) {
	if logf == nil {
		logf = bytehouse.EmptyConnectionContext.GetLogf()
	}
	if hostOverride == nil {
		hostOverride = bytehouse.EmptyConnectionContext.GetResolveHost()
	}

	host, urlValues, err := parseAndResolveHost(dsn, hostOverride)
	if err != nil {
		return nil, err
	}

	connOptions, err := makeConnConfigs(host, urlValues, logf)
	if err != nil {
		return nil, err
	}

	databaseName := urlValues.Get("database")

	authentication, err := makeAuthentication(urlValues)
	if err != nil {
		return nil, err
	}

	compress, err := parseBool(urlValues.Get(param.COMPRESS))
	if err != nil {
		return nil, err
	}

	querySettings, err := makeQuerySettings(urlValues)
	if err != nil {
		return nil, err
	}

	return &Config{
		connConfig:     connOptions,
		databaseName:   databaseName,
		authentication: authentication,
		compress:       compress,
		querySettings:  querySettings,
	}, nil
}

func parseAndResolveHost(dsn string, override func() (string, error)) (string, url.Values, error) {
	host, err := override()
	if err != nil {
		return "", nil, err
	}
	if host != "" {
		urlValues, err := url.ParseQuery(dsn)
		if err != nil {
			return "", nil, err
		}
		return host, urlValues, nil
	}

	dsnURL, err := url.Parse(dsn)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("%v:%v", dsnURL.Hostname(), dsnURL.Port()), dsnURL.Query(), nil
}

func makeQuerySettings(query url.Values) (map[string]interface{}, error) {
	qs := make(map[string]interface{})

	for k := range query {
		if _, ok := settings.Default[k]; ok {
			v, err := settings.SettingToValue(k, query.Get(k))
			if err != nil {
				return nil, err
			}
			qs[k] = v
		}
	}

	return qs, nil
}

func makeConnConfigs(host string, urlValues url.Values, logf func(s string, i ...interface{})) (*conn.ConnConfig, error) {
	var opts []conn.OptionConfig

	if logf != nil {
		opts = append(opts, conn.OptionLogf(logf))
	}

	if region := urlValues.Get(param.REGION); region != "" {
		host = ""
		if volcano := urlValues.Get(param.VOLCANO); volcano != "" {
			isVolc, err := strconv.ParseBool(volcano)
			if err != nil {
				return nil, fmt.Errorf(ErrParseParamFmt, param.VOLCANO, isVolc, volcano, err)
			}
			if isVolc {
				opts = append(opts, conn.OptionVolcano(region))
			} else {
				opts = append(opts, conn.OptionRegion(region))
			}
		} else {
			opts = append(opts, conn.OptionRegion(region))
		}
	}

	if host != "" {
		opts = append(opts, conn.OptionHostName(host))
	}

	if connStrategy := urlValues.Get(param.CONNECTION_OPEN_STRATEGY); connStrategy != "" {
		opts = append(opts, conn.OptionDialStrategy(conn.DialStrategy(connStrategy)))
	}

	if tlsConfig := urlValues.Get(param.TLS_CONFIG); tlsConfig != "" {
		opts = append(opts, conn.OptionTlsConfigFromRegistry(tlsConfig))
	}

	if altHosts := urlValues.Get(param.ALT_HOSTS); altHosts != "" {
		for _, h := range strings.Split(altHosts, ",") {
			opts = append(opts, conn.OptionHostName(h))
		}
	}

	if secure := urlValues.Get(param.SECURE); secure != "" {
		b, err := strconv.ParseBool(secure)
		if err != nil {
			return nil, fmt.Errorf(ErrParseParamFmt, param.SECURE, b, secure, err)
		}
		opts = append(opts, conn.OptionSecure(b))
	}

	if skipVerification := urlValues.Get(param.SKIP_VERIFICATION); skipVerification != "" {
		b, err := strconv.ParseBool(skipVerification)
		if err != nil {
			return nil, fmt.Errorf(ErrParseParamFmt, param.SKIP_VERIFICATION, b, skipVerification, err)
		}
		opts = append(opts, conn.OptionSkipVerification(b))
	}

	if noDelay := urlValues.Get(param.NO_DELAY); noDelay != "" {
		b, err := strconv.ParseBool(noDelay)
		if err != nil {
			return nil, fmt.Errorf(ErrParseParamFmt, b, noDelay, err)
		}
		opts = append(opts, conn.OptionNoDelay(b))
	}

	if connTimeout := urlValues.Get(param.CONNECTION_TIMEOUT); connTimeout != "" {
		duration, err := parseUint(connTimeout)
		if err != nil {
			return nil, fmt.Errorf(ErrParseParamFmt, param.CONNECTION_TIMEOUT, duration, connTimeout, err)
		}
		opts = append(opts, conn.OptionConnTimeout(duration))
	} else {
		opts = append(opts, conn.OptionConnTimeout(settings.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC))
	}

	if receiveTimeout := urlValues.Get(param.RECEIVE_TIMEOUT); receiveTimeout != "" {
		duration, err := parseUint(receiveTimeout)
		if err != nil {
			return nil, fmt.Errorf(ErrParseParamFmt, param.RECEIVE_TIMEOUT, duration, receiveTimeout, err)
		}
		opts = append(opts, conn.OptionReceiveTimeout(duration))
	} else {
		opts = append(opts, conn.OptionReceiveTimeout(settings.DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC))
	}

	if sendTimeout := urlValues.Get(param.SEND_TIMEOUT); sendTimeout != "" {
		duration, err := parseUint(sendTimeout)
		if err != nil {
			return nil, fmt.Errorf(ErrParseParamFmt, param.SEND_TIMEOUT, duration, sendTimeout, err)
		}
		opts = append(opts, conn.OptionSendTimeout(duration))
	} else {
		opts = append(opts, conn.OptionSendTimeout(settings.DBMS_DEFAULT_SEND_TIMEOUT_SEC))
	}

	return conn.NewConnConfig(opts...)
}

func makeAuthentication(urlValues url.Values) (conn.Authentication, error) {
	accessKey := urlValues.Get(param.ACCESS_KEY)
	region := strings.ToLower(urlValues.Get(param.REGION))

	// Try using AK/SK authentication
	if accessKey != "" {
		secretKey := urlValues.Get(param.SECRET_KEY)
		if secretKey == "" {
			return nil, ErrDsnMissingSecretKey
		}
		if region == "" {
			return nil, ErrDsnMissingRegion
		}
		return conn.NewSignatureAuthentication(accessKey, secretKey, region), nil
	}

	token := urlValues.Get(param.TOKEN)
	if token != "" {
		isSystemS := urlValues.Get(param.IS_SYSTEM)
		if isSystemS != "" {
			isSystem, err := parseBool(isSystemS)
			if err != nil {
				return nil, fmt.Errorf("expect bool for is_system")
			}
			if isSystem {
				return conn.NewSystemAuthentication(token), nil
			}
		}
		return conn.NewAPITokenAuthentication(token), nil
	}

	username := urlValues.Get(param.USER)
	account := urlValues.Get(param.ACCOUNT)
	password := urlValues.Get(param.PASSWORD)
	if username == "" {
		username = "default"
	}
	if account != "" {
		username = fmt.Sprintf("%v::%v", account, username)
	}
	return conn.NewPasswordAuthentication(username, password), nil
}

func parseBool(s string) (bool, error) {
	if s == "" {
		return false, nil
	}
	return strconv.ParseBool(s)
}

func parseUint(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}
