package sdk

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	bytehouse "github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/sdk/param"
)

var (
	ErrParsingParamFmt       = "error parsing %T for %v, parameter = %v"
	defaultConnectionTimeout = 3 * time.Second
	defaultReadTimeout       = time.Minute
	defaultWriteTimeout      = time.Minute
)

// Config is a configuration parsed from a DSN string.
type Config struct {
	connConfig     *conn.ConnConfig
	databaseName   string
	authentication *conn.Authentication
	//impersonation  *conn.Impersonation
	compress      bool
	querySettings map[string]string
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
		return nil, errors.ErrorfWithCaller("host port resolution error = %v", err)
	}

	connOptions, err := makeConnConfigs(host, urlValues, logf)
	if err != nil {
		return nil, errors.ErrorfWithCaller("makeConnConfigs error = %v", err)
	}

	databaseName := urlValues.Get("database")

	authentication := makeAuthentication(urlValues)

	//impersonation, err := makeImpersonation(urlValues)
	if err != nil {
		return nil, errors.ErrorfWithCaller("makeImpersonation error = %v", err)
	}

	compress, err := parseBool(urlValues.Get(param.COMPRESS))
	if err != nil {
		return nil, errors.ErrorfWithCaller("error parsing compress parameter as bool = %v", err)
	}

	querySettings := makeQuerySettings(urlValues)

	return &Config{
		connConfig:     connOptions,
		databaseName:   databaseName,
		authentication: authentication,
		//impersonation:  impersonation,
		compress:      compress,
		querySettings: querySettings,
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

func makeQuerySettings(query url.Values) map[string]string {
	qs := make(map[string]string)

	// set settings from dsn
	for k := range query {
		if _, ok := settings.Default[k]; ok {
			qs[k] = query.Get(k)
		}
	}

	return qs
}

func makeConnConfigs(host string, urlValues url.Values, logf func(s string, i ...interface{})) (*conn.ConnConfig, error) {
	var opts []conn.OptionConfig

	if logf != nil {
		opts = append(opts, conn.OptionLogf(logf))
	}

	if region := urlValues.Get(param.REGION); region != "" {
		opts = append(opts, conn.OptionRegion(conn.Region(region)))
		host = ""
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
			return nil, errors.ErrorfWithCaller(ErrParsingParamFmt, b, secure, err)
		}
		opts = append(opts, conn.OptionSecure(b))
	}

	if skipVerification := urlValues.Get(param.SKIP_VERIFICATION); skipVerification != "" {
		b, err := strconv.ParseBool(skipVerification)
		if err != nil {
			return nil, errors.ErrorfWithCaller(ErrParsingParamFmt, b, skipVerification, err)
		}
		opts = append(opts, conn.OptionSkipVerification(b))
	}

	if noDelay := urlValues.Get(param.NO_DELAY); noDelay != "" {
		b, err := strconv.ParseBool(noDelay)
		if err != nil {
			return nil, errors.ErrorfWithCaller(ErrParsingParamFmt, b, noDelay, err)
		}
		opts = append(opts, conn.OptionNoDelay(b))
	}

	if connTimeout := urlValues.Get(param.CONNECTION_TIMEOUT); connTimeout != "" {
		duration, err := time.ParseDuration(connTimeout)
		if err != nil {
			return nil, errors.ErrorfWithCaller(ErrParsingParamFmt, duration, connTimeout, err)
		}
		opts = append(opts, conn.OptionConnTimeout(duration))
	} else {
		opts = append(opts, conn.OptionConnTimeout(defaultConnectionTimeout))
	}

	if readTimeout := urlValues.Get(param.READ_TIMEOUT); readTimeout != "" {
		duration, err := time.ParseDuration(readTimeout)
		if err != nil {
			return nil, errors.ErrorfWithCaller(ErrParsingParamFmt, duration, readTimeout, err)
		}
		opts = append(opts, conn.OptionReadTimeout(duration))
	} else {
		opts = append(opts, conn.OptionReadTimeout(defaultReadTimeout))
	}

	if writeTimeout := urlValues.Get(param.WRITE_TIMEOUT); writeTimeout != "" {
		duration, err := time.ParseDuration(writeTimeout)
		if err != nil {
			return nil, errors.ErrorfWithCaller(ErrParsingParamFmt, duration, writeTimeout, err)
		}
		opts = append(opts, conn.OptionWriteTimeout(duration))
	} else {
		opts = append(opts, conn.OptionWriteTimeout(defaultWriteTimeout))
	}

	return conn.NewConnConfig(opts...)
}

func makeAuthentication(urlValues url.Values) *conn.Authentication {
	username := urlValues.Get(param.USER)
	if username == "" {
		username = "default"
	}
	accountID := urlValues.Get(param.ACCOUNT)
	if accountID != "" {
		username = fmt.Sprintf("%v::%v", accountID, username)
	}
	password := urlValues.Get(param.PASSWORD)
	token := urlValues.Get(param.TOKEN)
	return conn.NewAuthentication(token, username, password)
}

func parseBool(s string) (bool, error) {
	if s == "" {
		return false, nil
	}
	return strconv.ParseBool(s)
}
