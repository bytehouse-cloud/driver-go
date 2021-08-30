package bytehouse

import (
	"context"
	"fmt"
)

var EmptyConnectionContext = &ConnectionContext{
	Context: context.Background(),
	logf:    func(format string, a ...interface{}) {},
	getHost: func() (host string, err error) { return "", nil },
}

var DefaultConnectionContext = &ConnectionContext{
	Context: context.Background(),
	logf:    func(format string, a ...interface{}) { fmt.Printf(format, a...) },
	getHost: func() (host string, err error) { return "localhost:9000", nil },
}

func NewConnectionContext(
	logf func(s string, i ...interface{}),
	getHost func() (string, error),
) *ConnectionContext {
	if logf == nil {
		logf = EmptyConnectionContext.logf
	}
	if getHost == nil {
		getHost = EmptyConnectionContext.getHost
	}

	return &ConnectionContext{
		logf:    logf,
		getHost: getHost,
	}
}

type ConnectionContext struct {
	context.Context
	logf    func(string, ...interface{})
	getHost func() (string, error)
}

// SetLogf sets the logger for connector. default logger is no-op function
func (c *ConnectionContext) SetLogf(logf func(format string, a ...interface{})) {
	c.logf = logf
}

func (c *ConnectionContext) GetLogf() func(format string, a ...interface{}) {
	return c.logf
}

// SetResolveHost sets a callback function to resolve hostname for sql.Open and sdk.Open
// Once this is set, caller should no longer be providing full url, but only the url values.
// e.g. "tcp://localhost:9000?user=default&password=pass" -> "user=default&password=pass"
func (c *ConnectionContext) SetResolveHost(resolveHost func() (string, error)) {
	c.getHost = resolveHost
}

func (c *ConnectionContext) GetResolveHost() func() (string, error) {
	return c.getHost
}
