package conn

import (
	"bufio"
	"crypto/tls"
	errors2 "errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

var tick int32
var ErrNoHost = errors2.New("conn configs have no hosts")

func dial(configs *ConnConfig) (*connect, error) {
	var (
		err error
		abs = func(v int) int {
			if v < 0 {
				return -1 * v
			}
			return v
		}
		conn  net.Conn
		ident = abs(int(atomic.AddInt32(&tick, 1)))
	)
	tlsConfig := configs.tlsConfig
	if configs.secure {
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}
		tlsConfig.InsecureSkipVerify = configs.skipVerify
	}
	if len(configs.hosts) == 0 {
		return nil, ErrNoHost
	}

	checkedHosts := make(map[int]struct{}, len(configs.hosts))
	for i := range configs.hosts {
		var num int
		switch configs.dialStrategy {
		case DialInOrder:
			num = i
		case DialRandom, "":
			num = (ident + i) % len(configs.hosts)
		case DialTimeRandom:
			// select host based on milliseconds
			num = int((time.Now().UnixNano()/1000)%1000) % len(configs.hosts)
			for _, ok := checkedHosts[num]; ok; _, ok = checkedHosts[num] {
				num = int(time.Now().UnixNano()) % len(configs.hosts)
			}
			checkedHosts[num] = struct{}{}
		}
		switch {
		case configs.secure:
			conn, err = tls.DialWithDialer(
				&net.Dialer{
					Timeout: configs.connTimeout,
				},
				"tcp",
				configs.hosts[num],
				tlsConfig,
			)
		default:
			conn, err = net.DialTimeout("tcp", configs.hosts[num], configs.connTimeout)
		}
		if err == nil {
			configs.logf(
				"[dial] secure=%t, skip_verify=%t, strategy=%s, ident=%d, server=%d -> %s",
				configs.secure,
				configs.skipVerify,
				configs.dialStrategy,
				ident,
				num,
				conn.RemoteAddr(),
			)
			if tcp, ok := conn.(*net.TCPConn); ok {
				err = tcp.SetNoDelay(configs.noDelay) // Disable or enable the Nagle Algorithm for this tcp socket
				if err != nil {
					return nil, err
				}
			}

			return &connect{
				Conn:         conn,
				logf:         configs.logf,
				ident:        ident,
				zReader:      bytepool.NewZReaderDefault(conn),
				readTimeout:  configs.readTimeout,
				writeTimeout: configs.writeTimeout,
			}, nil
		} else {
			configs.logf(
				"[dial err] secure=%t, skip_verify=%t, strategy=%s, ident=%d, addr=%s\n%#v\n",
				configs.secure,
				configs.skipVerify,
				configs.dialStrategy,
				ident,
				configs.hosts[num],
				err,
			)
		}
	}

	return nil, err
}

type connect struct {
	net.Conn
	logf         func(string, ...interface{})
	ident        int
	buffer       *bufio.Reader
	closed       bool
	readTimeout  time.Duration
	writeTimeout time.Duration
	zReader      *bytepool.ZReader
}

func (conn *connect) Read(b []byte) (int, error) {
	err := conn.zReader.ReadFull(b)
	_ = conn.SetReadDeadline(time.Now().Add(conn.readTimeout))
	return len(b), err
}

func (conn *connect) ReadUvarint() (uint64, error) {
	return conn.zReader.ReadUvarint()
}

func (conn *connect) Write(b []byte) (int, error) {
	var (
		n      int
		err    error
		total  int
		srcLen = len(b)
	)
	for total < srcLen {
		if n, err = conn.Conn.Write(b[total:]); err != nil {
			return n, err
		}
		total += n
		_ = conn.SetWriteDeadline(time.Now().Add(conn.writeTimeout))
	}
	return n, nil
}

func (conn *connect) Close() error {
	if conn.closed {
		return nil
	}

	if err := conn.Conn.Close(); err != nil {
		return err
	}

	conn.closed = true
	return nil
}
