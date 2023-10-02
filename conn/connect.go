package conn

import (
	"bufio"
	"crypto/tls"
	errors2 "errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/utils/pointer"
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

	// set empty tls config
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

	// loop through all the give host and attempt to connect any of them
	// order of connection is given by client
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

		// use tls if secure flag is used
		switch {
		case configs.secure:
			conn, err = tls.DialWithDialer(
				&net.Dialer{
					Timeout: time.Duration(configs.connTimeoutSeconds) * time.Second,
				},
				"tcp",
				configs.hosts[num],
				tlsConfig,
			)
		default:
			conn, err = net.DialTimeout("tcp", configs.hosts[num], time.Duration(configs.connTimeoutSeconds)*time.Second)
		}

		if err == nil {
			// log which idx and address being used
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
			rReader := NewRefreshReader(conn, time.Duration(configs.receiveTimeoutSeconds)*time.Second)

			// return successful connection
			return &connect{
				Conn:           conn,
				logf:           configs.logf,
				ident:          ident,
				sendTimeout:    time.Duration(configs.sendTimeoutSeconds) * time.Second,
				receiveTimeout: time.Duration(configs.receiveTimeoutSeconds) * time.Second,
				refreshReader:  rReader, // this is outside the zReader to configure receive_timeout
				zReader:        bytepool.NewZReaderDefault(pointer.IoReader(rReader)),
				bWriter:        bufio.NewWriter(conn),
			}, nil
		}

		// log error and try another host
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

	// no more host to try, return final error
	return nil, err
}

type connect struct {
	net.Conn
	logf           func(string, ...interface{})
	ident          int
	closed         bool
	receiveTimeout time.Duration
	sendTimeout    time.Duration
	refreshReader  *RefreshReader
	zReader        *bytepool.ZReader
	bWriter        *bufio.Writer
}

func (conn *connect) Read(b []byte) (int, error) {
	err := conn.zReader.ReadFull(b)
	return len(b), err
}

func (conn *connect) ReadUvarint() (uint64, error) {
	return conn.zReader.ReadUvarint()
}

func (conn *connect) resetReceiveTimeout() {
	conn.SetReadDeadline(time.Now().Add(conn.receiveTimeout))
}

func (conn *connect) Write(b []byte) (int, error) {
	return conn.bWriter.Write(b)
}

func (conn *connect) Flush() error {
	defer conn.resetSendTimeout()
	defer conn.resetReceiveTimeout()
	return conn.bWriter.Flush()
}

func (conn *connect) resetSendTimeout() {
	conn.SetWriteDeadline(time.Now().Add(conn.sendTimeout))
}

func (conn *connect) Close() error {
	if conn.closed {
		return nil
	}

	if err := conn.Conn.Close(); err != nil {
		return err
	}

	if err := conn.zReader.Close(); err != nil {
		return err
	}

	conn.closed = true
	return nil
}

func (conn *connect) UpdateTimeouts(config *ConnConfig) {
	if config == nil {
		return
	}
	conn.sendTimeout = time.Duration(config.sendTimeoutSeconds) * time.Second
	conn.receiveTimeout = time.Duration(config.receiveTimeoutSeconds) * time.Second
}
