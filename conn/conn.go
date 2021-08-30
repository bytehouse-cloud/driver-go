package conn

import (
	"bufio"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type GatewayConn struct {
	connOptions *ConnConfig
	conn        *connect
	writer      *bufio.Writer
	encoder     *ch_encoding.Encoder
	decoder     *ch_encoding.Decoder
	compress    bool

	database       string
	userInfo       *UserInfo
	authentication *Authentication
	serverInfo     *data.ServerInfo
	settings       map[string]interface{}

	logf func(string, ...interface{})

	clone func() (*GatewayConn, error)
}

func NewGatewayConn(
	connOptions *ConnConfig,
	databaseName string,
	authentication *Authentication,
	compress bool,
	querySetting map[string]string,
) (*GatewayConn, error) {
	g := &GatewayConn{
		connOptions:    connOptions,
		compress:       compress,
		userInfo:       NewUserInfo(),
		authentication: authentication,
		serverInfo:     &data.ServerInfo{},
		settings:       make(map[string]interface{}),
		database:       databaseName,
	}

	// set initial query settings
	for k, v := range querySetting {
		if err := g.AddSetting(k, v); err != nil {
			return nil, err
		}
	}

	if err := g.connect(); err != nil {
		return nil, err
	}

	g.clone = func() (*GatewayConn, error) {
		newConn, err := NewGatewayConn(connOptions, databaseName, authentication, compress, querySetting)
		if err != nil {
			return nil, err
		}
		for k, v := range g.settings {
			newConn.settings[k] = v
		}
		return newConn, nil
	}

	return g, nil
}

func (g *GatewayConn) connect() error {
	newConn, err := dial(g.connOptions)
	if err != nil {
		return err
	}

	g.conn = newConn
	g.logf = newConn.logf

	g.writer = bufio.NewWriter(g.conn)

	if g.compress {
		g.decoder = ch_encoding.NewDecoderWithCompress(g.conn)
		g.encoder = ch_encoding.NewEncoderWithCompress(g.writer)
	} else {
		g.decoder = ch_encoding.NewDecoder(g.conn)
		g.encoder = ch_encoding.NewEncoder(g.writer)
	}

	if err := g.initialExchange(); err != nil {
		return err
	}
	return nil
}

func (g *GatewayConn) initialExchange() error {
	var err error
	if err = g.sendHello(); err != nil {
		return err
	}
	return g.receiveHello()
}

func (g *GatewayConn) sendHello() error {
	var err error

	if err = g.sendHelloProtocol(); err != nil {
		return err
	}
	if err = g.writeString(data.ClientName); err != nil {
		return err
	}
	if err = g.sendClientInfo(); err != nil {
		return err
	}
	if err = g.writeString(g.database); err != nil {
		return err
	}
	if err = g.writeAuthentication(); err != nil {
		return err
	}
	return g.flush()
}

func (g *GatewayConn) sendHelloProtocol() error {
	return g.writeUvarint(protocol.ClientHello)
}

func (g *GatewayConn) sendClientInfo() error {
	return data.WriteClientInfo(g.encoder)
}

func (g *GatewayConn) writeAuthentication() error {
	return g.authentication.WriteToEncoder(g.encoder)
}

func (g *GatewayConn) receiveHello() error {
	var (
		resp response.Packet
		err  error
	)

	resp, err = response.ReadPacket(g.decoder, g.compress, data.ClickHouseRevision)
	if err != nil {
		return err
	}
	switch resp := resp.(type) {
	case *response.HelloPacket:
	case *response.ExceptionPacket:
		return resp
	default:
		return errors.ErrorfWithCaller(expectedServerHello, resp)
	}

	g.serverInfo, err = data.ReadServerInfo(g.decoder)
	if err != nil {
		return err
	}
	g.logf(g.serverInfo.String())
	return nil
}

func (g *GatewayConn) CheckConnection() error {
	if err := g.writeUvarint(protocol.ClientPing); err != nil {
		return NewErrBadConnection(fmt.Sprintf("(func: %s) writeUint64 error = %s", errors.GetFunctionName(g.CheckConnection), err))
	}
	if err := g.flush(); err != nil {
		return err
	}
	u, err := g.readUvariant()
	if err != nil {
		return NewErrBadConnection(fmt.Sprintf("(func: %s) readUvariant error = %s", errors.GetFunctionName(g.CheckConnection), err))
	}
	if u != protocol.ServerPong {
		_ = g.conn.Close()
		return NewErrBadConnection(fmt.Sprintf("(func: %s) expected serverPong, received = %v", errors.GetFunctionName(g.CheckConnection), u))
	}
	return nil
}

// todo: add context and watch for cancellation
func (g *GatewayConn) SendQuery(query string) error {
	return g.SendQueryWithExternalTable(query, nil, "")
}

func (g *GatewayConn) SendQueryWithExternalTable(query string, extTables <-chan *data.Block, extTableName string) error {
	var err error
	if err = g.writeUvarint(protocol.ClientQuery); err != nil {
		return err
	}
	if err = g.sendQueryInfo(query); err != nil {
		return err
	}
	if extTables != nil {
		for t := range extTables {
			if err = g.SendClientDataWithTableName(t, extTableName); err != nil {
				return err
			}
		}
	}
	if err = g.SendClientData(&data.Block{}); err != nil {
		return err
	}
	return g.flush()
}

func (g *GatewayConn) sendQueryInfo(queryString string) error {
	newUUID, err := uuid.NewRandom()
	if err != nil {
		return errors.ErrorfWithCaller("uuid new random error: %s", err)
	}
	uuidString := newUUID.String()
	compression := protocol.CompressDisable
	if g.compress {
		compression = protocol.CompressEnable
	}

	if err = g.writeString(uuidString); err != nil {
		return err
	}
	if err = g.writeUvarint(protocol.InitialQuery); err != nil {
		return err
	}
	if err = g.writeString(g.authentication.username); err != nil {
		return err
	}
	if err = g.writeString(uuidString); err != nil {
		return err
	}
	if err = g.writeString(g.conn.LocalAddr().String()); err != nil {
		return err
	}
	if err = g.writeUvarint(TCP); err != nil {
		return err
	}
	if err = g.sendUserInfo(); err != nil {
		return err
	}
	if err = g.sendClientInfo(); err != nil {
		return err
	}
	if err = g.writeString(""); err != nil { // quota key
		return err
	}
	if err = g.writeUvarint(data.ClickHouseRevision); err != nil {
		return err
	}
	if err = g.sendSettings(); err != nil {
		return err
	}
	if err = g.writeUvarint(protocol.StageComplete); err != nil { // Query Stage
		return err
	}
	if err = g.writeUvarint(compression); err != nil {
		return err
	}
	return g.writeString(queryString)
}

func (g *GatewayConn) sendUsernameOrToken() error {
	if g.authentication.token != "" {
		return g.writeString(g.authentication.token)
	}
	return g.writeString(g.authentication.username)
}

func (g *GatewayConn) sendUserInfo() error {
	return WriteUserInfoToEncoder(g.encoder, g.userInfo)
}

func (g *GatewayConn) SendClientData(block *data.Block) error {
	return g.SendClientDataWithTableName(block, "")
}

func (g *GatewayConn) SendClientDataWithTableName(block *data.Block, tableName string) error {
	var err error
	if err = g.writeUvarint(protocol.ClientData); err != nil {
		return err
	}
	if err = g.writeString(tableName); err != nil {
		return err
	}
	if err = g.sendBlock(block); err != nil {
		return err
	}
	return g.flush()
}

func (g *GatewayConn) SendCancel() error {
	if err := g.writeUvarint(protocol.ClientCancel); err != nil {
		return err
	}
	return g.flush()
}

func (g *GatewayConn) sendBlock(block *data.Block) error {
	g.encoder.SelectCompress(g.compress)
	if err := data.WriteBlockToEncoder(g.encoder, block); err != nil {
		return errors.ErrorfWithCaller("data write error: %s", err)
	}
	g.encoder.SelectCompress(false)
	return nil
}

func (g *GatewayConn) Close() error {
	return g.conn.Close()
}

func (g *GatewayConn) sendSettings() error {
	var err error

	for k, v := range g.settings {
		if err = g.encoder.String(k); err != nil {
			return err
		}
		switch v := v.(type) {
		case string:
			if err = g.encoder.String(v); err != nil {
				return err
			}
		case int64:
			if err = g.encoder.Uvarint(uint64(v)); err != nil {
				return err
			}
		case uint64:
			if err = g.encoder.Uvarint(v); err != nil {
				return err
			}
		case bool:
			if err = g.encoder.Bool(v); err != nil {
				return err
			}
		case float32, float64:
			s := fmt.Sprint(v)
			if err = g.encoder.String(s); err != nil {
				return err
			}
			g.settings[k] = s
		}
	}

	return g.writeString("")
}

func (g *GatewayConn) AddSetting(key string, value interface{}) error {
	value, err := settings.SettingToValue(key, value)
	if err != nil {
		return err
	}
	g.settings[key] = value
	return nil
}

// AddSettingsChecked assumes that the key and val passed are correct (key exist and val is of correct data type)
// and will not return error until query reaches the server.
// callers are expected to handle the checking on their own.
func (g *GatewayConn) AddSettingsChecked(key string, val interface{}) {
	g.settings[key] = val
}

func (g *GatewayConn) GetDisplayName() string {
	if g.serverInfo.DisplayName != "" {
		return g.serverInfo.DisplayName
	}
	return g.serverInfo.Name
}

func (g *GatewayConn) InAnsiSQLMode() bool {
	if g == nil {
		return false
	}
	if g.settings == nil {
		return false
	}
	v, ok := g.settings["ansi_sql"]
	if !ok {
		return false
	}
	b := v.(bool)
	return b
}

func (g *GatewayConn) Log(s string, args ...interface{}) {
	g.logf(s, args...)
}

func (g *GatewayConn) SetLog(logf func(string, ...interface{})) {
	g.logf = logf
}

func (g *GatewayConn) SetCurrentDatabase(database string) {
	g.database = database
}

// Closed returns true iff conn is closed
func (g *GatewayConn) Closed() bool {
	return g.conn.closed
}

func (g *GatewayConn) GetAllSettings() map[string]interface{} {
	return g.settings
}

func (g *GatewayConn) flush() error {
	if err := g.encoder.Flush(); err != nil {
		return err
	}
	return g.conn.SetReadDeadline(time.Now().Add(g.connOptions.readTimeout))
}

func (g *GatewayConn) Clone() (*GatewayConn, error) {
	return g.clone()
}
