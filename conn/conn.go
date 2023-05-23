package conn

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"strings"

	"github.com/google/uuid"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
	"github.com/bytehouse-cloud/driver-go/driver/response"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/sdk/param"
)

type GatewayConn struct {
	connConfigs *ConnConfig
	conn        *connect
	encoder     *ch_encoding.Encoder
	decoder     *ch_encoding.Decoder

	compress  bool
	connected bool
	inQuery   bool

	database       string
	userInfo       *UserInfo
	authentication Authentication
	serverInfo     *data.ServerInfo
	settings       map[string]interface{}

	logf func(string, ...interface{})

	clone func() *GatewayConn
}

func NewGatewayConn(
	connConfigs *ConnConfig,
	database string,
	authentication Authentication,
	compress bool,
	querySetting map[string]interface{},
) *GatewayConn {
	g := &GatewayConn{
		connConfigs:    connConfigs,
		compress:       compress,
		userInfo:       NewUserInfo(),
		authentication: authentication,
		serverInfo:     &data.ServerInfo{},
		settings:       querySetting,
		database:       database,
	}

	g.clone = func() *GatewayConn {
		return NewGatewayConn(g.connConfigs, g.database, g.authentication, g.compress, g.settings)
	}

	return g
}

func (g *GatewayConn) forceConnect() error {
	if g.connected {
		g.conn.UpdateTimeouts(g.connConfigs)
		return nil
	}

	if err := g.connect(); err != nil {
		return err
	}
	return g.flushConnConfigs(g.connConfigs)
}

func (g *GatewayConn) connect() error {
	newConn, err := dial(g.connConfigs)
	if err != nil {
		return err
	}

	g.conn = newConn
	g.logf = newConn.logf

	if g.compress {
		g.decoder = ch_encoding.NewDecoderWithCompress(g.conn)
		g.encoder = ch_encoding.NewEncoderWithCompress(g.conn)
	} else {
		g.decoder = ch_encoding.NewDecoder(g.conn)
		g.encoder = ch_encoding.NewEncoder(g.conn)
	}

	if err := g.initialExchange(); err != nil {
		return err
	}
	g.connected = true
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
	return g.authentication.WriteAuthProtocol(g.encoder)
}

func (g *GatewayConn) sendClientInfo() error {
	return data.WriteClientInfo(g.encoder)
}

func (g *GatewayConn) writeAuthentication() error {
	return g.authentication.WriteAuthData(g.encoder)
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

func (g *GatewayConn) CheckConnection() (err error) {
	defer func() {
		if err != nil {
			g.connected = false
		}
	}()

	if err := g.forceConnect(); err != nil {
		return err
	}
	return g.Ping()
}

func (g *GatewayConn) Ping() error {
	if err := g.writeUvarint(protocol.ClientPing); err != nil {
		return NewErrBadConnection(fmt.Sprintf("write uint64 error: %s", err))
	}
	if err := g.flush(); err != nil {
		return err
	}
	u, err := g.readUvariant()
	if err != nil {
		return NewErrBadConnection(fmt.Sprintf("read uvarint error: %s", err))
	}
	if u != protocol.ServerPong {
		return NewErrBadConnection(fmt.Sprintf("expected serverPong, got: %v", u))
	}
	return nil
}

func (g *GatewayConn) SendQuery(query string) error {
	return g.SendQueryFull(query, "", nil, "")
}

func (g *GatewayConn) SendQueryFull(query, queryID string, extTables <-chan *data.Block, extTableName string) error {
	err := g.forceConnect()
	if err != nil {
		return err
	}
	if err = g.writeUvarint(protocol.ClientQuery); err != nil {
		return err
	}
	if err = g.sendQueryInfo(query, queryID); err != nil {
		return err
	}
	if extTables != nil {
		for t := range extTables {
			if err = g.sendClientDataWithTableName(t, extTableName); err != nil {
				return err
			}
		}
	}
	if err = g.SendClientData(&data.Block{}); err != nil {
		return err
	}
	if err = g.flush(); err != nil {
		return err
	}

	g.inQuery = true
	return nil
}

func (g *GatewayConn) sendQueryInfo(query, queryID string) error {
	if queryID == "" {
		newUUID, err := uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("uuid random generation error: %s", err)
		}
		queryID = newUUID.String()
	}

	compression := protocol.CompressDisable
	if g.compress {
		compression = protocol.CompressEnable
	}

	var err error
	if err = g.writeString(queryID); err != nil {
		return err
	}
	if err = g.writeUvarint(protocol.InitialQuery); err != nil {
		return err
	}
	if err = g.writeString(g.authentication.Identity()); err != nil {
		return err
	}
	if err = g.writeString(queryID); err != nil {
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
	return g.writeString(query)
}

func (g *GatewayConn) sendUserInfo() error {
	return WriteUserInfoToEncoder(g.encoder, g.userInfo)
}

func (g *GatewayConn) SendClientData(block *data.Block) error {
	return g.sendClientDataWithTableName(block, "")
}

func (g *GatewayConn) sendClientDataWithTableName(block *data.Block, tableName string) error {
	var err error
	if err = g.writeUvarint(protocol.ClientData); err != nil {
		return err
	}
	if err = g.writeString(tableName); err != nil {
		return err
	}

	// TODO: may be able to simplify without flush
	if err = g.sendBlock(block); err != nil {
		return err
	}
	return g.flush()
}

// Cancel cancels the current query and disconnects.
// Non-blocking process
func (g *GatewayConn) Cancel() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		_ = g.writeUvarint(protocol.ClientCancel)
		_ = g.flush()
		_ = g.conn.Close()
	}()

	g.inQuery = false
	g.connected = false
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
	if g.conn == nil {
		return nil
	}
	return g.conn.Close()
}

func (g *GatewayConn) verifySingleSetting(name string, value interface{}) error {
	var sb strings.Builder
	sb.WriteString("SET ")
	sb.WriteString(name)
	sb.WriteString(" = ")
	writeSettingValueFmt(&sb, value)
	return g.SendQueryAssertNoError(context.Background(), sb.String())
}

// verifySettings send set settings query to server to verify query settings
func (g *GatewayConn) flushConnConfigs(configs *ConnConfig) error {
	if configs == nil {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("SET ")
	sb.WriteString(param.SEND_TIMEOUT)
	sb.WriteString(" = ")
	writeSettingValueFmt(&sb, configs.sendTimeoutSeconds)
	sb.WriteByte(',')
	sb.WriteString(param.RECEIVE_TIMEOUT)
	sb.WriteString(" = ")
	writeSettingValueFmt(&sb, configs.receiveTimeoutSeconds)

	query := sb.String()

	// skip query history for flushing conn configs
	revert := g.setByteHouseNonUserQuery()
	defer revert()
	return g.SendQueryAssertNoError(context.Background(), query)
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

func (g *GatewayConn) AddSettingsTemporarily(temp map[string]interface{}) func() {
	var toRemove []string
	originalDelta := make(map[string]interface{})

	for k, v := range temp {
		originalValue, exists := g.settings[k]
		if !exists {
			toRemove = append(toRemove, k)
		} else {
			originalDelta[k] = originalValue
		}

		g.settings[k] = v
	}

	return func() {
		for _, k := range toRemove {
			delete(g.settings, k)
		}
		for k, v := range originalDelta {
			g.settings[k] = v
		}
	}
}

func (g *GatewayConn) ApplyConnConfigs(configs map[string]interface{}) {
	for k, v := range configs {
		if k == param.SEND_TIMEOUT {
			sec, ok := v.(uint64)
			if !ok {
				continue
			}
			g.connConfigs.sendTimeoutSeconds = sec
		}
		if k == param.RECEIVE_TIMEOUT {
			sec, ok := v.(uint64)
			if !ok {
				continue
			}
			g.connConfigs.receiveTimeoutSeconds = sec
		}
	}
}

func (g *GatewayConn) ApplyConnConfigsTemporarily(configs map[string]interface{}) func() {
	originalSendTimeout := g.connConfigs.sendTimeoutSeconds
	originalReceiveTimeout := g.connConfigs.receiveTimeoutSeconds

	for k, v := range configs {
		if k == param.SEND_TIMEOUT {
			sec, ok := v.(uint64)
			if !ok {
				continue
			}
			g.connConfigs.sendTimeoutSeconds = sec
		}
		if k == param.RECEIVE_TIMEOUT {
			sec, ok := v.(uint64)
			if !ok {
				continue
			}
			g.connConfigs.receiveTimeoutSeconds = sec
		}
	}

	return func() {
		g.connConfigs.sendTimeoutSeconds = originalSendTimeout
		g.connConfigs.receiveTimeoutSeconds = originalReceiveTimeout
	}
}

func (g *GatewayConn) AddSetting(key string, value interface{}) error {
	// if err := g.verifySingleSetting(key, value); err != nil {
	//	return err
	// }
	g.settings[key] = value
	return nil
}

// AddSettingChecked assumes that the key and val passed are correct (key exist and val is of correct data type)
// and will not return error until query reaches the server.
// callers are expected to handle the checking on their own.
func (g *GatewayConn) AddSettingChecked(key string, val interface{}) {
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
	return !g.connected || g.conn.closed
}

func (g *GatewayConn) GetAllSettings() map[string]interface{} {
	return g.settings
}

func (g *GatewayConn) flush() error {
	return g.encoder.Flush()
}

func (g *GatewayConn) Clone() *GatewayConn {
	return g.clone()
}

func (g *GatewayConn) InQueryingState() bool {
	return g.inQuery
}

// setByteHouseNonUserQuery interprets queries to be not
// explicitly executed by users until callback function is called
func (g *GatewayConn) setByteHouseNonUserQuery() func() {
	if g.serverInfo.Name != "ByteHouse" {
		return func() {}
	}

	// backup
	logID, havelogID := g.settings["log_id"]
	skipHistory, haveSkipHistory := g.settings["skip_history"]

	// apply
	delete(g.settings, "log_id")
	g.settings["skip_history"] = true

	// revert
	return func() {
		if havelogID {
			g.settings["log_id"] = logID
		}
		if haveSkipHistory {
			g.settings["skip_history"] = skipHistory
		} else {
			g.settings["skip_history"] = false
		}
	}
}

func writeSettingValueFmt(sb *strings.Builder, v interface{}) {
	switch v := v.(type) {
	case string:
		sb.WriteByte('\'')
		sb.WriteString(v)
		sb.WriteByte('\'')
	case bool:
		if v {
			sb.WriteByte('1')
			return
		}
		sb.WriteByte('0')
	default:
		sb.WriteString(fmt.Sprint(v))
	}
}
