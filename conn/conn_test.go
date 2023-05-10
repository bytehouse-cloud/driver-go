package conn

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool/mocks"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
	"github.com/bytehouse-cloud/driver-go/utils"
)

func TestNewGatewayConn(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Can throw error if wrong query setting",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig()
				g := NewGatewayConn(conf, "", NewPasswordAuthentication("u", "p"), false, map[string]interface{}{
					"boo": "baba",
				})
				require.Error(t, g.connect())
			},
		},
		{
			name: "Can throw error if cannot connect",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig()
				g := NewGatewayConn(conf, "", NewPasswordAuthentication("u", "p"), false, nil)
				require.Error(t, g.connect())
			},
		},
		{
			name: "Can throw error if connection timeout, random open strategy",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig(OptionHostName("localhost:123"), OptionDialStrategy(DialRandom), OptionConnTimeout(1))
				g := NewGatewayConn(conf, "", NewPasswordAuthentication("u", "p"), false, nil)
				require.Error(t, g.connect())
			},
		},
		{
			name: "Can throw error if connection timeout, in_order open strategy",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig(OptionDialStrategy(DialInOrder), OptionConnTimeout(1), OptionHostName("localhost:123"))
				g := NewGatewayConn(conf, "", NewPasswordAuthentication("u", "p"), false, nil)
				require.Error(t, g.connect())
			},
		},
		{
			name: "Can throw error if connection timeout, time_random open strategy",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig(OptionDialStrategy(DialTimeRandom), OptionConnTimeout(1), OptionHostName("localhost:123"))
				g := NewGatewayConn(conf, "", NewPasswordAuthentication("u", "p"), false, nil)
				require.Error(t, g.connect())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestGatewayConn_EncoderDecoder(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test Write String",
			test: func(t *testing.T) {
				buffer := bytes.NewBuffer([]byte(""))
				encoder := ch_encoding.NewEncoder(bufio.NewWriter(buffer))
				g := &GatewayConn{
					encoder: encoder,
				}
				err := g.writeString("test")
				require.NoError(t, err)
			},
		},
		{
			name: "Test Write Uvarint",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				g := &GatewayConn{
					encoder: encoder,
					decoder: decoder,
				}

				data2 := uint64(100)

				err := g.writeUvarint(data2)
				require.NoError(t, err)

				v, err := g.readUvariant()
				require.NoError(t, err)
				require.Equal(t, data2, v)
			},
		},
		{
			name: "Test Write Authentication",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				g := &GatewayConn{
					encoder:        encoder,
					decoder:        decoder,
					authentication: NewPasswordAuthentication("u", "p"),
				}

				require.NoError(t, g.writeAuthentication())
			},
		},
		{
			name: "Test Send Query",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)
				conf, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewPasswordAuthentication("u", "p"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}
				g.connected = true
				err := g.SendQuery("SELECT * FROM sample_table")
				require.NoError(t, err)
			},
		},
		{
			name: "Test Send Cancel",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)
				conf, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewPasswordAuthentication("u", "p"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}

				g.Cancel()
			},
		},
		{
			name: "Test Send Query with settings",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)
				conf, err := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					settings:       make(map[string]interface{}),
					authentication: NewPasswordAuthentication("u", "p"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}

				g.AddSettingChecked("min_compress_block_size", "100")
				g.AddSettingChecked("allow_experimental_cross_to_join_conversion", "true")
				g.AddSettingChecked("send_logs_level", "fatal")
				g.AddSettingChecked("totals_auto_threshold", "10.0")
				g.AddSettingChecked("optimize_subpart_number", "-1")
				g.AddSettingChecked("totals_auto_threshold", "1.02349")

				g.connected = true
				err = g.SendQuery("SELECT * FROM sample_table")
				require.NoError(t, err)
			},
		},
		{
			name: "Test Initial Exchange Success if returns serverhello",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)
				conf, err := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewPasswordAuthentication("u", "p"),
					logf:           func(s string, i ...interface{}) {},
				}

				err = g.writeUvarint(protocol.ServerHello)
				require.NoError(t, err)

				err = g.initialExchange()
				require.NoError(t, err)
			},
		},
		{
			name: "Test Initial Exchange Success if returns serverhello without auth token",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)
				conf, err := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewPasswordAuthentication("123", "123"),
					logf:           func(s string, i ...interface{}) {},
				}

				err = g.writeUvarint(protocol.ServerHello)
				require.NoError(t, err)

				err = g.initialExchange()
				require.NoError(t, err)
			},
		},
		{
			name: "Test Initial Exchange Success if returns serverhello without account and user",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				conf, err := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewPasswordAuthentication("u", "p"),
					logf:           func(s string, i ...interface{}) {},
				}

				err = g.writeUvarint(protocol.ServerHello)
				require.NoError(t, err)

				err = g.initialExchange()
				require.NoError(t, err)
			},
		},
		{
			name: "Test Check Connection",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				config, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: config,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewPasswordAuthentication("u", "p"),
				}

				g.connected = true
				require.NoError(t, g.CheckConnection())
			},
		},
		{
			name: "Test can close",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				newConf, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: newConf,
					conn: &connect{
						Conn:    &fakeConn{},
						zReader: mocks.NewFakedZReader(),
					},
					authentication: NewPasswordAuthentication("u", "p"),
				}

				err := g.Close()
				require.NoError(t, err)

				require.True(t, g.Closed())
			},
		},
		{
			name: "Test can set misc",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				conf, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewPasswordAuthentication("u", "p"),
				}

				g.SetLog(func(s string, i ...interface{}) {
					fmt.Printf("my log: "+s, i...)
				})
				g.Log("hello %s\n", "world")
				g.SetCurrentDatabase("somedatabase")
			},
		},
		{
			name: "Test can get display name",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				newConf, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connConfigs: newConf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					serverInfo: &data.ServerInfo{
						Name:         "hi",
						Revision:     0,
						MinorVersion: 0,
						MajorVersion: 0,
						Timezone:     nil,
						DisplayName:  "hello",
						VersionPatch: 0,
					},
					authentication: NewPasswordAuthentication("u", "p"),
				}

				require.Equal(t, g.GetDisplayName(), "hello")
				g.serverInfo.DisplayName = ""
				require.Equal(t, g.GetDisplayName(), "hi")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestGatewayConn_Settings(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)
	conf, _ := NewConnConfig(OptionDialStrategy(DialInOrder), OptionHostName("localhost:9000"))
	g := NewGatewayConn(
		conf, "default", NewPasswordAuthentication("default", ""),
		false, map[string]interface{}{},
	)

	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test add setting fail if setting not found",
			test: func(t *testing.T) {
				cur_con := g.Clone()
				err := cur_con.AddSetting("jack", "john")
				require.Error(t, err)
			},
		},
		{
			name: "Test add setting success if valid setting",
			test: func(t *testing.T) {
				cur_con := g.Clone()
				err := cur_con.AddSetting("min_compress_block_size", "123")
				require.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestGatewayConn_SettingsChecked(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test is ansi sql mode if added setting as true",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				g.AddSettingChecked("ansi_sql", true)
				require.True(t, g.InAnsiSQLMode())
			},
		},
		{
			name: "Test is ansi sql mode if added setting as ANSI",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				g.AddSettingChecked("dialect_type", "ANSI")
				require.True(t, g.InAnsiSQLMode())
			},
		},
		{
			name: "Test is ansi sql mode if added setting MYSQL",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				g.AddSettingChecked("dialect_type", "MYSQL")
				require.False(t, g.InAnsiSQLMode())
			},
		},
		{
			name: "Test is not ansi sql mode if added setting as false",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				g.AddSettingChecked("ansi_sql", false)
				require.False(t, g.InAnsiSQLMode())
			},
		},
		{
			name: "Test is not ansi sql mode if invalid values",
			test: func(t *testing.T) {
				var g *GatewayConn
				require.False(t, g.InAnsiSQLMode())

				g = &GatewayConn{}
				require.False(t, g.InAnsiSQLMode())

				g = &GatewayConn{
					settings: make(map[string]interface{}),
				}
				require.False(t, g.InAnsiSQLMode())
			},
		},
		{
			name: "Test if both settings are given THEN prioritize dialect type",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				g.AddSettingChecked("dialect_type", "MYSQL")
				g.AddSettingChecked("ansi_sql", true)
				require.False(t, g.InAnsiSQLMode())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
