package conn

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
)

func TestNewGatewayConn(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Can throw error if wrong query setting",
			test: func(t *testing.T) {
				_, err := NewGatewayConn(nil, "", &Authentication{},
					false, map[string]string{
						"boo": "baba",
					})
				require.Error(t, err)
				require.Equal(t, "driver-go(settings.SettingToValue): query settings not found: boo", err.Error())
			},
		},
		{
			name: "Can throw error if cannot connect",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig()
				_, err := NewGatewayConn(conf, "", &Authentication{}, false, nil)
				require.Error(t, err)
				require.Equal(t, "conn configs have no hosts", err.Error())
			},
		},
		{
			name: "Can throw error if connection timeout, random open strategy",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig(OptionHostName("localhost:123"), OptionDialStrategy(DialRandom), OptionConnTimeout(1))
				_, err := NewGatewayConn(conf, "", &Authentication{}, false, nil)
				require.Error(t, err)
				require.Equal(t, "dial tcp: i/o timeout", err.Error())
			},
		},
		{
			name: "Can throw error if connection timeout, in_order open strategy",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig(OptionDialStrategy(DialInOrder), OptionConnTimeout(1), OptionHostName("localhost:123"))
				_, err := NewGatewayConn(conf, "", &Authentication{}, false, nil)
				require.Error(t, err)
				require.Equal(t, "dial tcp: i/o timeout", err.Error())
			},
		},
		{
			name: "Can throw error if connection timeout, time_random open strategy",
			test: func(t *testing.T) {
				conf, _ := NewConnConfig(OptionDialStrategy(DialTimeRandom), OptionConnTimeout(1), OptionHostName("localhost:123"))
				_, err := NewGatewayConn(conf, "", &Authentication{}, false, nil)
				require.Error(t, err)
				require.Equal(t, "dial tcp: i/o timeout", err.Error())
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
					authentication: NewAuthentication("123", "123", "123"),
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
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}

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
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}

				require.NoError(t, g.SendCancel())
			},
		},
		{
			name: "Test Send token",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)
				conf, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}

				require.NoError(t, g.sendUsernameOrToken())
			},
		},
		{
			name: "Test Send User",
			test: func(t *testing.T) {
				var buffer bytes.Buffer
				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)
				conf, _ := NewConnConfig()
				g := &GatewayConn{
					encoder:     encoder,
					decoder:     decoder,
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("", "123", "123"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}

				require.NoError(t, g.sendUsernameOrToken())
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
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					settings:       make(map[string]interface{}),
					authentication: NewAuthentication("123", "123", "123"),
					logf:           func(s string, i ...interface{}) {},
					userInfo:       NewUserInfo(),
				}

				require.NoError(t, g.AddSetting("min_compress_block_size", "100"))
				require.NoError(t, g.AddSetting("allow_experimental_cross_to_join_conversion", "true"))
				require.NoError(t, g.AddSetting("send_logs_level", "fatal"))
				require.NoError(t, g.AddSetting("totals_auto_threshold", "10.0"))
				require.NoError(t, g.AddSetting("optimize_subpart_number", "-1"))
				require.NoError(t, g.AddSetting("totals_auto_threshold", "1.02349"))

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
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
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
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("", "123", "123"),
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
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
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
					connOptions: config,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
				}

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
					connOptions: newConf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
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
					connOptions: conf,
					conn: &connect{
						Conn: &fakeConn{},
					},
					authentication: NewAuthentication("123", "123", "123"),
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
					connOptions: newConf,
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
					authentication: NewAuthentication("123", "123", "123"),
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
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test add setting fail if setting not found",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				err := g.AddSetting("jack", "john")
				require.Error(t, err)
			},
		},
		{
			name: "Test add setting success if valid setting",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				err := g.AddSetting("min_compress_block_size", "123")
				require.NoError(t, err)
			},
		},
		{
			name: "Test add setting checked success even if invalid setting",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				g.AddSettingsChecked("jack", "john")
				require.Equal(t, "john", g.GetAllSettings()["jack"])
			},
		},
		{
			name: "Test is ansi sql mode if added setting as true",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				err := g.AddSetting("ansi_sql", "true")
				require.NoError(t, err)

				require.True(t, g.InAnsiSQLMode())
			},
		},
		{
			name: "Test is not ansi sql mode if added setting as false",
			test: func(t *testing.T) {
				g := &GatewayConn{
					settings: make(map[string]interface{}),
				}

				err := g.AddSetting("ansi_sql", "false")
				require.NoError(t, err)

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
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
