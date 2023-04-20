package conn

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
)

const (
	service = "bytehouse"
	request = "request"

	apiTokenAuthUsername = "bytehouse"
)

type Authentication interface {
	WriteAuthProtocol(*ch_encoding.Encoder) error
	WriteAuthData(*ch_encoding.Encoder) error
	Identity() string
}

type PasswordAuthentication struct {
	user     string
	password string
}

func NewPasswordAuthentication(user, password string) *PasswordAuthentication {
	return &PasswordAuthentication{
		user:     user,
		password: password,
	}
}

func (p *PasswordAuthentication) WriteAuthProtocol(encoder *ch_encoding.Encoder) error {
	return encoder.Uvarint(protocol.ClientHello)
}

func (p *PasswordAuthentication) WriteAuthData(encoder *ch_encoding.Encoder) error {
	err := encoder.String(p.user)
	if err != nil {
		return err
	}
	return encoder.String(p.password)
}

func (p *PasswordAuthentication) Identity() string {
	return p.user
}

type SystemAuthentication struct {
	token string
}

func NewSystemAuthentication(token string) *SystemAuthentication {
	return &SystemAuthentication{
		token: token,
	}
}

func (s *SystemAuthentication) WriteAuthProtocol(encoder *ch_encoding.Encoder) error {
	return encoder.Uvarint(protocol.ClientSystemHello)
}

func (s *SystemAuthentication) WriteAuthData(encoder *ch_encoding.Encoder) error {
	return encoder.String(s.token)
}

func (s *SystemAuthentication) Identity() string {
	return s.token
}

type APITokenAuthentication struct {
	token string
}

func NewAPITokenAuthentication(token string) *APITokenAuthentication {
	return &APITokenAuthentication{
		token: token,
	}
}

func (a *APITokenAuthentication) WriteAuthProtocol(encoder *ch_encoding.Encoder) error {
	return encoder.Uvarint(protocol.ClientHello)
}

func (a *APITokenAuthentication) WriteAuthData(encoder *ch_encoding.Encoder) error {
	err := encoder.String(apiTokenAuthUsername)
	if err != nil {
		return err
	}
	return encoder.String(a.token)
}

func (a *APITokenAuthentication) Identity() string {
	return apiTokenAuthUsername
}
