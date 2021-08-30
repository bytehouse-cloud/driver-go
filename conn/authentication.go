package conn

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type Authentication struct {
	token    string
	username string
	password string
}

func NewAuthentication(token, username, password string) *Authentication {
	return &Authentication{
		token:    token,
		username: username,
		password: password,
	}
}

func (a *Authentication) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if a.token != "" {
		return encoder.String(a.token)
	}
	err := encoder.String(a.username)
	if err != nil {
		return err
	}
	return encoder.String(a.password)
}
