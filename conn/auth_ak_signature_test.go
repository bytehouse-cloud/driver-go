package conn_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAKSKSigningProtocol(t *testing.T) {
	const (
		userAccessKey = "AK1899200289"
		userSecretKey = "SK90189ASHUSHU17823"
	)

	date := time.Now().Format(conn.SignatureDateFormat)

	// Prepare claims
	cred := &conn.AKCredential{
		AccessKey: userAccessKey,
		Date:      date,
		Region:    "cn-north-1",
		Service:   "ByteHouse",
	}

	// Signing Key
	signKey := conn.NewSigningKey(userSecretKey, cred)

	// Signature
	sig, err := conn.Sign(signKey, cred)
	assert.NoErrorf(t, err, "failed at signing claim")

	// Validate using correct signKey
	token, err := validateSignature(sig, signKey)
	assert.NoErrorf(t, err, "failed at validating signature")
	assert.True(t, token.Valid)

	// Validate using wrong signKey
	wrongSecretKey := "SK20300WRONGSCRT0ERC20"
	wrongSignKey := conn.NewSigningKey(wrongSecretKey, cred)

	token, err = validateSignature(sig, wrongSignKey)
	assert.Errorf(t, err, "expect error for using wrong signkey but no error")

	// Prepare spoofed credential
	spoofedCred := &conn.AKCredential{
		AccessKey: "AK1222004567",
		Date:      date,
		Region:    "asia-southeast-1",
		Service:   "Bytehouse",
	}

	// Validate using spoofed credential
	spoofedSignKey := conn.NewSigningKey(userSecretKey, spoofedCred)

	token, err = validateSignature(sig, spoofedSignKey)
	assert.Errorf(t, err, "expect error for using other signkey but no error")
}

func TestAKSKAuthenticationSmoked(t *testing.T) {
	const (
		userAccessKey = "AK1899200289"
		userSecretKey = "SK90189ASHUSHU17823"
	)

	date := time.Now().Format(conn.SignatureDateFormat)
	sa := conn.NewSignatureAuthentication(userAccessKey, userSecretKey, "cn-north-1")

	var buf bytes.Buffer
	enc := ch_encoding.NewEncoder(&buf)
	perr := sa.WriteAuthProtocol(enc)

	require.NoError(t, perr)

	werr := sa.WriteAuthData(enc)
	credScope := "\x08*AK1899200289/" + date + "/cn-north-1/bytehouse"

	require.NoError(t, werr)
	require.True(t, strings.HasPrefix(buf.String(), credScope))
}

// ValidateSignature ...
func validateSignature(sig string, signKey conn.SigningKey) (*jwt.Token, error) {
	token, err := jwt.Parse(sig, signKey.JWTSecretFunc)
	if err != nil {
		return nil, fmt.Errorf("validate signature failed: %w", err)
	}
	return token, nil
}
