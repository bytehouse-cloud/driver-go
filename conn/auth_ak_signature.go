package conn

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/protocol"
)

const (
	SignatureAuthService = "bytehouse" // Bytehouse is one of Volcano service
	SignatureDateFormat  = "20060102"
)

// AKCredential contain credential scope. It also
// implement jwt.Claims and used as a claim.
type AKCredential struct {
	AccessKey string `json:"sub"`
	Service   string `json:"aud"`  // default: ByteHouse
	Date      string `json:"date"` // format: yyyymmdd
	Region    string `json:"region"`
}

// Scope is credential scope in unix-path like separator.
func (v AKCredential) Scope() string {
	elems := []string{
		v.AccessKey,
		v.Date,
		v.Region,
		v.Service,
	}
	return strings.Join(elems, "/")
}

// Valid implement jwt.Claims
func (v AKCredential) Valid() error {
	if v.AccessKey == "" {
		return fmt.Errorf("driver: empty access key in AK/SK credential")
	}
	if v.Service == "" {
		return fmt.Errorf("driver: empty audience service in AK/SK credential")
	}
	_, err := time.Parse(SignatureDateFormat, v.Date)
	if err != nil {
		return fmt.Errorf("driver: invalid date format in AK/SK credential")
	}
	if v.Region == "" {
		return fmt.Errorf("driver: empty region in AK/SK credential")
	}
	return nil
}

// SigningKey represent signing key created from secret key + credential.
type SigningKey []byte

func (s SigningKey) JWTSecretFunc(_ *jwt.Token) (interface{}, error) {
	return []byte(s), nil
}

// NewSigningKey creates new signing key.
func NewSigningKey(secret string, cred *AKCredential) SigningKey {
	kDate := hmacSHA256([]byte(secret), cred.Date)
	kRegion := hmacSHA256(kDate, cred.Region)
	kService := hmacSHA256(kRegion, cred.Service)
	kSigning := hmacSHA256(kService, "request") // Volcano style
	return kSigning
}

// Sign signs credential (claim) using signing key.
// It returns the signature string and error if any.
func Sign(signingKey SigningKey, cred *AKCredential) (string, error) {
	if err := cred.Valid(); err != nil {
		return "", err
	}
	jwtc := jwt.NewWithClaims(jwt.SigningMethodHS256, cred)
	return jwtc.SignedString([]byte(signingKey))
}

// SignatureAuthentication handler based on ByteHouse AM
// custom AK/SK Signature Auth.
type SignatureAuthentication struct {
	cred      *AKCredential
	signature string
}

// NewSignatureAuthentication creates new Authentication handler based
// on ByteHouse AM custom AK/SK Signature Auth.
func NewSignatureAuthentication(accessKey, secretKey, region string) *SignatureAuthentication {
	cred := &AKCredential{
		AccessKey: accessKey,
		Region:    region,
		Date:      time.Now().Format(SignatureDateFormat),
		Service:   SignatureAuthService,
	}
	signingKey := NewSigningKey(secretKey, cred)

	signature, err := Sign(signingKey, cred)
	if err != nil {
		log.Print("WARN: invalid credential, failed to generate signature from AK/SK")
	}

	return &SignatureAuthentication{
		cred:      cred,
		signature: signature,
	}
}

func (s *SignatureAuthentication) WriteAuthProtocol(encoder *ch_encoding.Encoder) error {
	return encoder.Uvarint(protocol.ClientHelloSignature)
}

func (s *SignatureAuthentication) WriteAuthData(encoder *ch_encoding.Encoder) error {
	credstr := s.cred.Scope()
	if err := encoder.String(credstr); err != nil {
		return err
	}
	return encoder.String(s.signature)
}

func (s *SignatureAuthentication) Identity() string {
	return s.cred.Scope()
}

// hmacSHA256 ...
func hmacSHA256(key []byte, content string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(content))

	return mac.Sum(nil)
}
