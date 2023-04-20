package conn

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestAuthentication(t *testing.T) {
	tests := []struct {
		name            string
		auth            Authentication
		wantWrittenData []byte
	}{
		{
			name:            "If Password Authentication then Write OK",
			auth:            NewPasswordAuthentication("u1", "ps1"),
			wantWrittenData: []byte{0, 2, 117, 49, 3, 112, 115, 49},
		},
		{
			name:            "If System Authentication then Write OK",
			auth:            NewSystemAuthentication("some_system_token"),
			wantWrittenData: []byte{232, 7, 17, 115, 111, 109, 101, 95, 115, 121, 115, 116, 101, 109, 95, 116, 111, 107, 101, 110},
		},
		{
			name:            "If API Token Authentication then Write OK",
			auth:            NewAPITokenAuthentication("some_api_token"),
			wantWrittenData: []byte{0, 9, 98, 121, 116, 101, 104, 111, 117, 115, 101, 14, 115, 111, 109, 101, 95, 97, 112, 105, 95, 116, 111, 107, 101, 110},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			enc := ch_encoding.NewEncoder(&buf)
			err := tt.auth.WriteAuthProtocol(enc)
			require.NoError(t, err)
			err = tt.auth.WriteAuthData(enc)
			require.NoError(t, err)
			require.Equal(t, tt.wantWrittenData, buf.Bytes())
		})
	}
}
