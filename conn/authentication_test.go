package conn

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestAuthentication_WriteToEncoder(t *testing.T) {
	var buffer bytes.Buffer

	tests := []struct {
		name    string
		encoder *ch_encoding.Encoder
		auth    *Authentication
		wantErr bool
	}{
		{
			name:    "Can write to encoder with token",
			encoder: ch_encoding.NewEncoder(&buffer),
			auth:    NewAuthentication("123", "123", "123"),
			wantErr: false,
		},
		{
			name:    "Can write to encoder without token",
			encoder: ch_encoding.NewEncoder(&buffer),
			auth:    NewAuthentication("", "123", "123"),
			wantErr: false,
		},
		{
			name: "Can throw error if encoder has a problem",
			encoder: func() *ch_encoding.Encoder {
				file, _ := os.Create("")
				return ch_encoding.NewEncoder(file)
			}(),
			auth: &Authentication{
				token: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.auth.WriteToEncoder(tt.encoder)
			if tt.wantErr {
				fmt.Println(err)
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}
