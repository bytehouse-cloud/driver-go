package column

import (
	"bytes"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestIPv6ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		want            int
		wantDataWritten []net.IP
		wantRead        []string
		wantErr         bool
	}{
		{
			name: "Should return values read if is ipv4",
			args: args{
				texts: []string{
					"192.0.2.1", "192.0.2.10",
				},
			},
			wantRead: []string{
				"192.0.2.1", "192.0.2.10",
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should return values read if is ipv4-mapped v6",
			args: args{
				texts: []string{
					"::ffff:192.0.2.1",
				},
			},
			wantRead: []string{
				"192.0.2.1",
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "Should return values read if is ipv6 and ipv4",
			args: args{
				texts: []string{
					"2001:db8::68", "192.0.2.10",
				},
			},
			wantRead: []string{
				"2001:db8::68", "192.0.2.10",
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should return values read if has empty string",
			args: args{
				texts: []string{
					"", "2001:db8::68", "192.0.2.10", "null",
				},
			},
			wantDataWritten: []net.IP{
				net.IPv6zero,
				net.ParseIP("2001:db8::68"),
				net.ParseIP("192.0.2.10"),
				net.IPv6zero,
			},
			want:    4,
			wantErr: false,
		},
		{
			name: "Should return error if is not ip",
			args: args{
				texts: []string{
					"kookoo",
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "Should return error if is not ip",
			args: args{
				texts: []string{
					"192.0.2.1", "192.0.2.10", "baba",
				},
			},
			want:    2,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(IPV6, 1000)
			got, err := i.ReadFromTexts(tt.args.texts)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.Equal(t, tt.want, got)

			for idx, text := range tt.wantRead {
				assert.Equal(t, text, fmt.Sprint(i.GetValue(idx)))
			}
		})
	}
}

func TestIPv6ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "Should return values read if is ipv4",
			args: args{
				values: []interface{}{
					net.ParseIP("192.0.2.1"), net.ParseIP("192.0.2.10"),
				},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should return values read if is ipv4-mapped v6",
			args: args{
				values: []interface{}{
					net.ParseIP("::ffff:192.0.2.1"), nil,
				},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should return values read if is ipv6 and ipv4",
			args: args{
				values: []interface{}{
					net.ParseIP("2001:db8::68"), net.ParseIP("192.0.2.1"),
				},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should return error if is not ip",
			args: args{
				values: []interface{}{
					"kookoo",
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "Should return error if is not ip",
			args: args{
				values: []interface{}{
					net.ParseIP("192.0.2.1"), net.ParseIP("2001:db8::68"), "baba",
				},
			},
			want:    2,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(IPV6, 1000)
			got, err := i.ReadFromValues(tt.args.values)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.Equal(t, tt.want, got)

			for idx, value := range tt.args.values {
				if value == nil {
					value = zeroIPv6String
				}
				assert.Equal(t, fmt.Sprint(value), fmt.Sprint(i.GetValue(idx)))
			}
		})
	}
}

func TestIPv6ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		want            int
		wantDataWritten []net.IP
		wantRead        []string
		wantErr         bool
	}{
		{
			name: "Should return values read if is ipv4",
			args: args{
				texts: []string{
					"192.0.2.1", "192.0.2.10",
				},
			},
			wantRead: []string{
				"192.0.2.1", "192.0.2.10",
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should return values read if is ipv4-mapped v6",
			args: args{
				texts: []string{
					"::ffff:192.0.2.1",
				},
			},
			wantRead: []string{
				"192.0.2.1",
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "Should return values read if is ipv6 and ipv4",
			args: args{
				texts: []string{
					"2001:db8::68", "192.0.2.10",
				},
			},
			wantRead: []string{
				"2001:db8::68", "192.0.2.10",
			},
			want:    2,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData(IPV6, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.want)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(IPV6, len(tt.args.texts))
			err = newCopy.ReadFromDecoder(decoder)

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.True(t, value.Equal(newCopy.GetValue(index).(net.IP)))
					require.Equal(t, newCopy.GetString(index), value.String())
				}
			}

			require.Equal(t, newCopy.Len(), original.Len())
			require.Equal(t, newCopy.Zero(), original.Zero())
			require.Equal(t, newCopy.ZeroString(), original.ZeroString())
			require.NoError(t, original.Close())
			require.NoError(t, newCopy.Close())
		})
	}
}
