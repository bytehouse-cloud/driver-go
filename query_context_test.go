package bytehouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewQueryContext(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Can add query setting",
			test: func(t *testing.T) {
				c := context.Background()
				qc := NewQueryContext(c)
				settings := qc.GetQuerySettings()
				require.Equal(t, settings["log_queries"], nil)
				require.NoError(t, qc.AddQuerySetting("log_queries", "true"))
				settings = qc.GetQuerySettings()
				require.Equal(t, settings["log_queries"], true)
			},
		},
		{
			name: "Can set checked conn",
			test: func(t *testing.T) {
				c := context.Background()
				qc := NewQueryContext(c)
				require.False(t, qc.GetCheckedConn())
				qc.SetCheckedConn(true)
				require.True(t, qc.GetCheckedConn())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
