package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResult(t *testing.T) {
	res := &result{}

	_, err := res.LastInsertId()
	require.Error(t, err)
	require.Equal(t, "driver-go: LastInsertId is not supported", err.Error())

	_, err = res.RowsAffected()
	require.Error(t, err)
	require.Equal(t, "driver-go: RowsAffected is not supported", err.Error())
}
