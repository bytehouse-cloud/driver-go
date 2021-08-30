package column

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrInvalidColumnType_Is(t *testing.T) {
	a := NewErrInvalidColumnType("hi", "lol")
	b := NewErrInvalidColumnType("ok", "can")
	require.True(t, errors.Is(a, b))
}
