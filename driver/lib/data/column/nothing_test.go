package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestNothing(t *testing.T) {
	// Test general methods
	original := MustMakeColumnData(NOTHING, 0)

	n, err := original.ReadFromValues([]interface{}{"hello"})
	require.Equal(t, 0, n)
	require.NoError(t, err)

	n, err = original.ReadFromTexts([]string{"hello"})
	require.Equal(t, 0, n)
	require.NoError(t, err)

	require.Nil(t, original.GetValue(0))
	require.Equal(t, "", original.GetString(0))
	require.Nil(t, original.Zero())
	require.Equal(t, "", original.ZeroString())
	require.Equal(t, 0, original.Len())
	require.NoError(t, original.Close())

	// Test encoder & decoder
	newCopy := MustMakeColumnData(NOTHING, 0)
	var buffer bytes.Buffer
	encoder := ch_encoding.NewEncoder(&buffer)
	err = original.WriteToEncoder(encoder)
	require.NoError(t, err)

	decoder := ch_encoding.NewDecoder(&buffer)
	err = newCopy.ReadFromDecoder(decoder)
	require.NoError(t, err)

	require.Equal(t, original.GetValue(0), newCopy.GetValue(0))
	require.Equal(t, original.GetString(0), newCopy.GetString(0))
	require.Equal(t, original.Zero(), newCopy.Zero())
	require.Equal(t, original.ZeroString(), newCopy.ZeroString())
	require.Equal(t, original.Len(), newCopy.Len())
	require.Equal(t, original.Close(), newCopy.Close())
}
