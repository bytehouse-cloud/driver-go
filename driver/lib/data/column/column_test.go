package column

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateNestedNameColumnData(t *testing.T) {
	tests := []struct {
		name        string
		colType     string
		desiredType interface{}
	}{
		{
			name:        "if nested named column array then success",
			colType:     "Array(a Int32)",
			desiredType: &ArrayColumnData{},
		},
		{
			name:        "if nested named column map then success",
			colType:     "Map(a Int32, b Int32)",
			desiredType: &MapColumnData{},
		},
		{
			name:        "if nested named column tuple then success",
			colType:     "Tuple(a Int32, b Int32)",
			desiredType: &TupleColumnData{},
		},
		{
			name:        "if nested named column nullable then success",
			colType:     "Nullable(a String)",
			desiredType: &NullableColumnData{},
		},
		{
			name:        "if nested named column LowCardinality then success",
			colType:     "LowCardinality(a String)",
			desiredType: &LowCardinalityColumnData{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colGen, err := GenerateColumnDataFactory(CHColumnType(tt.colType))
			require.NoError(t, err)
			col := colGen(1)
			require.IsType(t, tt.desiredType, col)
		})
	}
}

func TestCHColumn(t *testing.T) {
	const nRows = 2
	int32ColGen, err := GenerateColumnDataFactory(UINT32)
	require.Nil(t, err)
	col := CHColumn{
		Name:           "dog",
		Type:           UINT32,
		Data:           MustMakeColumnData(UINT32, nRows),
		GenerateColumn: int32ColGen,
	}

	require.Equal(t, "UInt32", col.CHType())
	require.Equal(t, reflect.Uint32, col.ScanType().Kind())

	n, err := col.Data.ReadFromValues([]interface{}{uint32(1), uint32(2)})
	require.NoError(t, err)
	require.Equal(t, nRows, n)

	var result []string
	result = col.GetAllRowsFmt(result)
	require.Equal(t, len(result), nRows)

	var resultInterface []interface{}
	resultInterface = col.GetAllRows(resultInterface)
	require.Equal(t, len(result), nRows)
}
