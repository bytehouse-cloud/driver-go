package data

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func Test_blocksPrinter_Print(t *testing.T) {
	type args struct {
		blocks  []*Block
		builder *strings.Builder
	}
	tests := []struct {
		name     string
		printer  *blocksPrinter
		args     args
		expected string
	}{
		{
			name:    "Can print block with values",
			printer: NewBlocksPrinter(100),
			args: args{
				blocks: func() []*Block {
					b := &Block{
						info: &blockInfo{
							num1:        1,
							isOverflows: false,
							num2:        2,
							bucketNum:   -1,
							num3:        0,
						},
						NumColumns: 5,
						NumRows:    1,
						Columns: []*column.CHColumn{
							{
								Name: "a",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "b",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "c",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "d",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "e",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
						},
					}

					for _, col := range b.Columns {
						_, _ = col.Data.ReadFromValues([]interface{}{int32(1)})
					}

					return []*Block{b}
				}(),

				builder: &strings.Builder{},
			},
			expected: "┌─\u001B[1ma\u001B[0m─┬─\u001B[1mb\u001B[0m─┬─\u001B[1mc\u001B[0m─┬─\u001B[1md\u001B[0m─┬─\u001B[1me\u001B[0m─┐\n│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│\n└───┴───┴───┴───┴───┘\n",
		},
		{
			name:    "Can print block with no values",
			printer: NewBlocksPrinter(100),
			args: args{
				blocks: func() []*Block {
					b := &Block{
						NumColumns: 1,
						NumRows:    0,
						Columns: []*column.CHColumn{
							{
								Name: "a",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 0),
							},
						},
					}

					return []*Block{b}
				}(),

				builder: &strings.Builder{},
			},
			expected: "┌─\u001B[1ma\u001B[0m─┐\n└───┘\n",
		},
		{
			name:    "Can print multiple block with values",
			printer: NewBlocksPrinter(100),
			args: args{
				blocks: func() []*Block {
					b := &Block{
						info: &blockInfo{
							num1:        1,
							isOverflows: false,
							num2:        2,
							bucketNum:   -1,
							num3:        0,
						},
						NumColumns: 5,
						NumRows:    1,
						Columns: []*column.CHColumn{
							{
								Name: "a",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "b",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "c",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "d",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "e",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
						},
					}

					for _, col := range b.Columns {
						_, _ = col.Data.ReadFromValues([]interface{}{int32(1)})
					}

					return []*Block{b, b}
				}(),

				builder: &strings.Builder{},
			},
			expected: "┌─\u001B[1ma\u001B[0m─┬─\u001B[1mb\u001B[0m─┬─\u001B[1mc\u001B[0m─┬─\u001B[1md\u001B[0m─┬─\u001B[1me\u001B[0m─┐\n│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│\n└───┴───┴───┴───┴───┘\n┌─\u001B[1ma\u001B[0m─┬─\u001B[1mb\u001B[0m─┬─\u001B[1mc\u001B[0m─┬─\u001B[1md\u001B[0m─┬─\u001B[1me\u001B[0m─┐\n│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│\n└───┴───┴───┴───┴───┘\n",
		},
		{
			name:    "Can print extra rows left if balance runs out",
			printer: NewBlocksPrinter(2),
			args: args{
				blocks: func() []*Block {
					b := &Block{
						info: &blockInfo{
							num1:        1,
							isOverflows: false,
							num2:        2,
							bucketNum:   -1,
							num3:        0,
						},
						NumColumns: 3,
						NumRows:    3,
						Columns: []*column.CHColumn{
							{
								Name: "a",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 3),
							},
							{
								Name: "b",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 3),
							},
							{
								Name: "c",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 3),
							},
						},
					}

					for _, col := range b.Columns {
						_, _ = col.Data.ReadFromValues([]interface{}{int32(1), int32(1), int32(1)})
					}

					return []*Block{b}
				}(),

				builder: &strings.Builder{},
			},
			expected: "┌─\u001B[1ma\u001B[0m─┬─\u001B[1mb\u001B[0m─┬─\u001B[1mc\u001B[0m─┐\n│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│\n│\u001B[100m 1 \u001B[0m│\u001B[100m 1 \u001B[0m│\u001B[100m 1 \u001B[0m│\n... 1 more rows\n└───┴───┴───┘\n",
		},
		{
			name:    "Can hide extra blocks if balance runs out",
			printer: NewBlocksPrinter(2),
			args: args{
				blocks: func() []*Block {
					b := &Block{
						info: &blockInfo{
							num1:        1,
							isOverflows: false,
							num2:        2,
							bucketNum:   -1,
							num3:        0,
						},
						NumColumns: 5,
						NumRows:    1,
						Columns: []*column.CHColumn{
							{
								Name: "a",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "b",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "c",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "d",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
							{
								Name: "e",
								Type: "Int32",
								Data: column.MustMakeColumnData(column.INT32, 1),
							},
						},
					}

					for _, col := range b.Columns {
						_, _ = col.Data.ReadFromValues([]interface{}{int32(1)})
					}

					return []*Block{b, b, b, b}
				}(),

				builder: &strings.Builder{},
			},
			expected: "┌─\u001B[1ma\u001B[0m─┬─\u001B[1mb\u001B[0m─┬─\u001B[1mc\u001B[0m─┬─\u001B[1md\u001B[0m─┬─\u001B[1me\u001B[0m─┐\n│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│\n└───┴───┴───┴───┴───┘\n┌─\u001B[1ma\u001B[0m─┬─\u001B[1mb\u001B[0m─┬─\u001B[1mc\u001B[0m─┬─\u001B[1md\u001B[0m─┬─\u001B[1me\u001B[0m─┐\n│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│ 1 \u001B[0m│\n└───┴───┴───┴───┴───┘\nExtra blocks not printed: No balance row left\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, block := range tt.args.blocks {
				tt.printer.Print(block, tt.args.builder)
			}
			require.Equal(t, tt.expected, tt.args.builder.String())
		})
	}
}
