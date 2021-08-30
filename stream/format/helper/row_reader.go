package helper

import "github.com/bytehouse-cloud/driver-go/driver/lib/data/column"

type ColumnTextsReader interface {
	ReadFirstRow(colTexts [][]string, cols []*column.CHColumn) error
	ReadRowCont(colTexts [][]string, rowIdx int, cols []*column.CHColumn) error
}

func ReadFirstColumnTexts(colTexts [][]string, cols []*column.CHColumn, rReader ColumnTextsReader) (int, error) {
	var totalRowWrite int
	if err := rReader.ReadFirstRow(colTexts, cols); err != nil {
		return 0, err
	}
	totalRowWrite++

	for i := 1; i < len(colTexts[0]); i++ {
		if err := rReader.ReadRowCont(colTexts, i, cols); err != nil {
			return totalRowWrite, err
		}
		totalRowWrite++
	}

	return totalRowWrite, nil
}

func ReadColumnTextsCont(colTexts [][]string, cols []*column.CHColumn, rReader ColumnTextsReader) (int, error) {
	var totalRead int
	for i := 0; i < len(colTexts[0]); i++ {
		if err := rReader.ReadRowCont(colTexts, i, cols); err != nil {
			return totalRead, err
		}
		totalRead++
	}
	return totalRead, nil
}
