package helper

import "github.com/bytehouse-cloud/driver-go/driver/lib/data/column"

type RowWriter interface {
	WriteFirstRow(record []string, cols []*column.CHColumn) error
	WriteRowCont(record []string, cols []*column.CHColumn) error
}

func WriteFirstFrame(frame [][]string, cols []*column.CHColumn, rWriter RowWriter) (int, error) {
	var totalRowWrite int
	if err := rWriter.WriteFirstRow(frame[0], cols); err != nil {
		return 0, err
	}
	totalRowWrite++

	for i := 1; i < len(frame); i++ {
		if err := rWriter.WriteRowCont(frame[i], cols); err != nil {
			return totalRowWrite, err
		}
		totalRowWrite++
	}

	return totalRowWrite, nil
}

func WriteFrameCont(frame [][]string, cols []*column.CHColumn, rWriter RowWriter) (int, error) {
	var totalWrite int
	for _, record := range frame {
		if err := rWriter.WriteRowCont(record, cols); err != nil {
			return totalWrite, err
		}
		totalWrite++
	}
	return totalWrite, nil
}
