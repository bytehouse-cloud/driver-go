package response

import (
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

func readBlock(decoder *ch_encoding.Decoder, compress bool) (string, *data.Block, error) {
	var (
		err       error
		tempTable string
		block     *data.Block
	)
	tempTable, err = decoder.String() // temporary table
	if err != nil {
		return emptyString, nil, err
	}
	decoder.SetCompress(compress)
	block, err = data.ReadBlockFromDecoder(decoder)
	if err != nil {
		return emptyString, nil, err
	}
	decoder.SetCompress(false)
	return tempTable, block, nil
}

func writeBlock(table string, block *data.Block, encoder *ch_encoding.Encoder, compress bool) (err error) {
	err = encoder.String(table)
	if err != nil {
		return err
	}

	encoder.SelectCompress(compress)
	err = data.WriteBlockToEncoder(encoder, block)
	if err != nil {
		return err
	}
	encoder.SelectCompress(false)
	return nil
}

func readStringSlice(decoder *ch_encoding.Decoder) ([]string, error) {
	var (
		result []string
		err    error
		count  uint64
	)

	count, err = decoder.Uvarint()
	if err != nil {
		return nil, err
	}
	result = make([]string, count)
	for i := range result {
		result[i], err = decoder.String()
		if err != nil {
			return nil, err
		}
	}
	return result, err
}

func writeStringSlice(ss []string, encoder *ch_encoding.Encoder) (err error) {
	err = encoder.Uvarint(uint64(len(ss)))
	if err != nil {
		return err
	}
	for _, s := range ss {
		if err = encoder.String(s); err != nil {
			return err
		}
	}
	return nil
}

func formatUint64(i uint64) string {
	return strconv.FormatUint(i, 10)
}

func formatBool(b bool) string {
	return strconv.FormatBool(b)
}

const emptyString = ""
