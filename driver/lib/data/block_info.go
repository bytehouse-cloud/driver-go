package data

import "github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"

type blockInfo struct {
	num1        uint64
	isOverflows bool
	num2        uint64
	bucketNum   int32
	num3        uint64
}

func readBlockInfo(decoder *ch_encoding.Decoder) (*blockInfo, error) {
	var (
		err  error
		info blockInfo
	)
	if info.num1, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if info.isOverflows, err = decoder.Bool(); err != nil {
		return nil, err
	}
	if info.num2, err = decoder.Uvarint(); err != nil {
		return nil, err
	}
	if info.bucketNum, err = decoder.Int32(); err != nil {
		return nil, err
	}
	if info.num3, err = decoder.Uvarint(); err != nil {
		return nil, err
	}

	return &info, nil
}

func writeBlockInfo(encoder *ch_encoding.Encoder, info *blockInfo) error {
	if info == nil {
		info = &blockInfo{}
	}
	if err := encoder.Uvarint(1); err != nil {
		return err
	}
	if err := encoder.Bool(info.isOverflows); err != nil {
		return err
	}
	if err := encoder.Uvarint(2); err != nil {
		return err
	}
	if info.bucketNum == 0 {
		info.bucketNum = -1
	}
	if err := encoder.Int32(info.bucketNum); err != nil {
		return err
	}
	if err := encoder.Uvarint(0); err != nil {
		return err
	}
	return nil
}
