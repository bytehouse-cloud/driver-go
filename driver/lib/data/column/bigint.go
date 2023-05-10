package column

import (
	"math/big"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type BigIntColumnData struct {
	byteCount int
	raw       []byte
	isClosed  bool
	isSigned  bool
}

func (d *BigIntColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	if _, err := decoder.Read(d.raw); err != nil {
		return errors.ErrorfWithCaller("bigInt(byteCount=%d,isSigned=%v) readFromDecoder got error=[%v]", d.byteCount, d.isSigned, err)
	}

	return nil
}

func (d *BigIntColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if _, err := encoder.Write(d.raw); err != nil {
		return errors.ErrorfWithCaller("bigInt(byteCount=%d,isSigned=%v) writeToEncoder got error=[%v]", d.byteCount, d.isSigned, err)
	}

	return nil
}

func (d *BigIntColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	for i, value := range values {
		if value == nil {
			_ = d.readBigInt(i, 0)
			continue
		}

		if err := d.readBigInt(i, value); err != nil {
			return i, err
		}
	}

	return len(values), nil
}

func (d *BigIntColumnData) ReadFromTexts(texts []string) (int, error) {
	for i, text := range texts {
		if isEmptyOrNull(text) {
			_ = d.readBigInt(i, 0)
			continue
		}

		bi, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return i, errors.ErrorfWithCaller("unable to parseBigInt(byteCount=%d,isSigned=%v) from %s", d.byteCount, d.isSigned, text)
		}

		// Put it into little-endian bytes
		if err := d.readBigInt(i, bi); err != nil {
			return i, err
		}
	}

	return len(texts), nil
}

func (d *BigIntColumnData) GetValue(row int) interface{} {
	return d.getBigInt(row)
}

func (d *BigIntColumnData) getBigInt(row int) *big.Int {
	bytes := d.raw[row*d.byteCount : (row+1)*d.byteCount]
	if d.isSigned {
		return rawToBigInt(bytes, true)
	}

	return rawToBigInt(bytes, false)
}

func (d *BigIntColumnData) GetString(row int) string {
	return d.getBigInt(row).String()
}

func (d *BigIntColumnData) Zero() interface{} {
	return 0
}

func (d *BigIntColumnData) ZeroString() string {
	return zeroString
}

func (d *BigIntColumnData) Len() int {
	return len(d.raw) / d.byteCount
}

func (d *BigIntColumnData) Close() error {
	if d.isClosed {
		return nil
	}
	d.isClosed = true
	bytepool.PutBytes(d.raw)
	return nil
}

func (d *BigIntColumnData) readBigInt(i int, val interface{}) error {
	var (
		bi  *big.Int
		err error
	)

	if d.isSigned {
		bi, err = d.convertToBigIntWithSigned(val)
	} else {
		bi, err = d.convertToBigIntUnsigned(val)
	}

	if err != nil {
		return err
	}

	if bi.BitLen() > d.byteCount*8 {
		return errors.ErrorfWithCaller("BigInt %s received got bit length %d exceed largest allowed of column type %d", bi.String(), bi.BitLen(), d.byteCount*8)
	}

	putBigIntLittleEndianImpl(d.raw[i*d.byteCount:(i+1)*d.byteCount], bi)
	return nil
}

func (d *BigIntColumnData) convertToBigIntWithSigned(val interface{}) (*big.Int, error) {
	res := new(big.Int)
	switch v := val.(type) {
	case int: // at least 32 bits in size, It is a distinct type, however, and not an alias for, say, int32.
		return res.SetInt64(int64(v)), nil
	case int8:
		return res.SetInt64(int64(v)), nil
	case int16:
		return res.SetInt64(int64(v)), nil
	case int32:
		return res.SetInt64(int64(v)), nil
	case int64:
		return res.SetInt64(v), nil
	case uint: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint8: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint16: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint32: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint64: // cast up
		return res.SetUint64(v), nil
	case big.Int:
		return &v, nil
	case *big.Int:
		return v, nil
	default:
		// Do nothing
	}

	return nil, errors.ErrorfWithCaller("input value=[%#v] for cannot be converted to BigInt", val)
}

func (d *BigIntColumnData) convertToBigIntUnsigned(val interface{}) (*big.Int, error) {
	res := new(big.Int)
	switch v := val.(type) {
	case int: // at least 32 bits in size, It is a distinct type, however, and not an alias for, say, int32.
		if v < 0 {
			return nil, errors.ErrorfWithCaller("expected unsigned integer got %d", v)
		}
		return res.SetInt64(int64(v)), nil
	case int8:
		if v < 0 {
			return nil, errors.ErrorfWithCaller("expected unsigned integer got %d", v)
		}
		return res.SetInt64(int64(v)), nil
	case int16:
		if v < 0 {
			return nil, errors.ErrorfWithCaller("expected unsigned integer got %d", v)
		}
		return res.SetInt64(int64(v)), nil
	case int32:
		if v < 0 {
			return nil, errors.ErrorfWithCaller("expected unsigned integer got %d", v)
		}
		return res.SetInt64(int64(v)), nil
	case int64:
		if v < 0 {
			return nil, errors.ErrorfWithCaller("expected unsigned integer got %d", v)
		}
		return res.SetInt64(v), nil
	case uint: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint8: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint16: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint32: // cast up
		return res.SetUint64(uint64(v)), nil
	case uint64: // cast up
		return res.SetUint64(v), nil
	case big.Int:
		if v.Sign() == -1 {
			return nil, errors.ErrorfWithCaller("expected unsigned integer got %s", v.String())
		}
		return &v, nil
	case *big.Int:
		if v.Sign() == -1 {
			return nil, errors.ErrorfWithCaller("expected unsigned integer got %s", v.String())
		}
		return v, nil
	default:
		// Do nothing
	}

	return nil, errors.ErrorfWithCaller("input value=[%#v] for cannot be converted to BigInt", val)
}

func putBigIntLittleEndianImpl(dest []byte, v *big.Int) {
	var sign int
	if v.Sign() < 0 {
		v.Not(v).FillBytes(dest)
		sign = -1
	} else {
		v.FillBytes(dest)
	}
	endianSwap(dest, sign < 0)
}

func rawToBigInt(v []byte, signed bool) *big.Int {
	// LittleEndian to BigEndian
	endianSwap(v, false)
	var lt = new(big.Int)
	if signed && len(v) > 0 && v[0]&0x80 != 0 {
		// [0] ^ will +1
		for i := 0; i < len(v); i++ {
			v[i] = ^v[i]
		}
		lt.SetBytes(v)
		// neg ^ will -1
		lt.Not(lt)
	} else {
		lt.SetBytes(v)
	}
	return lt
}

func endianSwap(src []byte, not bool) {
	for i := 0; i < len(src)/2; i++ {
		if not {
			src[i], src[len(src)-i-1] = ^src[len(src)-i-1], ^src[i]
		} else {
			src[i], src[len(src)-i-1] = src[len(src)-i-1], src[i]
		}
	}
}
