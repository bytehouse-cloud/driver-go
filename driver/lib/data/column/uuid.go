package column

import (
	"github.com/google/uuid"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	uuidLen        = 16
	zeroUUIDString = "00000000-0000-0000-0000-000000000000"
)

var zeroUUID = uuid.MustParse(zeroUUIDString)

type UUIDColumnData struct {
	raw      []byte
	isClosed bool
}

func (u *UUIDColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(u.raw)
	return err
}

func (u *UUIDColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(u.raw)
	return err
}

func (u *UUIDColumnData) ReadFromValues(values []interface{}) (int, error) {
	for i, value := range values {
		uid, ok := value.(uuid.UUID)
		if !ok {
			return i, NewErrInvalidColumnType(value, uid)
		}
		// convert uuidv1 to uuidv4
		swapV1V4(uid[:])
		copy(u.raw[i*uuidLen:], uid[:])
	}

	return len(values), nil
}

func (u *UUIDColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		err error
		uid uuid.UUID
	)

	for i, text := range texts {
		if text == "" {
			copy(u.raw[i*uuidLen:], zeroUUID[:])
			continue
		}

		// uuid library encodes it in uuidv1
		uid, err = uuid.Parse(text)
		if err != nil {
			return i, err
		}
		// convert uuidv1 to uuidv4
		swapV1V4(uid[:])
		copy(u.raw[i*uuidLen:], uid[:])
	}

	return len(texts), nil
}

// swapV1V4 converts uuidV1 to uuidV4 and vice versa
// Symmetrical function
func swapV1V4(s []byte) {
	// Check for early panic
	_ = s[15]
	s[0], s[7] = s[7], s[0]
	s[1], s[6] = s[6], s[1]
	s[2], s[5] = s[5], s[2]
	s[3], s[4] = s[4], s[3]
	s[8], s[15] = s[15], s[8]
	s[9], s[14] = s[14], s[9]
	s[10], s[13] = s[13], s[10]
	s[11], s[12] = s[12], s[11]
}

func (u *UUIDColumnData) get(row int) uuid.UUID {
	v := make([]byte, uuidLen)
	copy(v, u.raw[row*uuidLen:(row+1)*uuidLen])
	// Convert from uuidv4 to uuidv1
	swapV1V4(v)
	newUuid, _ := uuid.FromBytes(v)
	return newUuid
}

func (u *UUIDColumnData) GetValue(row int) interface{} {
	return u.get(row)
}

func (u *UUIDColumnData) GetString(row int) string {
	return u.get(row).String()
}

func (u *UUIDColumnData) Zero() interface{} {
	return zeroUUID
}

func (u *UUIDColumnData) ZeroString() string {
	return zeroUUIDString
}

func (u *UUIDColumnData) Len() int {
	return len(u.raw) / uuidLen
}

func (u *UUIDColumnData) Close() error {
	if u.isClosed {
		return nil
	}
	u.isClosed = true
	bytepool.PutBytes(u.raw)
	return nil
}
