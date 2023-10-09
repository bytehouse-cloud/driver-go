package pointer

import "io"

func IoReader(s io.Reader) *io.Reader {
	return &s
}
