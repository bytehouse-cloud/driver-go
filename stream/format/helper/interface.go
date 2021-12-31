package helper

import "io"

type Writer interface {
	io.Writer
	io.StringWriter
	io.ByteWriter
}
