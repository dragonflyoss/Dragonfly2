package clientutil

import (
	"bufio"
	"io"
)

type BufferedReader struct {
	B *bufio.Reader
	R io.Reader
}

func (b BufferedReader) Read(p []byte) (n int, err error) {
	return b.B.Read(p)
}

func BufferReader(buf *bufio.Reader, real io.Reader) *BufferedReader {
	return &BufferedReader{B: buf, R: real}
}
