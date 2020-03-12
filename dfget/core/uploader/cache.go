package uploader

import (
	"bytes"
	"io"
	"os"
	"sync"
)

type cacheBuffer struct {
	// cache size
	size int64
	// is cache valid
	valid  bool

	buf *bytes.Buffer
	f *os.File

	sync.Mutex
}

func newCacheBuffer() *cacheBuffer {
	return &cacheBuffer{
		valid: false,
		buf: nil,
	}
}

// readBytes return the bytes in buffer
func (c *cacheBuffer) readBytes() (data []byte, valid bool) {
	c.Lock()
	defer c.Unlock()

	if !c.valid {
		return nil, false
	}

	_, err := c.f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, false
	}

	data = make([]byte, c.size)
	n, err := c.f.Read(data)
	if err != nil || n != int(c.size) {
		return nil, false
	}

	return data, true
}

func (c *cacheBuffer) close() {
	c.Lock()
	defer c.Unlock()

	if c.f != nil {
		c.f.Close()
		c.valid = false
	}
}
