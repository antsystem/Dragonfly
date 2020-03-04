package uploader

import "bytes"

type cacheBuffer struct {
	// is cache valid
	size int64
	valid  bool
	buf *bytes.Buffer
}

func newCacheBuffer() *cacheBuffer {
	return &cacheBuffer{
		valid: false,
		buf: nil,
	}
}