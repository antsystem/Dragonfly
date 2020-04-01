package seed

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
)

func GenerateKeyByUrl(url string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(url)))
}

func NewPreFetchResult(success bool, canceled bool, err error, close func()) PreFetchResult {
	return PreFetchResult{
		Success:  success,
		Canceled: canceled,
		Err:      err,
		close:    close,
	}
}

func (r *PreFetchResult) Close() {
	r.Once.Do(r.close)
}

func CopyBufferToWriterAt(off int64, writerAt io.WriterAt, rd io.Reader) (n int64, err error) {
	buffer := bytes.NewBuffer(nil)
	bufSize := int64(256 * 1024)

	for {
		_, err := io.CopyN(buffer, rd, bufSize)
		if err != nil && err != io.EOF {
			return 0, err
		}

		wcount, werr := writerAt.WriteAt(buffer.Bytes(), off)
		n += int64(wcount)
		if werr != nil {
			return n, err
		}

		if err == io.EOF {
			return n, io.EOF
		}

		buffer.Reset()
		off += int64(wcount)
	}
}
