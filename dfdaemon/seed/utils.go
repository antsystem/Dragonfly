package seed

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
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
	}
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

func EncodeUintArray(input []uint64) []byte {
	arrLen := len(input)
	data := make([]byte, arrLen * 8)

	bytesIndex := 0
	for i := 0; i < arrLen; i ++ {
		binary.LittleEndian.PutUint64(data[bytesIndex: bytesIndex+ 8], input[i])
		bytesIndex += 8
	}

	return data[:bytesIndex]
}

func DecodeToUintArray(data []byte) []uint64 {
	var(
		bytesIndex int
	)

	arrLen := len(data) / 8
	out := make([]uint64, arrLen)
	for i := 0; i < arrLen ; i ++ {
		out[i] = binary.LittleEndian.Uint64(data[bytesIndex: bytesIndex + 8])
		bytesIndex += 8
	}

	return out
}
