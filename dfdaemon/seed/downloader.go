package seed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/limitreader"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"

	"github.com/pkg/errors"
)

// downloader manage the downloading of seed file
type downloader interface {
	DownloadToWriterAt(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration, writeOff int64, writerAt io.WriterAt, rateLimit bool) (length int64, err error)
	Download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration, writer io.Writer) (length int64, err error)
}

func newLocalDownloader(url string, header map[string][]string, rate *ratelimiter.RateLimiter, copyCache bool) downloader {
	return &localDownloader{
		url:    url,
		header: header,
		rate:   rate,
		copyCache: copyCache,
	}
}

type localDownloader struct {
	url    string
	header map[string][]string
	// todo: support ssl?

	rate *ratelimiter.RateLimiter
	// if copyCache sets, the response body will store to memory cache and transfer to writer
	copyCache		bool
}

func (ld *localDownloader) Download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writer io.Writer) (length int64, err error) {
	return ld.download(ctx, rangeStruct, timeout, 0, nil, writer, false, true)
}

func (ld *localDownloader) DownloadToWriterAt(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writeOff int64, writerAt io.WriterAt, rateLimit bool) (length int64, err error) {
	return ld.download(ctx, rangeStruct, timeout, writeOff, writerAt, nil, true, rateLimit)
}

func (ld *localDownloader) download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writeOff int64, writerAt io.WriterAt, writer io.Writer, bWriteAt bool, rateLimit bool) (length int64, err error) {
	var (
		written int64
		rd		io.Reader
	)

	header := map[string]string{}
	for k, v := range ld.header {
		header[k] = v[0]
	}

	header[config.StrRange] = fmt.Sprintf("bytes=%d-%d", rangeStruct.StartIndex, rangeStruct.EndIndex)
	resp, err := httputils.HTTPWithHeaders("GET", ld.url, header, timeout, nil)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, errortypes.NewHttpError(resp.StatusCode, "resp code is not 200 or 206")
	}

	expectedLen := rangeStruct.EndIndex - rangeStruct.StartIndex + 1

	defer resp.Body.Close()
	rd = resp.Body
	if rateLimit {
		rd = limitreader.NewLimitReaderWithLimiter(ld.rate, resp.Body, false)
	}

	if ld.copyCache {
		buf := bytes.NewBuffer(nil)
		written, err = io.CopyN(buf, rd, expectedLen)
		if written < expectedLen {
			return 0, errors.Wrap(io.ErrShortWrite, fmt.Sprintf("download from [%d,%d], expecte read %d, but got %d", rangeStruct.StartIndex, rangeStruct.EndIndex, expectedLen, written))
		}

		var n int
		if bWriteAt {
			n, err = writerAt.WriteAt(buf.Bytes(), writeOff)
		}else{
			n, err = writer.Write(buf.Bytes())
		}

		written = int64((n))

	}else {
		if bWriteAt {
			written, err = CopyBufferToWriterAt(writeOff, writerAt, rd)
		} else {
			written, err = io.CopyN(writer, rd, expectedLen)
		}
	}

	if err == io.EOF {
		err = nil
	}

	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("failed to download from [%d,%d]", rangeStruct.StartIndex, rangeStruct.EndIndex))
	}

	if written < expectedLen {
		return 0, errors.Wrap(io.ErrShortWrite, fmt.Sprintf("download from [%d,%d], expecte read %d, but got %d", rangeStruct.StartIndex, rangeStruct.EndIndex, expectedLen, written))
	}

	return written, err
}
