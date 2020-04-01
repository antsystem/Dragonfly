package seed

import (
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
	DownloadToWriterAt(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration, writeOff int64, writerAt io.WriterAt) (length int64, err error)
	Download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration, writer io.Writer) (length int64, err error)
}

func newLocalDownloader(url string, header map[string][]string, rate *ratelimiter.RateLimiter) downloader {
	return &localDownloader{
		url:    url,
		header: header,
		rate:   rate,
	}
}

type localDownloader struct {
	url    string
	header map[string][]string
	// todo: support ssl?

	rate *ratelimiter.RateLimiter
}

func (ld *localDownloader) Download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writer io.Writer) (length int64, err error) {
	return ld.download(ctx, rangeStruct, timeout, 0, nil, writer, false)
}

func (ld *localDownloader) DownloadToWriterAt(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration, writeOff int64, writerAt io.WriterAt) (length int64, err error) {
	return ld.download(ctx, rangeStruct, timeout, writeOff, writerAt, nil, true)
}

func (ld *localDownloader) download(ctx context.Context, rangeStruct httputils.RangeStruct, timeout time.Duration,
	writeOff int64, writerAt io.WriterAt, writer io.Writer, bWriteAt bool) (length int64, err error) {
	var (
		written int64
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
	rd := limitreader.NewLimitReaderWithLimiter(ld.rate, resp.Body, false)

	if bWriteAt {
		written, err = CopyBufferToWriterAt(writeOff, writerAt, rd)
	} else {
		written, err = io.CopyN(writer, rd, expectedLen)
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
